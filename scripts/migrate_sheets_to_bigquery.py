#!/usr/bin/env python3
"""
Google Sheets → BigQuery 마이그레이션 스크립트

각 플랫폼별 시트 데이터를 BigQuery 통합 스키마로 변환하여 적재합니다.
- Amazon (US자사몰): 16개 컬럼 → 27개 컬럼 (없는 필드 NULL)
- Shopee (SG_shopee, PH_shopee): 19개 컬럼 → 27개 컬럼
- TikTok (US_tiktok): 14개 컬럼 → 27개 컬럼 (없는 필드 NULL)
"""

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials

from publishers.bigquery_publisher import BigQueryPublisher

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# =====================================================================
# 설정
# =====================================================================
SPREADSHEET_ID = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
SHEETS_SERVICE_ACCOUNT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "credentials.json",
)
BQ_SERVICE_ACCOUNT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "config", "bigquery-service-account.json",
)

# 마이그레이션할 시트 목록
MIGRATION_TARGETS = [
    {"sheet_name": "US자사몰", "platform": "amazon"},
    {"sheet_name": "US_amazone", "platform": "amazon"},
    {"sheet_name": "UK_amazone", "platform": "amazon"},
    {"sheet_name": "SG_shopee", "platform": "shopee"},
    {"sheet_name": "PH_shopee", "platform": "shopee"},
    {"sheet_name": "US_TIkTOK", "platform": "tiktok"},
]

# 레거시 시트의 컬럼명 → BigQuery 스키마 컬럼명 매핑
# US_amazone, UK_amazone 시트는 다른 헤더명을 사용
COLUMN_MAPPINGS = {
    "US_amazone": {
        "Review ID": "review_id",
        "ASIN": "product_id",
        "Rating": "star",
        "Title": "title",
        "Author": "author",
        "Date": "date",
        "Location": "author_country",
        "Verified Purchase": "verified_purchase",
        "Content": "content",
        "Helpful Count": "likes_count",
        "Scraped At": "collected_at",
    },
    "UK_amazone": {
        "Review ID": "review_id",
        "ASIN": "product_id",
        "Rating": "star",
        "Title": "title",
        "Author": "author",
        "Date": "date",
        "Location": "author_country",
        "Verified Purchase": "verified_purchase",
        "Content": "content",
        "Helpful Count": "likes_count",
        "Scraped At": "collected_at",
    },
}

# 시트명 → platform_country 매핑
SHEET_PLATFORM_COUNTRY = {
    "US자사몰": "US",
    "US_amazone": "US",
    "UK_amazone": "UK",
    "SG_shopee": "SG",
    "PH_shopee": "PH",
    "US_TIkTOK": "US",
}


def get_sheets_client() -> gspread.Client:
    """Google Sheets 인증"""
    credentials = Credentials.from_service_account_file(
        SHEETS_SERVICE_ACCOUNT,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(credentials)


def migrate_sheet(
    sheets_client: gspread.Client,
    bq_publisher: BigQueryPublisher,
    sheet_name: str,
    platform: str,
) -> dict:
    """단일 시트의 데이터를 BigQuery로 마이그레이션"""
    print(f"\n{'=' * 60}")
    print(f"  [{platform.upper()}] {sheet_name} 마이그레이션 시작")
    print(f"{'=' * 60}")

    # 1. Google Sheets에서 데이터 읽기
    spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)
    try:
        sheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        logger.warning("[%s] 시트 '%s'를 찾을 수 없습니다. 건너뜁니다.", platform, sheet_name)
        return {"sheet_name": sheet_name, "platform": platform, "status": "skipped", "total": 0}

    all_data = sheet.get_all_records()
    total_rows = len(all_data)
    logger.info("[%s] %s: 총 %d개 행 읽기 완료", platform, sheet_name, total_rows)

    if total_rows == 0:
        logger.info("[%s] 데이터가 없습니다. 건너뜁니다.", platform)
        return {"sheet_name": sheet_name, "platform": platform, "status": "empty", "total": 0}

    # 2. 헤더 확인 (디버깅)
    headers = list(all_data[0].keys())
    logger.info("[%s] 헤더 (%d개): %s", platform, len(headers), headers)

    # 3. BigQuery 스키마에 맞게 변환
    # 레거시 시트의 경우 컬럼명 매핑 적용
    column_mapping = COLUMN_MAPPINGS.get(sheet_name, {})

    reviews = []
    for row in all_data:
        review = {}
        for key, value in row.items():
            mapped_key = column_mapping.get(key, key)
            review[mapped_key] = value

        # review_id가 비어있는 행은 건너뛰기
        if not review.get("review_id"):
            continue

        # 시트명 기반 platform_country 설정 (데이터에 없는 경우)
        if not review.get("platform_country"):
            review["platform_country"] = SHEET_PLATFORM_COUNTRY.get(sheet_name, "")

        reviews.append(review)

    logger.info("[%s] %d개 유효 리뷰 (review_id 있음)", platform, len(reviews))

    if not reviews:
        return {"sheet_name": sheet_name, "platform": platform, "status": "no_valid_reviews", "total": 0}

    # 4. BigQuery에 적재 (MERGE)
    # collected_at: 각 행의 collected_at 값을 그대로 사용
    # BigQueryPublisher는 행별로 collected_at을 처리합니다.
    # 하지만 현재 publish_incremental은 단일 collected_at을 사용하므로
    # 각 행의 collected_at을 보존하기 위해 직접 정규화합니다.
    from datetime import datetime, timezone

    normalized_reviews = []
    for r in reviews:
        # 각 행의 collected_at 보존
        row_collected_at = r.get("collected_at", "") or datetime.now(timezone.utc).isoformat()
        normalized = bq_publisher._normalize_review(r, platform, str(row_collected_at))
        normalized_reviews.append(normalized)

    # 배치로 MERGE 실행
    batch_size = 1000
    total_inserted = 0
    total_updated = 0

    for i in range(0, len(normalized_reviews), batch_size):
        batch = normalized_reviews[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(normalized_reviews) + batch_size - 1) // batch_size

        logger.info(
            "[%s] 배치 %d/%d (%d개) MERGE 중...",
            platform, batch_num, total_batches, len(batch),
        )

        result = bq_publisher._merge_reviews(batch)
        total_inserted += result["inserted"]
        total_updated += result["updated"]

        logger.info(
            "[%s] 배치 %d/%d 완료: insert=%d, update=%d",
            platform, batch_num, total_batches,
            result["inserted"], result["updated"],
        )

    summary = {
        "sheet_name": sheet_name,
        "platform": platform,
        "status": "success",
        "total": len(normalized_reviews),
        "inserted": total_inserted,
        "updated": total_updated,
    }

    print(f"\n  결과: 총 {len(normalized_reviews)}개 → 삽입 {total_inserted}, 업데이트 {total_updated}")
    return summary


def main():
    print("\n" + "=" * 60)
    print("  Google Sheets → BigQuery 마이그레이션")
    print("  프로젝트: ax-test-jaeho")
    print("  데이터셋: ax_cs")
    print("  테이블: platform_reviews")
    print("=" * 60)

    if not SPREADSHEET_ID:
        logger.error("GOOGLE_SHEETS_SPREADSHEET_ID 환경변수가 설정되지 않았습니다.")
        sys.exit(1)

    logger.info("스프레드시트 ID: %s", SPREADSHEET_ID)

    # 인증
    sheets_client = get_sheets_client()
    bq_publisher = BigQueryPublisher(
        project_id="ax-test-jaeho",
        dataset_id="ax_cs",
        table_id="platform_reviews",
        credentials_file=BQ_SERVICE_ACCOUNT,
    )

    # 마이그레이션 실행
    results = []
    for target in MIGRATION_TARGETS:
        try:
            result = migrate_sheet(
                sheets_client, bq_publisher,
                target["sheet_name"], target["platform"],
            )
            results.append(result)
        except Exception as e:
            logger.error("[%s] %s 마이그레이션 실패: %s", target["platform"], target["sheet_name"], e)
            results.append({
                "sheet_name": target["sheet_name"],
                "platform": target["platform"],
                "status": "error",
                "error": str(e),
                "total": 0,
            })

    # 최종 요약
    print("\n" + "=" * 60)
    print("  마이그레이션 완료 요약")
    print("=" * 60)
    total_all = 0
    for r in results:
        status_icon = {
            "success": "OK",
            "skipped": "SKIP",
            "empty": "EMPTY",
            "error": "FAIL",
            "no_valid_reviews": "SKIP",
        }.get(r["status"], "?")

        detail = ""
        if r["status"] == "success":
            detail = f"삽입={r['inserted']}, 업데이트={r['updated']}"
            total_all += r["total"]
        elif r["status"] == "error":
            detail = f"오류: {r.get('error', 'unknown')}"

        print(f"  [{status_icon:>5}] {r['sheet_name']:<15} ({r['platform']:<8}) 총 {r['total']:>6}개  {detail}")

    print(f"\n  전체 마이그레이션 행: {total_all}개")
    print("=" * 60)


if __name__ == "__main__":
    main()
