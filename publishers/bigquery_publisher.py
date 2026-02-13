"""
BigQuery Publisher - 리뷰 데이터를 BigQuery에 적재

- MERGE 방식으로 중복 자동 제거
- 플랫폼별 데이터 정규화 (Amazon, Shopee, TikTok)
- 없는 필드는 NULL로 자동 처리
"""

import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from google.cloud import bigquery
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)


class BigQueryPublisher:
    """BigQuery를 사용하여 리뷰 데이터 발행"""

    SCOPES = ["https://www.googleapis.com/auth/bigquery"]

    # BigQuery 통합 스키마의 모든 컬럼 (순서 중요)
    ALL_COLUMNS = [
        "review_id", "platform", "platform_country", "collected_at",
        "product_name", "product_id",
        "author", "author_country",
        "star", "title", "content", "date",
        "verified_purchase", "item_type",
        "reply_content", "seller_reply", "reply_count",
        "image_urls", "video_urls", "has_video",
        "likes_count",
        "detailed_rating_product", "detailed_rating_seller", "detailed_rating_delivery",
        "order_id", "sku",
    ]

    def __init__(
        self,
        project_id: str = "ax-test-jaeho",
        dataset_id: str = "ax_cs",
        table_id: str = "platform_reviews",
        credentials_file: str = "config/bigquery-service-account.json",
    ):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.full_table_id = f"{project_id}.{dataset_id}.{table_id}"

        credentials = Credentials.from_service_account_file(
            credentials_file, scopes=self.SCOPES
        )
        self.client = bigquery.Client(project=project_id, credentials=credentials)
        logger.info("BigQuery 클라이언트 초기화 완료: %s", self.full_table_id)

    def publish_incremental(
        self,
        reviews: list[dict],
        platform: str,
        collected_at: Optional[str] = None,
    ) -> dict:
        """증분 업데이트 메인 메서드 (MERGE 방식)"""
        if not reviews:
            logger.info("[%s] 추가할 리뷰가 없습니다", platform)
            return {"inserted": 0, "updated": 0, "total_processed": 0, "status": "success"}

        collected_at = collected_at or datetime.now(timezone.utc).isoformat()

        # 1. 정규화
        normalized = [self._normalize_review(r, platform, collected_at) for r in reviews]
        logger.info("[%s] %d개 리뷰 정규화 완료", platform, len(normalized))

        # 2. 배치 MERGE (1000건씩)
        batch_size = 1000
        total_inserted = 0
        total_updated = 0

        for i in range(0, len(normalized), batch_size):
            batch = normalized[i:i + batch_size]
            result = self._merge_reviews(batch)
            total_inserted += result["inserted"]
            total_updated += result["updated"]
            logger.info(
                "[%s] 배치 %d/%d 완료: insert=%d, update=%d",
                platform, i // batch_size + 1,
                (len(normalized) + batch_size - 1) // batch_size,
                result["inserted"], result["updated"],
            )

        logger.info(
            "[%s] 총 %d개 처리: insert=%d, update=%d",
            platform, len(normalized), total_inserted, total_updated,
        )

        return {
            "inserted": total_inserted,
            "updated": total_updated,
            "total_processed": len(normalized),
            "status": "success",
        }

    def _normalize_review(self, review: dict, platform: str, collected_at: str) -> dict:
        """플랫폼별 리뷰 데이터를 통합 스키마로 정규화"""
        normalized = {
            "review_id": str(review.get("review_id", "")),
            "platform": platform,
            "platform_country": _normalize_country(review.get("platform_country"), platform),
            "collected_at": _to_timestamp(review.get("collected_at")) or _to_timestamp(collected_at),
            "product_name": _to_str(review.get("product_name")),
            "product_id": _to_str(review.get("product_id")),
            "author": _to_str(review.get("author")),
            "author_country": _normalize_country(review.get("author_country"), platform),
            "star": _to_float(review.get("star")),
            "title": _to_str(review.get("title")),
            "content": _to_str(review.get("content")),
            "date": _to_date_str(review.get("date")),
            "verified_purchase": _to_bool(review.get("verified_purchase")),
            "item_type": _to_str(review.get("item_type")),
            "reply_content": _to_str(review.get("reply_content")),
            "seller_reply": _to_str(review.get("seller_reply")),
            "reply_count": _to_int(review.get("reply_count")),
            "image_urls": _format_urls(review.get("image_urls")),
            "video_urls": _format_urls(review.get("video_urls")),
            "has_video": _to_bool(review.get("has_video")),
            "likes_count": _to_int(review.get("likes_count")),
            "detailed_rating_product": _to_float(review.get("detailed_rating_product")),
            "detailed_rating_seller": _to_float(review.get("detailed_rating_seller")),
            "detailed_rating_delivery": _to_float(review.get("detailed_rating_delivery")),
            "order_id": _to_str(review.get("order_id")),
            "sku": _to_str(review.get("sku")),
        }
        return normalized

    def _merge_reviews(self, reviews: list[dict]) -> dict:
        """임시 테이블 + MERGE 문으로 중복 제거 및 삽입"""
        temp_table_id = f"_temp_merge_{uuid.uuid4().hex[:8]}"
        temp_full_id = f"{self.project_id}.{self.dataset_id}.{temp_table_id}"

        try:
            # 1. 임시 테이블 생성 (원본과 동일 스키마, 파티셔닝/클러스터링 제외)
            source_table = self.client.get_table(self.full_table_id)
            temp_table = bigquery.Table(temp_full_id, schema=source_table.schema)
            temp_table = self.client.create_table(temp_table)

            # 2. 임시 테이블에 데이터 로드
            rows_to_insert = []
            for r in reviews:
                row = {col: r.get(col) for col in self.ALL_COLUMNS}
                row["created_at"] = datetime.now(timezone.utc).isoformat()
                row["updated_at"] = datetime.now(timezone.utc).isoformat()
                rows_to_insert.append(row)

            errors = self.client.insert_rows_json(temp_table, rows_to_insert)
            if errors:
                logger.error("임시 테이블 삽입 오류: %s", errors[:3])
                raise RuntimeError(f"임시 테이블 삽입 실패: {errors[:3]}")

            # Streaming buffer flush 대기
            time.sleep(10)

            # 3. MERGE 실행
            columns_str = ", ".join(self.ALL_COLUMNS)
            source_columns_str = ", ".join(f"source.{c}" for c in self.ALL_COLUMNS)

            merge_sql = f"""
            MERGE `{self.full_table_id}` AS target
            USING `{temp_full_id}` AS source
            ON target.review_id = source.review_id AND target.platform = source.platform
            WHEN NOT MATCHED THEN
              INSERT ({columns_str}, created_at, updated_at)
              VALUES ({source_columns_str}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
            WHEN MATCHED AND (
              COALESCE(target.likes_count, 0) != COALESCE(source.likes_count, 0) OR
              COALESCE(target.reply_content, '') != COALESCE(source.reply_content, '') OR
              COALESCE(target.seller_reply, '') != COALESCE(source.seller_reply, '')
            ) THEN
              UPDATE SET
                likes_count = source.likes_count,
                reply_content = source.reply_content,
                seller_reply = source.seller_reply,
                reply_count = source.reply_count,
                updated_at = CURRENT_TIMESTAMP()
            """

            job = self.client.query(merge_sql)
            job.result()

            stats = job.num_dml_affected_rows or 0
            # DML 통계에서 insert/update 구분
            dml_stats = job._properties.get("statistics", {}).get("query", {}).get("dmlStats", {})
            inserted = int(dml_stats.get("insertedRowCount", "0"))
            updated = int(dml_stats.get("updatedRowCount", "0"))

            return {"inserted": inserted, "updated": updated}

        finally:
            # 4. 임시 테이블 삭제
            self.client.delete_table(temp_full_id, not_found_ok=True)

    def get_existing_review_ids(self, platform: str, days: int = 30) -> set[str]:
        """기존 review_id 집합 조회"""
        query = f"""
        SELECT review_id
        FROM `{self.full_table_id}`
        WHERE platform = @platform
          AND DATE(collected_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("platform", "STRING", platform),
                bigquery.ScalarQueryParameter("days", "INT64", days),
            ]
        )
        result = self.client.query(query, job_config=job_config).result()
        ids = {row.review_id for row in result}
        logger.info("[%s] 기존 review_id %d개 로드 (최근 %d일)", platform, len(ids), days)
        return ids


# =====================================================================
# 유틸리티 함수
# =====================================================================

_COUNTRY_NORMALIZE = {
    # --- 영문 국가명 → ISO 2자리 코드 ---
    "United States": "US",
    "United Kingdom": "UK",
    "Canada": "CA",
    "Mexico": "MX",
    "Brazil": "BR",
    "India": "IN",
    "Germany": "DE",
    "France": "FR",
    "Italy": "IT",
    "Spain": "ES",
    "Japan": "JP",
    "Australia": "AU",
    "Netherlands": "NL",
    "Singapore": "SG",
    "Philippines": "PH",
    "Taiwan": "TW",
    "Thailand": "TH",
    "Vietnam": "VN",
    "Malaysia": "MY",
    "Indonesia": "ID",
    "South Korea": "KR",
    "China": "CN",
    "Hong Kong": "HK",
    "New Zealand": "NZ",
    "Morocco": "MA",
    "Trinidad and Tobago": "TT",
    "Colombia": "CO",
    "Argentina": "AR",
    "Chile": "CL",
    "Peru": "PE",
    "Portugal": "PT",
    "Poland": "PL",
    "Sweden": "SE",
    "Norway": "NO",
    "Denmark": "DK",
    "Finland": "FI",
    "Belgium": "BE",
    "Switzerland": "CH",
    "Austria": "AT",
    "Ireland": "IE",
    "South Africa": "ZA",
    "Egypt": "EG",
    "Saudi Arabia": "SA",
    "United Arab Emirates": "AE",
    "Turkey": "TR",
    "Russia": "RU",
    "Ukraine": "UA",
    "Israel": "IL",
    "Nigeria": "NG",
    "Kenya": "KE",
    "Pakistan": "PK",
    "Bangladesh": "BD",
    "Sri Lanka": "LK",
    # --- 소문자 변형 (Amazon 파서 결과 대비) ---
    "united states": "US",
    "united kingdom": "UK",
    # --- 한국어 국가명 ---
    "미국": "US",
    "영국": "UK",
    "캐나다": "CA",
    "일본": "JP",
    "독일": "DE",
    "프랑스": "FR",
    "호주": "AU",
    "인도": "IN",
    "싱가포르": "SG",
    "필리핀": "PH",
    "대만": "TW",
    "태국": "TH",
    "말레이시아": "MY",
    "멕시코": "MX",
    "브라질": "BR",
    "중국": "CN",
    "한국": "KR",
    # --- ISO 코드 정규화 (GB → UK 통일) ---
    "GB": "UK",
}

_PLATFORM_DEFAULT_COUNTRY = {
    "tiktok": "US",
}


def _normalize_country(value: Any, platform: str) -> Optional[str]:
    """국가명을 ISO 2자리 코드로 정규화하고, 없으면 플랫폼 기본값 반환"""
    s = _to_str(value)
    if s:
        s = s.strip()
        # 정확한 매핑이 있으면 사용
        if s in _COUNTRY_NORMALIZE:
            return _COUNTRY_NORMALIZE[s]
        # 대소문자 무시하고 한번 더 시도
        for key, code in _COUNTRY_NORMALIZE.items():
            if key.lower() == s.lower():
                return code
        # 이미 2자리 ISO 코드인 경우 대문자로 반환
        if len(s) == 2 and s.isalpha():
            return s.upper()
        return s
    return _PLATFORM_DEFAULT_COUNTRY.get(platform)


def _to_str(value: Any) -> Optional[str]:
    if value is None or value == "" or value == 0:
        return None
    return str(value)


def _to_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _to_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def _to_bool(value: Any) -> Optional[bool]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "yes", "1")
    return bool(value)


def _to_date_str(value: Any) -> Optional[str]:
    """다양한 날짜 형식을 YYYY-MM-DD 문자열로 변환"""
    if value is None or value == "":
        return None

    # datetime 객체 직접 처리
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if hasattr(value, 'isoformat'):  # date 객체
        return str(value)

    s = str(value).strip()

    # 이미 YYYY-MM-DD 형식이면 그대로
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    # YYYY-MM-DDTHH:MM:SS 형식이면 날짜만 추출
    if "T" in s:
        return s[:10]
    # "YYYY-MM-DD HH:MM:SS" 형식 (공백 구분) → 날짜만 추출
    if len(s) > 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    # MM/DD/YYYY 형식
    if "/" in s:
        parts = s.split("/")
        if len(parts) == 3:
            try:
                return f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
            except (ValueError, IndexError):
                pass
    # 영문 날짜: "February 11, 2026", "January 1, 2025" 등
    try:
        parsed = datetime.strptime(s, "%B %d, %Y")
        return parsed.strftime("%Y-%m-%d")
    except ValueError:
        pass
    # Unix timestamp (10자리 정수)
    if s.isdigit() and len(s) >= 9:
        try:
            return datetime.fromtimestamp(int(s)).strftime("%Y-%m-%d")
        except (ValueError, OSError):
            pass
    return None


def _format_urls(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    if isinstance(value, list):
        return ";".join(str(u) for u in value if u)
    return str(value) if value else None


def _to_timestamp(value: Any) -> Optional[str]:
    """다양한 타임스탬프 형식을 BigQuery TIMESTAMP 형식으로 변환

    BigQuery 요구 형식: YYYY-MM-DD HH:MM:SS 또는 ISO 8601
    문제: Google Sheets에서 '2026-02-12 5:18:35' (시간 패딩 없음) 형식이 올 수 있음
    """
    if value is None or value == "":
        return None
    s = str(value).strip()

    # ISO 8601 형식이면 그대로
    if "T" in s:
        return s

    # 'YYYY-MM-DD H:MM:SS' → 'YYYY-MM-DD HH:MM:SS' 변환
    if " " in s:
        parts = s.split(" ", 1)
        date_part = parts[0]
        time_part = parts[1] if len(parts) > 1 else "00:00:00"

        # 시간 파트의 각 요소를 2자리로 패딩
        time_components = time_part.split(":")
        padded_time = ":".join(c.zfill(2) for c in time_components)

        return f"{date_part} {padded_time}"

    return s
