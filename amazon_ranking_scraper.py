"""
Amazon 상품 랭킹 수집기 - BSR 순위 + 평점 + 리뷰 수

각 상품 상세 페이지에서 Best Sellers Rank, 고객 평점, 리뷰 수를 수집하여
BigQuery에 적재합니다. (MERGE: 동일 asin+region+hour 데이터 중복 방지)

Usage:
    python amazon_ranking_scraper.py --region us
    python amazon_ranking_scraper.py --region uk
    python amazon_ranking_scraper.py --region all
"""

import asyncio
import csv
import logging
import sys
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

BQ_PROJECT = "member-378109"
BQ_DATASET = "jaeho"
BQ_TABLE = "amazon_product_rankings"
BQ_CREDENTIALS = "config/bigquery-service-account.json"

# fmt: off
CREATE_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}` (
    asin             STRING    NOT NULL OPTIONS(description='Amazon Standard Identification Number (10자리 고유 상품 코드)'),
    region           STRING    NOT NULL OPTIONS(description='수집 지역: us(미국) 또는 uk(영국)'),
    product_name     STRING             OPTIONS(description='상품명 (products.csv 기준)'),
    bsr_rank         INT64              OPTIONS(description='Best Sellers Rank 메인 순위 (낙을수록 높은 순위, 예: 6 = 전체 6위)'),
    bsr_category     STRING             OPTIONS(description='BSR 메인 카테고리명 (예: Beauty & Personal Care)'),
    bsr_category_url STRING             OPTIONS(description='BSR 메인 카테고리 베스트셀러 페이지 URL'),
    sub_rank         INT64              OPTIONS(description='첫 번째 서브카테고리 내 BSR 순위 (예: 1 = 해당 서브카테고리 1위)'),
    sub_category     STRING             OPTIONS(description='첫 번째 서브카테고리명 (예: Facial Masks, Facial Serums)'),
    rating           FLOAT64            OPTIONS(description='고객 평점 (0.0 ~ 5.0 범위, 소수점 1자리)'),
    review_count     INT64              OPTIONS(description='누적 리뷰 수 (해당 시점 전체 리뷰 총합)'),
    collected_at     TIMESTAMP NOT NULL OPTIONS(description='수집 시각 (UTC 기준 ISO 8601)'),
    collected_date   DATE      NOT NULL OPTIONS(description='수집 날짜 (파티션 키 — PARTITION BY collected_date)')
)
PARTITION BY collected_date
OPTIONS (
    description = 'Amazon 상품 BSR 순위 및 고객 평점 시간별 스냅샷 (US/UK). 매시간 수집.',
    partition_expiration_days = 730
)
"""
# fmt: on


def load_products(region: str) -> tuple[list[str], dict[str, str]]:
    """products.csv 에서 ASIN 목록 로드."""
    csv_path = f"config/products{'_uk' if region == 'uk' else ''}.csv"
    asins: list[str] = []
    names: dict[str, str] = {}
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                asin = row.get("asin", "").strip()
                name = row.get("name", "").strip()
                if asin:
                    asins.append(asin)
                    if name:
                        names[asin] = name
    except FileNotFoundError:
        logger.error("CSV not found: %s", csv_path)
    logger.info("[%s] Loaded %d products from CSV", region.upper(), len(asins))
    return asins, names


def _get_bq_client():
    """BigQuery 클라이언트 초기화."""
    from google.cloud import bigquery
    from google.oauth2.service_account import Credentials

    credentials = Credentials.from_service_account_file(
        BQ_CREDENTIALS,
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=BQ_PROJECT, credentials=credentials)


def ensure_table_exists():
    """BQ 테이블이 없으면 생성."""
    client = _get_bq_client()
    client.query(CREATE_TABLE_DDL).result()
    logger.info("BQ 테이블 확인/생성 완료: %s.%s.%s", BQ_PROJECT, BQ_DATASET, BQ_TABLE)


def insert_rankings(rows: list[dict]) -> int:
    """
    BigQuery에 랭킹 데이터 MERGE 적재.

    중복 키: asin + region + TIMESTAMP_TRUNC(collected_at, HOUR)
    같은 시간대에 재시도(retry)가 발생해도 중복 row 없음.
    """
    if not rows:
        return 0

    from google.cloud import bigquery

    client = _get_bq_client()
    full_table = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    temp_table = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}_tmp_{ts}"

    schema = [
        bigquery.SchemaField("asin",             "STRING"),
        bigquery.SchemaField("region",           "STRING"),
        bigquery.SchemaField("product_name",     "STRING"),
        bigquery.SchemaField("bsr_rank",         "INT64"),
        bigquery.SchemaField("bsr_category",     "STRING"),
        bigquery.SchemaField("bsr_category_url", "STRING"),
        bigquery.SchemaField("sub_rank",         "INT64"),
        bigquery.SchemaField("sub_category",     "STRING"),
        bigquery.SchemaField("rating",           "FLOAT64"),
        bigquery.SchemaField("review_count",     "INT64"),
        bigquery.SchemaField("collected_at",     "TIMESTAMP"),
        bigquery.SchemaField("collected_date",   "DATE"),
    ]

    normalized = [
        {
            "asin":             r["asin"],
            "region":           r["region"],
            "product_name":     r.get("product_name", ""),
            "bsr_rank":         r.get("bsr_rank"),
            "bsr_category":     r.get("bsr_category", ""),
            "bsr_category_url": r.get("bsr_category_url", ""),
            "sub_rank":         r.get("sub_rank"),
            "sub_category":     r.get("sub_category", ""),
            "rating":           r.get("rating"),
            "review_count":     r.get("review_count"),
            "collected_at":     r["collected_at"],
            "collected_date":   r["collected_date"],
        }
        for r in rows
    ]

    # 1. 임시 테이블에 적재
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
    )
    client.load_table_from_json(normalized, temp_table, job_config=job_config).result()
    logger.info("임시 테이블 적재 완료: %d rows → %s", len(normalized), temp_table)

    # 2. MERGE: 동일 asin+region+시간대 → UPDATE, 신규 → INSERT
    merge_sql = f"""
    MERGE `{full_table}` T
    USING `{temp_table}` S
    ON  T.asin   = S.asin
    AND T.region = S.region
    AND TIMESTAMP_TRUNC(T.collected_at, HOUR) = TIMESTAMP_TRUNC(S.collected_at, HOUR)
    WHEN MATCHED THEN UPDATE SET
        product_name     = S.product_name,
        bsr_rank         = S.bsr_rank,
        bsr_category     = S.bsr_category,
        bsr_category_url = S.bsr_category_url,
        sub_rank         = S.sub_rank,
        sub_category     = S.sub_category,
        rating           = S.rating,
        review_count     = S.review_count,
        collected_at     = S.collected_at
    WHEN NOT MATCHED THEN INSERT (
        asin, region, product_name,
        bsr_rank, bsr_category, bsr_category_url,
        sub_rank, sub_category,
        rating, review_count,
        collected_at, collected_date
    ) VALUES (
        S.asin, S.region, S.product_name,
        S.bsr_rank, S.bsr_category, S.bsr_category_url,
        S.sub_rank, S.sub_category,
        S.rating, S.review_count,
        S.collected_at, S.collected_date
    )
    """
    client.query(merge_sql).result()
    logger.info("MERGE 완료: %d rows", len(normalized))

    # 3. 임시 테이블 삭제
    client.delete_table(temp_table, not_found_ok=True)

    return len(normalized)


async def run_region(region: str) -> int:
    """단일 region 랭킹 수집 전체 흐름."""
    from scrapers.amazon.ranking_scraper import scrape_all_rankings

    asins, names = load_products(region)
    if not asins:
        logger.warning("[%s] 수집할 상품이 없습니다", region.upper())
        return 0

    print(f"\n{'='*60}")
    print(f"   Amazon Ranking Scraper")
    print(f"{'='*60}")
    print(f"   Region  : {region.upper()}")
    print(f"   Products: {len(asins)}")
    print(f"   Time    : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    results = await scrape_all_rankings(asins, region, product_names=names)

    if not results:
        logger.warning("[%s] 수집된 데이터 없음", region.upper())
        return 0

    inserted = insert_rankings(results)
    print(f"\n[{region.upper()}] BQ 적재 완료: {inserted}개 rows")

    bsr_collected = sum(1 for r in results if r.get("bsr_rank"))
    print(f"[{region.upper()}] BSR 수집: {bsr_collected}/{len(asins)} 상품")
    for r in results[:5]:
        bsr = r.get("bsr_rank", "-")
        sub = r.get("sub_rank", "-")
        sub_cat = r.get("sub_category", "")[:25]
        rating = r.get("rating", "-")
        name = r.get("product_name", r["asin"])[:35]
        print(f"   {r['asin']}  BSR #{bsr}  sub #{sub} {sub_cat}  {rating}\u2605  {name}")

    return inserted


async def main():
    region = "us"
    if "--region" in sys.argv:
        idx = sys.argv.index("--region")
        if idx + 1 < len(sys.argv):
            region = sys.argv[idx + 1].lower()

    if region not in ("us", "uk", "all"):
        print(f"Invalid region: {region}. Use 'us', 'uk', or 'all'.")
        sys.exit(1)

    ensure_table_exists()

    if region == "all":
        total = 0
        for r in ("us", "uk"):
            total += await run_region(r)
        print(f"\n전체 완료: {total}개 rows")
    else:
        await run_region(region)


if __name__ == "__main__":
    asyncio.run(main())
