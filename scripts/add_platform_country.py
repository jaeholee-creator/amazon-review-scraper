#!/usr/bin/env python3
"""BigQuery platform_country 컬럼 추가 및 기존 데이터 마이그레이션"""
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

TABLE = "ax-test-jaeho.ax_cs.platform_reviews"
CREDS_FILE = "config/bigquery-service-account.json"

credentials = Credentials.from_service_account_file(
    CREDS_FILE, scopes=["https://www.googleapis.com/auth/bigquery"]
)
client = bigquery.Client(project="ax-test-jaeho", credentials=credentials)


def run_query(sql, description):
    print(f"\n=== {description} ===")
    print(f"SQL: {sql[:200]}...")
    job = client.query(sql)
    job.result()
    dml_stats = job._properties.get("statistics", {}).get("query", {}).get("dmlStats", {})
    updated = int(dml_stats.get("updatedRowCount", "0"))
    if updated:
        print(f"  {updated:,}건 업데이트")
    else:
        print("  완료")
    return job


# 1. platform_country 컬럼 추가
print("=== Step 1: platform_country 컬럼 추가 ===")
try:
    alter_sql = f"""
    ALTER TABLE `{TABLE}`
    ADD COLUMN platform_country STRING
    OPTIONS(description='마켓플레이스/채널 국가코드 | Amazon: US/UK | Shopee: SG/TW/PH | TikTok: US 등')
    """
    job = client.query(alter_sql)
    job.result()
    print("  platform_country 컬럼 추가 완료")
except Exception as e:
    if "Already Exists" in str(e) or "Duplicate" in str(e).lower():
        print("  platform_country 컬럼이 이미 존재합니다")
    else:
        raise

# 2. Shopee: author_country → platform_country 이동, author_country = NULL
run_query(f"""
UPDATE `{TABLE}`
SET platform_country = author_country,
    author_country = NULL,
    updated_at = CURRENT_TIMESTAMP()
WHERE platform = 'shopee' AND author_country IS NOT NULL
""", "Step 2: Shopee - author_country → platform_country 이동 + NULL 처리")

# 3. Amazon: platform_country 설정
# UK marketplace 리뷰 (UK_amazone 시트에서 온 데이터)
# 기존 마이그레이션 시 UK_amazone은 5건, author_country=UK인 리뷰가 UK marketplace 출처
run_query(f"""
UPDATE `{TABLE}`
SET platform_country = 'UK',
    updated_at = CURRENT_TIMESTAMP()
WHERE platform = 'amazon' AND author_country = 'UK' AND platform_country IS NULL
""", "Step 3a: Amazon UK marketplace (author_country=UK → platform_country=UK)")

# 나머지 Amazon → US marketplace
run_query(f"""
UPDATE `{TABLE}`
SET platform_country = 'US',
    updated_at = CURRENT_TIMESTAMP()
WHERE platform = 'amazon' AND platform_country IS NULL
""", "Step 3b: Amazon US marketplace (나머지 → platform_country=US)")

# 4. TikTok: platform_country = US
run_query(f"""
UPDATE `{TABLE}`
SET platform_country = 'US',
    updated_at = CURRENT_TIMESTAMP()
WHERE platform = 'tiktok' AND platform_country IS NULL
""", "Step 4: TikTok → platform_country=US")

# 5. 최종 확인
print("\n=== 최종 데이터 분포 ===")
query = f"""
SELECT platform, platform_country, author_country, COUNT(*) as cnt
FROM `{TABLE}`
GROUP BY platform, platform_country, author_country
ORDER BY platform, platform_country, cnt DESC
"""
for row in client.query(query).result():
    ac = row.author_country or 'NULL'
    pc = row.platform_country or 'NULL'
    print(f"  {row.platform:10s} | pc={pc:5s} | ac={ac:5s} | {row.cnt:,}건")

print("\n완료!")
