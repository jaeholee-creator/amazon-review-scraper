#!/usr/bin/env python3
"""BigQuery author_country 정규화 일회성 스크립트"""
import sys
sys.path.insert(0, '.')

from google.cloud import bigquery
from google.oauth2.service_account import Credentials

TABLE = "ax-test-jaeho.ax_cs.platform_reviews"
CREDS_FILE = "config/bigquery-service-account.json"

# 변환 대상: old_value → new_value
UPDATES = {
    "GB": "UK",
    "Philippines": "PH",
    "Singapore": "SG",
    "Taiwan": "TW",
    "United States": "US",
    "United Kingdom": "UK",
    "Canada": "CA",
    "Mexico": "MX",
    "Morocco": "MA",
    "Trinidad and Tobago": "TT",
    "Brazil": "BR",
    "India": "IN",
    "Germany": "DE",
    "France": "FR",
    "Japan": "JP",
    "Australia": "AU",
}

credentials = Credentials.from_service_account_file(
    CREDS_FILE, scopes=["https://www.googleapis.com/auth/bigquery"]
)
client = bigquery.Client(project="ax-test-jaeho", credentials=credentials)

# 1. 먼저 현재 author_country 분포 확인
print("=== 현재 author_country 분포 ===")
query = f"""
SELECT author_country, platform, COUNT(*) as cnt
FROM `{TABLE}`
WHERE author_country IS NOT NULL
GROUP BY author_country, platform
ORDER BY cnt DESC
"""
for row in client.query(query).result():
    print(f"  {row.author_country:30s} | {row.platform:10s} | {row.cnt:,}건")

# 2. 일괄 UPDATE (CASE WHEN 사용)
case_clauses = "\n".join(
    f"    WHEN '{old}' THEN '{new}'" for old, new in UPDATES.items()
)
where_values = ", ".join(f"'{old}'" for old in UPDATES.keys())

update_sql = f"""
UPDATE `{TABLE}`
SET author_country = CASE author_country
{case_clauses}
    ELSE author_country
  END,
  updated_at = CURRENT_TIMESTAMP()
WHERE author_country IN ({where_values})
"""

print("\n=== UPDATE 실행 ===")
job = client.query(update_sql)
job.result()

dml_stats = job._properties.get("statistics", {}).get("query", {}).get("dmlStats", {})
updated = int(dml_stats.get("updatedRowCount", "0"))
print(f"  {updated:,}건 업데이트 완료")

# 3. 업데이트 후 분포 확인
print("\n=== 업데이트 후 author_country 분포 ===")
for row in client.query(query).result():
    print(f"  {row.author_country:30s} | {row.platform:10s} | {row.cnt:,}건")

print("\n완료!")
