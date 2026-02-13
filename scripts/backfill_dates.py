#!/usr/bin/env python3
"""
BigQuery date 컬럼 백필 스크립트

Google Sheets 원본에서 date 데이터를 읽어 BigQuery의 NULL date를 채움.
각 시트별 date 형식이 다르므로 개별 파싱 로직 적용.
"""
import re
import sys
from datetime import datetime

from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build

SPREADSHEET_ID = '1NVUVShv5tAveINA9DdB2D21z71L3tF0In5JVK6LYX9s'
SHEETS_CREDS = 'config/service-account.json'
BQ_CREDS = 'config/bigquery-service-account.json'
TABLE = 'ax-test-jaeho.ax_cs.platform_reviews'


def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        SHEETS_CREDS,
        scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
    )
    return build('sheets', 'v4', credentials=creds)


def get_bq_client():
    creds = service_account.Credentials.from_service_account_file(
        BQ_CREDS, scopes=['https://www.googleapis.com/auth/bigquery']
    )
    return bigquery.Client(project='ax-test-jaeho', credentials=creds)


def read_sheet(service, sheet_name, range_suffix=''):
    """시트 데이터 읽기 → (headers, rows)"""
    range_str = f"'{sheet_name}'!A:Z" if not range_suffix else f"'{sheet_name}'!{range_suffix}"
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=range_str
    ).execute()
    values = result.get('values', [])
    if not values:
        return [], []
    return values[0], values[1:]


def parse_amazon_us_date(date_str):
    """'Reviewed in the United States on February 10, 2026' → 'YYYY-MM-DD'"""
    if not date_str:
        return None
    # "on Month DD, YYYY"
    m = re.search(r'on\s+(\w+ \d{1,2},\s*\d{4})', date_str)
    if m:
        try:
            return datetime.strptime(m.group(1).replace('  ', ' '), '%B %d, %Y').strftime('%Y-%m-%d')
        except ValueError:
            pass
    return None


def parse_amazon_uk_date(date_str):
    """'Reviewed in the United Kingdom on 9 February 2026' → 'YYYY-MM-DD'"""
    if not date_str:
        return None
    # "on DD Month YYYY"
    m = re.search(r'on\s+(\d{1,2}\s+\w+\s+\d{4})', date_str)
    if m:
        try:
            return datetime.strptime(m.group(1), '%d %B %Y').strftime('%Y-%m-%d')
        except ValueError:
            pass
    # 미국 형식도 시도
    return parse_amazon_us_date(date_str)


def parse_tiktok_date(date_str):
    """'February 11, 2026' → 'YYYY-MM-DD'"""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str.strip(), '%B %d, %Y').strftime('%Y-%m-%d')
    except ValueError:
        return None


def parse_shopee_date(submit_date, submit_time=''):
    """submit_date='2026-02-12' 또는 submit_time=Unix timestamp → 'YYYY-MM-DD'"""
    if submit_date and len(submit_date) == 10 and submit_date[4] == '-':
        return submit_date
    if submit_time and submit_time.isdigit() and len(submit_time) >= 9:
        try:
            return datetime.fromtimestamp(int(submit_time)).strftime('%Y-%m-%d')
        except (ValueError, OSError):
            pass
    return None


def batch_update_dates(bq_client, updates, platform):
    """BigQuery에 date 일괄 업데이트 (review_id → date 매핑)"""
    if not updates:
        print(f"  [{platform}] 업데이트할 데이터 없음")
        return 0

    # CASE WHEN 구문으로 일괄 UPDATE (1000건씩)
    total_updated = 0
    batch_size = 500

    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]
        case_clauses = "\n".join(
            f"    WHEN review_id = '{rid}' THEN DATE('{date}')"
            for rid, date in batch
        )
        review_ids = ", ".join(f"'{rid}'" for rid, _ in batch)

        sql = f"""
        UPDATE `{TABLE}`
        SET date = CASE
        {case_clauses}
          ELSE date
        END,
        updated_at = CURRENT_TIMESTAMP()
        WHERE platform = '{platform}'
          AND date IS NULL
          AND review_id IN ({review_ids})
        """

        job = bq_client.query(sql)
        job.result()
        dml = job._properties.get("statistics", {}).get("query", {}).get("dmlStats", {})
        cnt = int(dml.get("updatedRowCount", "0"))
        total_updated += cnt
        print(f"  [{platform}] 배치 {i // batch_size + 1}: {cnt}건 업데이트")

    return total_updated


def main():
    print("=" * 80)
    print("BigQuery date 백필 시작")
    print("=" * 80)

    service = get_sheets_service()
    bq = get_bq_client()

    # =====================================================
    # 1. US_amazone (56건 누락)
    # =====================================================
    print("\n[1/5] US_amazone 처리 중...")
    headers, rows = read_sheet(service, 'US_amazone')
    h_map = {h: i for i, h in enumerate(headers)}
    updates = []
    for row in rows:
        rid = row[h_map['Review ID']] if h_map.get('Review ID', -1) < len(row) else ''
        date_raw = row[h_map['Date']] if h_map.get('Date', -1) < len(row) else ''
        parsed = parse_amazon_us_date(date_raw)
        if rid and parsed:
            updates.append((rid, parsed))
    print(f"  파싱 성공: {len(updates)}건")
    cnt = batch_update_dates(bq, updates, 'amazon')
    print(f"  US_amazone 완료: {cnt}건 업데이트")

    # =====================================================
    # 2. UK_amazone (5건 누락)
    # =====================================================
    print("\n[2/5] UK_amazone 처리 중...")
    headers, rows = read_sheet(service, 'UK_amazone')
    h_map = {h: i for i, h in enumerate(headers)}
    updates = []
    for row in rows:
        rid = row[h_map['Review ID']] if h_map.get('Review ID', -1) < len(row) else ''
        date_raw = row[h_map['Date']] if h_map.get('Date', -1) < len(row) else ''
        parsed = parse_amazon_uk_date(date_raw)
        if rid and parsed:
            updates.append((rid, parsed))
    print(f"  파싱 성공: {len(updates)}건")
    cnt = batch_update_dates(bq, updates, 'amazon')
    print(f"  UK_amazone 완료: {cnt}건 업데이트")

    # =====================================================
    # 3. shopee (56,450건 누락)
    # =====================================================
    print("\n[3/5] shopee 처리 중...")
    headers, rows = read_sheet(service, 'shopee')
    h_map = {h: i for i, h in enumerate(headers)}
    updates = []
    for row in rows:
        rid_idx = h_map.get('comment_id', -1)
        sd_idx = h_map.get('submit_date', -1)
        st_idx = h_map.get('submit_time', -1)
        rid = row[rid_idx] if rid_idx < len(row) and rid_idx >= 0 else ''
        sd = row[sd_idx] if sd_idx < len(row) and sd_idx >= 0 else ''
        st = row[st_idx] if st_idx < len(row) and st_idx >= 0 else ''
        parsed = parse_shopee_date(sd, st)
        if rid and parsed:
            updates.append((str(rid), parsed))
    print(f"  파싱 성공: {len(updates):,}건")
    cnt = batch_update_dates(bq, updates, 'shopee')
    print(f"  shopee 완료: {cnt:,}건 업데이트")

    # =====================================================
    # 4. US_TIkTOK (45건 누락)
    # =====================================================
    print("\n[4/5] US_TIkTOK 처리 중...")
    headers, rows = read_sheet(service, 'US_TIkTOK')
    h_map = {h: i for i, h in enumerate(headers)}
    updates = []
    for row in rows:
        rid_idx = h_map.get('review_id', -1)
        date_idx = h_map.get('date', -1)
        rid = row[rid_idx] if rid_idx < len(row) and rid_idx >= 0 else ''
        date_raw = row[date_idx] if date_idx < len(row) and date_idx >= 0 else ''
        parsed = parse_tiktok_date(date_raw)
        if rid and parsed:
            updates.append((rid, parsed))
    print(f"  파싱 성공: {len(updates)}건")
    cnt = batch_update_dates(bq, updates, 'tiktok')
    print(f"  US_TIkTOK 완료: {cnt}건 업데이트")

    # =====================================================
    # 5. 최종 확인
    # =====================================================
    print("\n" + "=" * 80)
    print("최종 date NULL 분포")
    print("=" * 80)
    for r in bq.query(f'''
      SELECT platform, platform_country,
        COUNTIF(date IS NULL) as null_cnt,
        COUNTIF(date IS NOT NULL) as has_cnt,
        COUNT(*) as total
      FROM `{TABLE}`
      GROUP BY platform, platform_country
      ORDER BY platform, platform_country
    ''').result():
        pct = r.null_cnt / r.total * 100 if r.total else 0
        print(f'  {r.platform:8s} {r.platform_country:4s} | null={r.null_cnt:>6,} | has={r.has_cnt:>6,} | null%={pct:.1f}%')

    print("\n완료!")


if __name__ == '__main__':
    main()
