"""
Amazon Review Scraper - Unified HTML Crawling Mode (Airflow Edition)

CSV/상태파일 제거. Google Sheets를 Single Source of Truth로 사용.
Playwright Chromium 기반, ARM64 호환.

Usage:
    python daily_scraper.py --region us              # US 전체
    python daily_scraper.py --region uk              # UK 전체
    python daily_scraper.py --region all             # US + UK 순차 실행
    python daily_scraper.py --region uk --test       # UK 테스트 (10페이지)
    python daily_scraper.py --region us --limit 3    # US 3개만
"""

import asyncio
import os
import sys
import time
import random
import re


# =============================================================================
# Region-aware config loader
# =============================================================================

def load_config(region: str) -> dict:
    """Region에 따라 설정 로드."""
    if region == 'uk':
        from config.settings_uk import (
            MIN_DELAY,
            MAX_DELAY,
            GOOGLE_SHEETS_URL,
            SHEET_NAME,
            get_collection_date_range,
            get_collection_date_range_str,
            get_run_date_str,
        )
        from config.settings import PRODUCT_NAMES, TOP_5_ASINS
    else:
        from config.settings import (
            MIN_DELAY,
            MAX_DELAY,
            GOOGLE_SHEETS_URL,
            SHEET_NAME,
            PRODUCT_NAMES,
            TOP_5_ASINS,
            get_collection_date_range,
            get_collection_date_range_str,
            get_run_date_str,
        )

    return {
        'min_delay': MIN_DELAY,
        'max_delay': MAX_DELAY,
        'google_sheets_url': GOOGLE_SHEETS_URL,
        'sheet_name': SHEET_NAME,
        'product_names': PRODUCT_NAMES,
        'top_asins': TOP_5_ASINS,
        'get_collection_date_range': get_collection_date_range,
        'get_collection_date_range_str': get_collection_date_range_str,
        'get_run_date_str': get_run_date_str,
    }


# =============================================================================
# Main
# =============================================================================

async def main():
    # CLI 파라미터 파싱
    region = 'us'
    if '--region' in sys.argv:
        idx = sys.argv.index('--region')
        if idx + 1 < len(sys.argv):
            region = sys.argv[idx + 1].lower()

    test_mode = '--test' in sys.argv
    limit = None
    if '--limit' in sys.argv:
        idx = sys.argv.index('--limit')
        if idx + 1 < len(sys.argv):
            limit = int(sys.argv[idx + 1])

    if region not in ('us', 'uk', 'all'):
        print(f"Invalid region: {region}. Use 'us', 'uk', or 'all'.")
        sys.exit(1)

    if region == 'all':
        for r in ('us', 'uk'):
            await run_region(r, test_mode, limit)
        return

    await run_region(region, test_mode, limit)


async def run_region(region: str, test_mode: bool, limit: int | None):
    """단일 region 스크래핑 실행. Google Sheets를 단일 중복 체크 소스로 사용."""
    # 설정 로드
    cfg = load_config(region)

    product_names = cfg['product_names']
    asin_list = list(cfg['top_asins'])
    if limit:
        asin_list = asin_list[:limit]

    start_date, end_date = cfg['get_collection_date_range']()
    date_range_str = cfg['get_collection_date_range_str']()
    max_pages = 10 if test_mode else 100

    print(f"\n{'='*60}")
    print(f"   BIODANCE Daily Review Scraper (Airflow Edition)")
    print(f"{'='*60}")
    print(f"   Region: {region.upper()}")
    print(f"   Range: {date_range_str}")
    print(f"   Products: {len(asin_list)}")
    print(f"   Mode: {'TEST' if test_mode else 'FULL'} (max {max_pages} pages)")
    print(f"{'='*60}")

    # Step 1: Google Sheets에서 기존 ID 조회 (중복 방지 - 단일 소스)
    print("\n[Step 1] Fetching existing review IDs from Google Sheets...")
    collected_ids = set()
    uploader = None
    try:
        from src.sheets_uploader import SheetsUploader
        uploader = SheetsUploader(credentials_file='credentials.json')
        sheets_ids = uploader.get_existing_review_ids(
            cfg['google_sheets_url'], cfg['sheet_name']
        )
        collected_ids.update(sheets_ids)
        print(f"   Sheets IDs: {len(sheets_ids)}")
    except FileNotFoundError:
        print("   credentials.json not found. Skipping Sheets ID check.")
    except Exception as e:
        print(f"   Sheets ID fetch error: {e}")

    # Step 2: 세션 초기화 (Chromium, ARM64 호환)
    print("\n[Step 2] Session initialization...")
    from src.browser_session import BrowserSession
    session = BrowserSession(region=region)

    try:
        await session.start()
        await session.login()
        print("   Session ready.")
    except Exception as e:
        print(f"\nFailed to initialize session: {e}")
        await session.close()
        return

    # Step 3: 각 제품 HTML 크롤링
    print("\n[Step 3] Scraping reviews (HTML crawling)...")
    start_time = time.time()
    results = []

    try:
        for i, asin in enumerate(asin_list, 1):
            name = product_names.get(asin, asin)
            print(f"\n{'—'*60}")
            print(f"[{i}/{len(asin_list)}] {name} ({asin})")

            # HTML 크롤링으로 리뷰 수집
            reviews, status, error_msg = await session.scrape_reviews_html(
                asin, start_date, end_date, collected_ids, max_pages,
            )

            # ID 트래킹 (메모리에서만 관리)
            new_ids = {r['review_id'] for r in reviews if r.get('review_id')}
            collected_ids.update(new_ids)

            print(f"   -> {len(reviews)} reviews ({status})")

            results.append({
                'asin': asin,
                'product_name': name,
                'reviews': reviews,
                'review_count': len(reviews),
                'status': status,
                'error_message': error_msg,
            })

            # 제품 간 대기
            if i < len(asin_list):
                delay = random.uniform(cfg['min_delay'], cfg['max_delay'])
                await asyncio.sleep(delay)

    finally:
        await session.close()

    elapsed = time.time() - start_time

    # Step 4: 최종 요약
    total_reviews = sum(r['review_count'] for r in results)
    success_count = sum(1 for r in results if r['status'] == 'success')
    partial_count = sum(1 for r in results if r['status'] == 'partial')
    failed_count = sum(1 for r in results if r['status'] == 'failed')

    print(f"\n{'='*60}")
    print("   FINAL SUMMARY")
    print(f"{'='*60}")
    print(f"   Region: {region.upper()}")
    print(f"   Range: {date_range_str}")
    print(f"   Total reviews: {total_reviews}")
    print(f"   Time: {elapsed:.1f}s")
    print(f"   Success: {success_count} | Partial: {partial_count} | Failed: {failed_count}")
    print()

    for r in results:
        icon = '  ' if r['status'] == 'success' else '! ' if r['status'] == 'partial' else 'X '
        err = f" ({r['error_message']})" if r.get('error_message') else ""
        print(f"   {icon}{r['product_name']}: {r['review_count']}ea{err}")

    print(f"{'='*60}")

    # Step 5: Google Sheets 업로드 (중복은 이미 Step 1에서 필터링됨)
    print("\n[Step 5] Uploading to Google Sheets...")
    try:
        if uploader is None:
            from src.sheets_uploader import SheetsUploader
            uploader = SheetsUploader(credentials_file='credentials.json')

        all_reviews = []
        for result in results:
            all_reviews.extend(result['reviews'])

        if all_reviews:
            upload_result = uploader.upload_reviews(
                spreadsheet_url=cfg['google_sheets_url'],
                sheet_name=cfg['sheet_name'],
                reviews=all_reviews,
                append=True,
            )

            if upload_result['success']:
                print(f"   Sheets: {upload_result['rows_added']} rows added")
                print(f"   Total rows: {upload_result['total_rows']}")
            else:
                print(f"   Sheets error: {upload_result.get('error', 'Unknown error')}")
        else:
            print("   No reviews to upload")

    except FileNotFoundError:
        print("   credentials.json not found. Skipping upload.")
    except Exception as e:
        print(f"   Sheets error: {e}")

    # Step 6: Slack 알림
    print("\n[Step 6] Sending Slack notification...")
    try:
        from src.slack_notifier import SlackNotifier
        slack = SlackNotifier()
        sent = slack.send_daily_scrape_report(
            date_range_str, results, elapsed,
            channel_name=f'Amazon {region.upper()}',
        )
        print(f"   Slack: {'sent' if sent else 'failed'}")
    except Exception as e:
        print(f"   Slack error: {e}")

    print("\n   Done!")


if __name__ == '__main__':
    asyncio.run(main())
