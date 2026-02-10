"""
Amazon Review Scraper - Unified HTML Crawling Mode

API 완전 제거. Playwright HTML 크롤링만 사용.
US/UK 통합 스크립트.

Usage:
    python daily_scraper.py --region us              # US 전체
    python daily_scraper.py --region uk              # UK 전체
    python daily_scraper.py --region all             # US + UK 순차 실행
    python daily_scraper.py --region uk --test       # UK 테스트 (10페이지)
    python daily_scraper.py --region us --limit 3    # US 3개만
"""

import asyncio
import csv
import json
import os
import sys
import time
import random
import aiohttp
import re


# =============================================================================
# Region-aware config loader
# =============================================================================

def load_config(region: str) -> dict:
    """Region에 따라 설정 로드."""
    if region == 'uk':
        from config.settings_uk import (
            SCRAPER_STATE_FILE,
            MIN_DELAY,
            MAX_DELAY,
            GOOGLE_SHEETS_URL,
            SHEET_NAME,
            PRODUCTS_CSV,
            get_collection_date_range,
            get_collection_date_range_str,
            get_run_date_str,
        )
    else:
        from config.settings import (
            SCRAPER_STATE_FILE,
            MIN_DELAY,
            MAX_DELAY,
            GOOGLE_SHEETS_URL,
            SHEET_NAME,
            PRODUCTS_CSV,
            get_collection_date_range,
            get_collection_date_range_str,
            get_run_date_str,
        )

    return {
        'scraper_state_file': SCRAPER_STATE_FILE,
        'min_delay': MIN_DELAY,
        'max_delay': MAX_DELAY,
        'google_sheets_url': GOOGLE_SHEETS_URL,
        'sheet_name': SHEET_NAME,
        'products_csv': PRODUCTS_CSV,
        'get_collection_date_range': get_collection_date_range,
        'get_collection_date_range_str': get_collection_date_range_str,
        'get_run_date_str': get_run_date_str,
    }


# =============================================================================
# Helpers
# =============================================================================

def load_products(csv_path: str) -> dict:
    """products.csv에서 ASIN -> 제품명 매핑 로드."""
    products = {}
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            products[row['asin']] = row['name']
    return products


def load_state(state_file: str) -> dict:
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            return json.load(f)
    return {'collected_review_ids': [], 'last_run_date': None}


def save_state(state: dict, state_file: str):
    os.makedirs(os.path.dirname(state_file), exist_ok=True)
    with open(state_file, 'w') as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def save_reviews(reviews: list, output_path: str, append: bool = False):
    """리뷰를 CSV로 저장."""
    if not reviews:
        return

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    fieldnames = [
        'asin', 'review_id', 'rating', 'title', 'author', 'date',
        'date_parsed', 'location', 'verified_purchase', 'content',
        'helpful_count', 'image_count', 'image_urls', 'video_urls',
        'local_media_files', 'scraped_at'
    ]

    mode = 'a' if append else 'w'
    write_header = not append or not os.path.exists(output_path)

    with open(output_path, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        for review in reviews:
            row = {k: review.get(k, '') for k in fieldnames}
            if row.get('date_parsed') and hasattr(row['date_parsed'], 'isoformat'):
                row['date_parsed'] = row['date_parsed'].isoformat()
            writer.writerow(row)


# =============================================================================
# Media Download
# =============================================================================

async def download_media(reviews: list, media_dir: str) -> int:
    """리뷰에 포함된 이미지/비디오를 다운로드.

    Args:
        reviews: 리뷰 리스트 (image_urls, video_urls 필드 포함)
        media_dir: 미디어 저장 디렉토리

    Returns:
        다운로드한 파일 수
    """
    os.makedirs(media_dir, exist_ok=True)
    download_count = 0

    async with aiohttp.ClientSession() as http:
        for review in reviews:
            review_id = review.get('review_id', 'unknown')
            local_files = []

            # 이미지 다운로드
            image_urls = review.get('image_urls', '')
            if image_urls:
                for idx, url in enumerate(image_urls.split('|')):
                    url = url.strip()
                    if not url:
                        continue
                    ext = _get_extension(url, '.jpg')
                    filename = f"{review_id}_img{idx+1}{ext}"
                    filepath = os.path.join(media_dir, filename)
                    if await _download_file(http, url, filepath):
                        local_files.append(filename)
                        download_count += 1

            # 비디오 다운로드
            video_urls = review.get('video_urls', '')
            if video_urls:
                for idx, url in enumerate(video_urls.split('|')):
                    url = url.strip()
                    if not url:
                        continue
                    ext = _get_extension(url, '.mp4')
                    filename = f"{review_id}_vid{idx+1}{ext}"
                    filepath = os.path.join(media_dir, filename)
                    if await _download_file(http, url, filepath):
                        local_files.append(filename)
                        download_count += 1

            review['local_media_files'] = '|'.join(local_files)

    return download_count


async def _download_file(http: aiohttp.ClientSession, url: str, filepath: str) -> bool:
    """단일 파일 다운로드. 이미 존재하면 스킵."""
    if os.path.exists(filepath):
        return True
    try:
        async with http.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status == 200:
                with open(filepath, 'wb') as f:
                    f.write(await resp.read())
                return True
    except Exception:
        pass
    return False


def _get_extension(url: str, default: str = '.jpg') -> str:
    """URL에서 파일 확장자 추출."""
    match = re.search(r'\.([a-zA-Z0-9]{2,4})(?:\?|$)', url.split('/')[-1])
    if match:
        ext = '.' + match.group(1).lower()
        if ext in ('.jpg', '.jpeg', '.png', '.gif', '.webp', '.mp4', '.webm', '.mov'):
            return ext
    return default


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
    """단일 region 스크래핑 실행."""
    # 설정 로드
    cfg = load_config(region)

    products = load_products(cfg['products_csv'])
    asin_list = list(products.keys())
    if limit:
        asin_list = asin_list[:limit]

    start_date, end_date = cfg['get_collection_date_range']()
    date_range_str = cfg['get_collection_date_range_str']()
    run_date = cfg['get_run_date_str']()
    max_pages = 10 if test_mode else 100

    state = load_state(cfg['scraper_state_file'])
    collected_ids = set(state.get('collected_review_ids', []))

    print(f"\n{'='*60}")
    print(f"   BIODANCE Daily Review Scraper (HTML Crawling)")
    print(f"{'='*60}")
    print(f"   Region: {region.upper()}")
    print(f"   Range: {date_range_str}")
    print(f"   Products: {len(asin_list)}")
    print(f"   Known IDs: {len(collected_ids)}")
    print(f"   Mode: {'TEST' if test_mode else 'FULL'} (max {max_pages} pages)")
    print(f"{'='*60}")

    # Step 1: Google Sheets에서 기존 ID 조회 (중복 방지)
    print("\n[Step 1] Fetching existing review IDs from Google Sheets...")
    try:
        from src.sheets_uploader import SheetsUploader
        uploader = SheetsUploader(credentials_file='credentials.json')
        sheets_ids = uploader.get_existing_review_ids(
            cfg['google_sheets_url'], cfg['sheet_name']
        )
        collected_ids.update(sheets_ids)
        print(f"   Sheets IDs: {len(sheets_ids)} | Total known: {len(collected_ids)}")
    except FileNotFoundError:
        uploader = None
        print("   credentials.json not found. Skipping Sheets ID check.")
    except Exception as e:
        uploader = None
        print(f"   Sheets ID fetch error: {e}")

    # Step 2: 세션 초기화 (Single Page Architecture)
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
    output_file = f'data/{region}/{run_date}/all_reviews.csv'
    file_started = False

    try:
        for i, asin in enumerate(asin_list, 1):
            name = products.get(asin, asin)
            print(f"\n{'—'*60}")
            print(f"[{i}/{len(asin_list)}] {name} ({asin})")

            # HTML 크롤링으로 리뷰 수집
            reviews, status, error_msg = await session.scrape_reviews_html(
                asin, start_date, end_date, collected_ids, max_pages,
            )

            # ID 트래킹
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

            # 미디어 다운로드
            if reviews:
                media_dir = f'data/{region}/{run_date}/media/{asin}'
                media_count = await download_media(reviews, media_dir)
                if media_count:
                    print(f"   -> {media_count} media files downloaded")

            # CSV 저장 (점진적)
            if reviews:
                save_reviews(reviews, output_file, append=file_started)
                file_started = True

            # 제품 간 대기
            if i < len(asin_list):
                delay = random.uniform(cfg['min_delay'], cfg['max_delay'])
                await asyncio.sleep(delay)

    finally:
        await session.close()

    elapsed = time.time() - start_time

    # Step 4: 상태 저장
    state['collected_review_ids'] = list(collected_ids)
    state['last_run_date'] = run_date
    save_state(state, cfg['scraper_state_file'])

    # Step 5: 최종 요약
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

    # Step 6: Google Sheets 업로드
    print("\n[Step 6] Uploading to Google Sheets...")
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

    # Step 7: Slack 알림
    print("\n[Step 7] Sending Slack notification...")
    from src.slack_notifier import SlackNotifier
    slack = SlackNotifier()
    sent = slack.send_daily_scrape_report(date_range_str, results, elapsed)
    print(f"   Slack: {'sent' if sent else 'failed'}")

    print("\n   Done!")


if __name__ == '__main__':
    asyncio.run(main())
