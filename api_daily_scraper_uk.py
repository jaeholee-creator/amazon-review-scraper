"""
Amazon UK Review Scraper - API Mode v2

Strategy:
1. Playwright로 로그인 → 인증된 브라우저 컨텍스트 확보
2. 제품별: 리뷰 페이지 방문 → 네트워크 요청에서 CSRF 캡처
   - CSRF 있음 → API로 모든 페이지 수집 (page 1부터)
   - CSRF 없음 → HTML 파싱 (리뷰 ≤10개, 이미 페이지에 포함)
3. 날짜 필터링 → CSV 저장 → Slack 알림

Usage:
    python api_daily_scraper_uk.py              # 전체 제품 실행
    python api_daily_scraper_uk.py --test       # 테스트 모드 (10페이지)
    python api_daily_scraper_uk.py --limit 3    # 3개 제품만
"""

import asyncio
import csv
import json
import os
import sys
import time
import random
from datetime import date

import requests as http_requests
from bs4 import BeautifulSoup
from src.browser_session import BrowserSession
from src.parser import ReviewParser
from src.slack_notifier import SlackNotifier
from src.sheets_uploader import SheetsUploader
from config.settings_uk import (
    SCRAPER_STATE_FILE,
    USER_AGENTS,
    AMAZON_BASE_URL,
    MIN_DELAY,
    MAX_DELAY,
    MAX_RETRIES,
    get_collection_date_range,
    get_collection_date_range_str,
    get_run_date_str,
    GOOGLE_SHEETS_URL,
    SHEET_NAME,
    API_URL,
)

PRODUCTS_CSV = 'config/products_uk.csv'


# =============================================================================
# API Functions
# =============================================================================

def api_fetch_page(headers: dict, asin: str, page_num: int) -> list:
    """API로 리뷰 한 페이지 가져오기."""
    post_data = {
        'sortBy': 'recent',
        'reviewerType': 'all_reviews',
        'formatType': '',
        'mediaType': '',
        'filterByStar': 'all_stars',
        'filterByAge': '',
        'pageNumber': str(page_num),
        'filterByLanguage': '',
        'filterByKeyword': '',
        'shouldAppend': 'undefined',
        'deviceType': 'desktop',
        'canShowIntHeader': 'undefined',
        'reftag': f'cm_cr_arp_d_paging_btm_next_{page_num}',
        'pageSize': '10',
        'asin': asin,
        'scope': 'reviewsAjax0',
    }

    resp = http_requests.post(API_URL, headers=headers, data=post_data, timeout=15)

    if resp.status_code != 200:
        raise Exception(f"HTTP {resp.status_code}")

    return parse_api_response(resp.text)


def parse_api_response(body: str) -> list:
    """Amazon streaming JSON 응답 파싱 (&&& 구분)."""
    parser = ReviewParser()
    html_parts = []

    for chunk in body.split('&&&'):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            parsed = json.loads(chunk)
            if isinstance(parsed, list) and len(parsed) >= 3:
                if parsed[0] == 'append' and 'review_list' in str(parsed[1]):
                    html_parts.append(parsed[2])
        except (json.JSONDecodeError, IndexError):
            pass

    if not html_parts:
        return []

    soup = BeautifulSoup(''.join(html_parts), 'html.parser')
    return parser.parse_reviews(soup)


# =============================================================================
# Review Collection
# =============================================================================

def filter_reviews(
    reviews: list,
    start_date: date,
    end_date: date,
    collected_ids: set,
) -> tuple:
    """날짜 필터링 + 중복 제거. Returns (filtered_reviews, reached_cutoff)."""
    filtered = []
    reached_cutoff = False

    for r in reviews:
        d = r.get('date_parsed')
        if not d:
            continue
        d = d.date() if hasattr(d, 'date') else d

        if d < start_date:
            reached_cutoff = True
            break
        if d > end_date:
            continue

        rid = r.get('review_id', '')
        if rid and rid in collected_ids:
            continue

        filtered.append(r)

    return filtered, reached_cutoff


def get_api_headers(cookie_str: str, user_agent: str, asin: str, csrf_token: str) -> dict:
    """API 요청 헤더 생성."""
    return {
        'Accept': 'text/html,*/*',
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': f'{AMAZON_BASE_URL}/product-reviews/{asin}?sortBy=recent',
        'Cookie': cookie_str,
        'User-Agent': user_agent,
        'anti-csrftoken-a2z': csrf_token,
    }


def collect_via_api(
    cookie_str: str,
    user_agent: str,
    asin: str,
    csrf_token: str,
    start_date: date,
    end_date: date,
    collected_ids: set,
    max_pages: int = 100,
) -> tuple:
    """API로 모든 페이지 수집. Returns (reviews, status, error_msg)."""
    headers = get_api_headers(cookie_str, user_agent, asin, csrf_token)
    all_reviews = []
    error_count = 0

    for page_num in range(1, max_pages + 1):
        try:
            reviews = api_fetch_page(headers, asin, page_num)

            if not reviews:
                break

            filtered, cutoff = filter_reviews(reviews, start_date, end_date, collected_ids)
            all_reviews.extend(filtered)

            if filtered:
                print(f"   [Page {page_num}] +{len(filtered)} reviews")
            if cutoff:
                print(f"   Date cutoff reached at page {page_num}")
                break

            error_count = 0
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

        except Exception as e:
            error_count += 1
            print(f"   [Page {page_num}] Error: {e}")
            if error_count >= MAX_RETRIES:
                status = 'partial' if all_reviews else 'failed'
                return all_reviews, status, str(e)
            time.sleep(3)

    return all_reviews, 'success', None


# =============================================================================
# Helpers
# =============================================================================

def load_products() -> dict:
    """products.csv에서 ASIN → 제품명 매핑 로드."""
    products = {}
    with open(PRODUCTS_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            products[row['asin']] = row['name']
    return products


def load_state() -> dict:
    if os.path.exists(SCRAPER_STATE_FILE):
        with open(SCRAPER_STATE_FILE, 'r') as f:
            return json.load(f)
    return {'collected_review_ids': [], 'last_run_date': None}


def save_state(state: dict):
    os.makedirs(os.path.dirname(SCRAPER_STATE_FILE), exist_ok=True)
    with open(SCRAPER_STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def save_reviews(reviews: list, output_path: str, append: bool = False):
    """리뷰를 CSV로 저장."""
    if not reviews:
        return

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    fieldnames = [
        'asin', 'review_id', 'rating', 'title', 'author', 'date',
        'date_parsed', 'location', 'verified_purchase', 'content',
        'helpful_count', 'image_count', 'scraped_at'
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
# Main
# =============================================================================

async def main():
    test_mode = '--test' in sys.argv
    limit = None
    if '--limit' in sys.argv:
        idx = sys.argv.index('--limit')
        if idx + 1 < len(sys.argv):
            limit = int(sys.argv[idx + 1])

    products = load_products()
    asin_list = list(products.keys())
    if limit:
        asin_list = asin_list[:limit]

    start_date, end_date = get_collection_date_range()
    date_range_str = get_collection_date_range_str()
    run_date = get_run_date_str()
    max_pages = 10 if test_mode else 100

    state = load_state()
    collected_ids = set(state.get('collected_review_ids', []))

    print(f"\n{'='*60}")
    print("   BIODANCE UK Daily Review Scraper (API v2)")
    print(f"{'='*60}")
    print(f"   Range: {date_range_str}")
    print(f"   Products: {len(asin_list)}")
    print(f"   Known IDs: {len(collected_ids)}")
    print(f"   Mode: {'TEST' if test_mode else 'FULL'}")
    print(f"{'='*60}")

    # Step 1: 세션 초기화 (Single Page Architecture)
    print("\n[Step 1] Session initialization...")
    session = BrowserSession(region='uk')  # UK 설정 사용
    user_agent = random.choice(USER_AGENTS)

    try:
        await session.start()
        await session.login()
        cookie_str = await session.update_cookies()
        print(f"   Session ready. Cookies: {len(cookie_str.split(';'))}")
    except Exception as e:
        print(f"\nFailed to initialize session: {e}")
        await session.close()
        return

    # Step 2: 각 제품 스크래핑
    print("\n[Step 2] Scraping reviews...")
    start_time = time.time()
    results = []
    output_file = f'data/uk/{run_date}/all_reviews_api_uk.csv'
    file_started = False

    try:
        for i, asin in enumerate(asin_list, 1):
            name = products.get(asin, asin)
            print(f"\n{'—'*60}")
            print(f"[{i}/{len(asin_list)}] {name} ({asin})")

            # CSRF 캡처 (같은 페이지에서 이동)
            prep = await session.capture_csrf(asin)

            # 로그인 리다이렉트 → 같은 세션에서 재로그인
            if prep['redirected']:
                print("   Re-login attempt...")
                if not await session.re_login():
                    print("   Re-login failed. Skipping remaining products.")
                    break
                cookie_str = await session.update_cookies()
                prep = await session.capture_csrf(asin)
                if prep['redirected']:
                    print("   Still redirected after re-login. Skipping.")
                    results.append({
                        'asin': asin, 'product_name': name,
                        'reviews': [], 'review_count': 0,
                        'status': 'failed', 'error_message': 'Auth redirect',
                    })
                    continue

            csrf = prep['csrf_token']

            # 쿠키 갱신 (리뷰 페이지 방문 후)
            cookie_str = await session.update_cookies()

            if csrf:
                # API 모드: 모든 페이지를 API로 수집
                reviews, status, error_msg = collect_via_api(
                    cookie_str, user_agent, asin, csrf,
                    start_date, end_date, collected_ids, max_pages,
                )
            else:
                # HTML 모드: 첫 페이지에 모든 리뷰 포함 (≤10개)
                filtered, _ = filter_reviews(
                    prep['html_reviews'], start_date, end_date, collected_ids,
                )
                reviews = filtered
                status = 'success'
                error_msg = None

            # ASIN 태그
            for r in reviews:
                r['asin'] = asin

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

            # CSV 저장 (점진적)
            if reviews:
                save_reviews(reviews, output_file, append=file_started)
                file_started = True

            # 제품 간 대기
            if i < len(asin_list):
                delay = random.uniform(2, 3.5)
                time.sleep(delay)

    finally:
        await session.close()

    elapsed = time.time() - start_time

    # Step 3: 상태 저장
    state['collected_review_ids'] = list(collected_ids)
    state['last_run_date'] = run_date
    save_state(state)

    # Step 4: 최종 요약
    total_reviews = sum(r['review_count'] for r in results)
    success_count = sum(1 for r in results if r['status'] == 'success')
    partial_count = sum(1 for r in results if r['status'] == 'partial')
    failed_count = sum(1 for r in results if r['status'] == 'failed')

    print(f"\n{'='*60}")
    print("   FINAL SUMMARY")
    print(f"{'='*60}")
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

    # Step 5: Google Sheets 업로드
    print("\n[Step 5] Uploading to Google Sheets...")
    try:
        uploader = SheetsUploader(credentials_file='credentials.json')

        # 모든 리뷰 수집
        all_reviews = []
        for result in results:
            all_reviews.extend(result['reviews'])

        if all_reviews:
            upload_result = uploader.upload_reviews(
                spreadsheet_url=GOOGLE_SHEETS_URL,
                sheet_name=SHEET_NAME,
                reviews=all_reviews,
                append=True
            )

            if upload_result['success']:
                print(f"   ✅ Sheets: {upload_result['rows_added']} rows added")
                print(f"   Total rows: {upload_result['total_rows']}")
            else:
                print(f"   ❌ Sheets: {upload_result.get('error', 'Unknown error')}")
        else:
            print(f"   No reviews to upload")

    except FileNotFoundError:
        print(f"   ⚠️ Sheets: credentials.json not found")
        print(f"   Download from: https://console.cloud.google.com/")
    except Exception as e:
        print(f"   ❌ Sheets error: {e}")

    # Step 6: Slack 알림
    print("\n[Step 6] Sending Slack notification...")
    slack = SlackNotifier()
    sent = slack.send_daily_scrape_report(date_range_str, results, elapsed)
    print(f"   Slack: {'sent' if sent else 'failed'}")

    print("\n   Done!")


if __name__ == '__main__':
    asyncio.run(main())
