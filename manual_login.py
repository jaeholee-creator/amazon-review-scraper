"""
수동 로그인 도우미 - 브라우저 창을 열어 직접 로그인 후 쿠키 저장.
CAPTCHA가 나타날 때 사용.

Usage:
    python manual_login.py
"""

import asyncio
import json
from playwright.async_api import async_playwright
from config.settings import AMAZON_BASE_URL, COOKIES_FILE, DATA_DIR
import os


async def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    pw = await async_playwright().start()
    browser = await pw.firefox.launch(headless=False)  # 브라우저 창 표시
    context = await browser.new_context(
        viewport={'width': 1920, 'height': 1080},
        locale='en-US',
        timezone_id='America/New_York',
    )

    page = await context.new_page()

    # Amazon 로그인 페이지로 이동
    print("브라우저 창이 열렸습니다.")
    print("1. Amazon에 로그인해주세요 (CAPTCHA 포함)")
    print("2. 로그인 완료 후 이 터미널에서 Enter를 누르세요.")
    print()

    await page.goto(f'{AMAZON_BASE_URL}/gp/sign-in.html', wait_until='domcontentloaded')

    # 사용자가 수동 로그인 완료할 때까지 대기
    signal_file = f'{DATA_DIR}/login_done.signal'
    if os.path.exists(signal_file):
        os.remove(signal_file)

    print(f">>> 로그인 완료 후 다음 명령을 실행하세요:")
    print(f"    touch {signal_file}")
    print(f">>> 대기 중...")

    while not os.path.exists(signal_file):
        await asyncio.sleep(1)

    os.remove(signal_file)
    print("   신호 감지! 계속 진행합니다...")

    # 로그인 상태 확인
    await page.goto(AMAZON_BASE_URL, wait_until='domcontentloaded')
    await page.wait_for_timeout(3000)

    try:
        account_text = await page.inner_text('#nav-link-accountList')
        if 'Hello' in account_text and 'Sign in' not in account_text:
            print(f"   로그인 확인: {account_text.strip()[:60]}")
        else:
            print(f"   로그인 상태 불확실: {account_text.strip()[:60]}")
    except Exception:
        print("   계정 텍스트를 찾을 수 없습니다.")

    # 리뷰 페이지 접근 테스트
    print("\n리뷰 페이지 접근 테스트...")
    review_url = f'{AMAZON_BASE_URL}/product-reviews/B0B2RM68G2?pageNumber=1&sortBy=recent'
    await page.goto(review_url, wait_until='networkidle', timeout=30000)
    await page.wait_for_timeout(3000)

    url = page.url
    has_reviews = await page.query_selector('[data-hook="review"]')
    print(f"   URL: {url}")
    print(f"   리뷰 존재: {has_reviews is not None}")
    print(f"   리다이렉트: {'/ap/' in url}")

    # 쿠키 저장
    cookies = await context.cookies()
    with open(COOKIES_FILE, 'w') as f:
        json.dump(cookies, f)
    print(f"\n   쿠키 저장 완료: {len(cookies)} entries -> {COOKIES_FILE}")

    await browser.close()
    await pw.stop()

    print("\n이제 api_daily_scraper.py를 실행할 수 있습니다.")
    print("   python3 api_daily_scraper.py --limit 1 --test")


asyncio.run(main())
