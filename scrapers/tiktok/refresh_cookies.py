#!/usr/bin/env python3
"""
TikTok Seller Center 수동 쿠키 갱신 스크립트 (standalone)

로컬 브라우저에서 수동 로그인 후 쿠키를 추출하여 서버에 전송합니다.
patchright 없이 일반 playwright로 동작합니다.

사용법:
    python3 scrapers/tiktok/refresh_cookies.py              # 로컬에서 브라우저 열기 + 쿠키 저장
    python3 scrapers/tiktok/refresh_cookies.py --deploy      # 쿠키를 서버에도 전송
"""
import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

try:
    from playwright.async_api import async_playwright
except ImportError:
    print("❌ playwright가 설치되어 있지 않습니다.")
    print("   pip3 install playwright && python3 -m playwright install chromium")
    sys.exit(1)


SELLER_CENTER_URL = "https://seller-us.tiktok.com"
RATING_PAGE_URL = "https://seller-us.tiktok.com/product/rating?shop_region=US"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data", "tiktok")
COOKIE_FILE = os.path.join(DATA_DIR, "cookies.json")
# SSH alias for the production server
SERVER_SSH = "oracle-cloud"
SERVER_COOKIE_PATH = "/home/ubuntu/scraper/data/tiktok/cookies.json"


async def open_browser_and_wait_for_login():
    """브라우저를 열고 수동 로그인을 기다린 후 쿠키를 저장합니다."""
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
    profile_dir = os.path.join(DATA_DIR, "browser_profile_local")
    Path(profile_dir).mkdir(parents=True, exist_ok=True)

    pw = await async_playwright().start()

    context = await pw.chromium.launch_persistent_context(
        profile_dir,
        headless=False,
        viewport={"width": 1440, "height": 900},
        locale="en-US",
        timezone_id="America/New_York",
    )

    page = context.pages[0] if context.pages else await context.new_page()

    # Seller Center 로그인 페이지로 이동
    print("\n🌐 브라우저가 열립니다. TikTok Seller Center에 수동으로 로그인해주세요.")
    print("   로그인 후 Rating 페이지가 보이면 자동으로 쿠키를 저장합니다.\n")

    await page.goto(SELLER_CENTER_URL + "/account/login", wait_until="domcontentloaded")

    # 로그인 완료 대기 (최대 5분)
    print("⏳ 로그인 대기 중... (최대 5분)")
    logged_in = False
    for i in range(60):  # 5초 × 60 = 5분
        await page.wait_for_timeout(5000)
        current_url = page.url
        if "/account/login" not in current_url and "/account/register" not in current_url:
            if "seller-us.tiktok.com" in current_url:
                logged_in = True
                print(f"✅ 로그인 성공! URL: {current_url[:80]}")
                break
        if (i + 1) % 6 == 0:
            elapsed = (i + 1) * 5
            print(f"   ... {elapsed}초 경과, 로그인 대기 중")

    if not logged_in:
        print("❌ 5분 내 로그인이 완료되지 않았습니다.")
        await context.close()
        await pw.stop()
        return False

    # Rating 페이지 접속 테스트
    print("📋 Rating 페이지 접속 테스트...")
    await page.goto(RATING_PAGE_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(5000)

    if "/account/login" in page.url or "/account/register" in page.url:
        print("❌ Rating 페이지 접근 실패 — 세션이 유효하지 않습니다.")
        await context.close()
        await pw.stop()
        return False

    print("✅ Rating 페이지 접근 확인!")

    # 쿠키 저장
    cookies = await context.cookies()
    with open(COOKIE_FILE, "w", encoding="utf-8") as f:
        json.dump(cookies, f, ensure_ascii=False, indent=2)

    print(f"🍪 쿠키 저장 완료: {len(cookies)}개 → {COOKIE_FILE}")

    await context.close()
    await pw.stop()
    return True


def deploy_cookies_to_server():
    """쿠키 파일을 서버에 SCP로 전송합니다."""
    if not os.path.exists(COOKIE_FILE):
        print(f"❌ 쿠키 파일 없음: {COOKIE_FILE}")
        return False

    print(f"\n📤 서버로 쿠키 전송: {SERVER_SSH}:{SERVER_COOKIE_PATH}")
    try:
        result = subprocess.run(
            ["scp", COOKIE_FILE, f"{SERVER_SSH}:{SERVER_COOKIE_PATH}"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            print("✅ 서버 쿠키 전송 완료!")
            return True
        else:
            print(f"❌ SCP 실패: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ 서버 전송 실패: {e}")
        return False


def main():
    deploy = "--deploy" in sys.argv

    success = asyncio.run(open_browser_and_wait_for_login())
    if not success:
        sys.exit(1)

    if deploy:
        deploy_cookies_to_server()
    else:
        print(f"\n💡 서버에 쿠키를 전송하려면:")
        print(f"   python3 scrapers/tiktok/refresh_cookies.py --deploy")
        print(f"   또는 수동으로: scp {COOKIE_FILE} {SERVER_SSH}:{SERVER_COOKIE_PATH}")


if __name__ == "__main__":
    main()
