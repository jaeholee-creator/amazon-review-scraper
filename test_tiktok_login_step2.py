"""www.tiktok.com 선행 로그인만 테스트 (seller center 캡차 우회)"""
import asyncio
import json
import logging
import os
import random
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
sys.path.insert(0, ".")

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()


async def test_tiktok_com_login():
    pw = await async_playwright().start()
    profile_dir = "data/tiktok/browser_profile"  # 메인 프로필 사용 (쿠키 공유)

    ctx = await pw.chromium.launch_persistent_context(
        profile_dir,
        headless=True,
        viewport={"width": 1440, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/133.0.0.0 Safari/537.36"
        ),
        locale="en-US",
        timezone_id="America/New_York",
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-features=IsolateOrigins,site-per-process,SameSiteByDefaultCookies,CookiesWithoutSameSiteMustBeSecure,ThirdPartyCookieBlocking,ImprovedCookieControls",
            "--disable-site-isolation-trials",
            "--disable-web-security",
        ],
        ignore_default_args=["--enable-automation"],
        bypass_csp=True,
    )

    page = ctx.pages[0] if ctx.pages else await ctx.new_page()
    await page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        window.chrome = { runtime: { onConnect: { addListener: function() {} } } };
    """)

    email = os.getenv("TIKTOK_EMAIL", "")
    pwd = os.getenv("TIKTOK_PASSWORD", "")

    # Step 1: www.tiktok.com 로그인
    print("=== www.tiktok.com 로그인 시작 ===")
    await page.goto(
        "https://www.tiktok.com/login/phone-or-email/email",
        wait_until="domcontentloaded",
        timeout=30000,
    )
    await page.wait_for_timeout(3000)

    # 쿠키 배너 닫기
    for cookie_sel in [
        'button:has-text("Accept all")',
        'button:has-text("Decline optional cookies")',
    ]:
        btn = await page.query_selector(cookie_sel)
        if btn and await btn.is_visible():
            await btn.click()
            print(f"쿠키 배너 닫기: {cookie_sel}")
            await page.wait_for_timeout(1000)
            break

    # 이메일 입력 (타이핑 방식)
    email_input = await page.query_selector('input[name="username"]')
    if not email_input:
        print("ERROR: 이메일 필드 없음!")
        await ctx.close()
        await pw.stop()
        return

    await email_input.click()
    await page.wait_for_timeout(300)
    await email_input.fill("")
    for char in email:
        await page.keyboard.press(char)
        await page.wait_for_timeout(random.randint(30, 80))
    print(f"이메일 입력: {email}")

    await page.wait_for_timeout(500)

    # 비밀번호 입력 (타이핑 방식)
    pwd_input = await page.query_selector('input[type="password"]')
    if not pwd_input:
        print("ERROR: 비밀번호 필드 없음!")
        await ctx.close()
        await pw.stop()
        return

    await pwd_input.click()
    await page.wait_for_timeout(300)
    for char in pwd:
        await page.keyboard.press(char)
        await page.wait_for_timeout(random.randint(30, 80))
    print("비밀번호 입력 완료")

    await page.wait_for_timeout(1000)

    # Log in 버튼 클릭
    login_btn = await page.query_selector('button[data-e2e="login-button"]')
    if login_btn:
        await login_btn.click()
        print("Log in 버튼 클릭")
    else:
        await page.keyboard.press("Enter")
        print("Enter 키로 제출")

    # 8초 대기
    await page.wait_for_timeout(8000)
    print(f"URL after login: {page.url}")

    # 에러 메시지 확인
    body_text = await page.evaluate("() => document.body.innerText.substring(0, 2000)")
    if "Maximum number" in body_text:
        print("ERROR: Rate limited!")
        await ctx.close()
        await pw.stop()
        return

    # 캡차 확인
    from utils.captcha_solver import TikTokCaptchaSolver
    solver = TikTokCaptchaSolver(page)
    if await solver.is_captcha_visible():
        print("캡차 감지! 자동 풀기 시도...")
        success = await solver.solve()
        print(f"캡차 결과: {success}")
        await page.wait_for_timeout(3000)
    else:
        print("캡차 없음")

    # 프레임 확인 (캡차가 iframe에 있을 수 있음)
    frames = page.frames
    print(f"Frames: {len(frames)}")
    for f in frames:
        if f.url and "captcha" in f.url.lower() or "verify" in f.url.lower():
            print(f"  캡차 iframe: {f.url}")

    # 스크린샷
    await page.screenshot(path="data/tiktok/debug_step2_after_login.png", full_page=True)
    print("스크린샷 저장")

    # 세션 쿠키 확인
    await page.wait_for_timeout(3000)
    cookies = await ctx.cookies()
    session_cookies = {c["name"]: c["value"][:20] for c in cookies
                      if c["name"] in ("sid_tt", "sessionid", "sessionid_ss", "sid_guard")}
    print(f"세션 쿠키: {session_cookies}")

    has_session = any(c["name"] in ("sessionid", "sid_tt") for c in cookies)

    if not has_session:
        print("세션 쿠키 없음 - 로그인 실패")
        # 페이지 텍스트 출력
        print(f"Body text: {body_text[:500]}")

        # 추가 대기 후 재확인
        for i in range(4):
            await page.wait_for_timeout(5000)
            cookies = await ctx.cookies()
            has_session = any(c["name"] in ("sessionid", "sid_tt") for c in cookies)
            if has_session:
                print(f"세션 쿠키 획득! (대기 {(i+1)*5}초 후)")
                break
            print(f"  대기 중... ({(i+1)*5}초)")

    if has_session:
        print("\n=== www.tiktok.com 로그인 성공! Seller Center 이동 ===")
        await page.goto(
            "https://seller-us.tiktok.com/product/rating?shop_region=US",
            wait_until="domcontentloaded",
            timeout=30000,
        )
        await page.wait_for_timeout(8000)
        print(f"Seller Center URL: {page.url}")

        # 로그인 상태 확인
        if "/account/login" not in page.url and "/account/register" not in page.url:
            print("Seller Center 접근 성공!")
            await page.screenshot(path="data/tiktok/debug_step2_seller_center.png", full_page=True)
            body = await page.evaluate("() => document.body.innerText.substring(0, 500)")
            print(f"Seller Center body: {body[:300]}")
        else:
            print(f"Seller Center 접근 실패: {page.url}")
            await page.screenshot(path="data/tiktok/debug_step2_seller_failed.png", full_page=True)
    else:
        print("\n=== 로그인 최종 실패 ===")

    await ctx.close()
    await pw.stop()


asyncio.run(test_tiktok_com_login())
