"""www.tiktok.com 로그인 디버깅 스크립트"""
import asyncio
import json
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
sys.path.insert(0, ".")

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()


async def debug_tiktok_login():
    pw = await async_playwright().start()
    ctx = await pw.chromium.launch_persistent_context(
        "data/tiktok/browser_profile_debug",
        headless=True,
        viewport={"width": 1440, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/133.0.0.0 Safari/537.36"
        ),
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
    await page.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
    )

    # www.tiktok.com 로그인
    await page.goto(
        "https://www.tiktok.com/login/phone-or-email/email",
        wait_until="domcontentloaded",
        timeout=30000,
    )
    await page.wait_for_timeout(3000)

    email = os.getenv("TIKTOK_EMAIL", "")
    pwd = os.getenv("TIKTOK_PASSWORD", "")

    # 이메일 입력
    email_input = await page.query_selector('input[name="username"]')
    if email_input:
        await email_input.click()
        await email_input.fill(email)
        print(f"이메일 입력: {email}")
    else:
        print("이메일 필드 없음!")

    await page.wait_for_timeout(500)

    # 비밀번호 입력
    pwd_input = await page.query_selector('input[type="password"]')
    if pwd_input:
        await pwd_input.click()
        await pwd_input.fill(pwd)
        print("비밀번호 입력 완료")
    else:
        print("비밀번호 필드 없음!")

    await page.wait_for_timeout(500)

    # 로그인 버튼 클릭
    btn = await page.query_selector('button[data-e2e="login-button"]')
    if btn:
        await btn.click()
        print("Log in 버튼 클릭")
    else:
        print("Log in 버튼 없음!")

    # 8초 대기 (캡차 나타날 시간)
    await page.wait_for_timeout(8000)
    print(f"URL: {page.url}")

    # 스크린샷
    await page.screenshot(path="data/tiktok/debug_tiktok_com_after_login.png", full_page=True)
    print("스크린샷 저장: data/tiktok/debug_tiktok_com_after_login.png")

    # 에러 메시지 확인
    body_text = await page.evaluate("() => document.body.innerText.substring(0, 2000)")
    print(f"\n--- Body Text (처음 500자) ---")
    print(body_text[:500])

    # 모든 iframe 확인
    frames = page.frames
    print(f"\n--- Frames ({len(frames)}개) ---")
    for f in frames:
        print(f"  {f.url}")

    # 캡차 관련 요소
    captcha_info = await page.evaluate("""
        () => {
            const selectors = [
                '#captcha-verify-image',
                '#captcha_container',
                '[class*="captcha"]',
                '[class*="Captcha"]',
                '[id*="captcha"]',
                'iframe[src*="captcha"]',
                'iframe[src*="verify"]',
                '[class*="verify"]',
                '[class*="Verify"]',
                '[class*="puzzle"]',
                '[class*="Puzzle"]',
                '[class*="secsdk"]',
                '.tiktok-captcha',
            ];
            const found = {};
            for (const sel of selectors) {
                try {
                    const els = document.querySelectorAll(sel);
                    if (els.length > 0) {
                        found[sel] = Array.from(els).map(el => ({
                            tag: el.tagName,
                            class: (el.className || '').toString().substring(0, 100),
                            visible: el.offsetParent !== null || el.tagName === 'IFRAME',
                        }));
                    }
                } catch(e) {}
            }
            return found;
        }
    """)
    print(f"\n--- 캡차 요소 ---")
    print(json.dumps(captcha_info, indent=2))

    # 에러 메시지 요소
    error_info = await page.evaluate("""
        () => {
            const errorEls = document.querySelectorAll('[class*="error"], [class*="Error"], [class*="alert"], [class*="Alert"], [class*="warning"], [class*="Warning"]');
            return Array.from(errorEls).map(el => ({
                cls: (el.className || '').toString().substring(0, 100),
                text: el.textContent.substring(0, 200).trim(),
                visible: el.offsetParent !== null,
            })).filter(e => e.visible && e.text.length > 0);
        }
    """)
    print(f"\n--- 에러 요소 ---")
    print(json.dumps(error_info, indent=2, ensure_ascii=False))

    # 세션 쿠키 확인
    cookies = await ctx.cookies()
    session_cookies = [c for c in cookies if c["name"] in ("sid_tt", "sessionid", "sessionid_ss")]
    print(f"\n--- 세션 쿠키 ---")
    for c in session_cookies:
        print(f"  {c['name']}: {c['value'][:20]}...")

    await ctx.close()
    await pw.stop()


asyncio.run(debug_tiktok_login())
