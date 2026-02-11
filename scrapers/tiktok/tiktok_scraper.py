"""
TikTok Shop Seller Center Review Scraper - Playwright 기반 HTML 파싱
"""
import asyncio
import hashlib
import json
import logging
import os
import random
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright, Page, BrowserContext

logger = logging.getLogger(__name__)


class TikTokShopScraper:
    """TikTok Shop Seller Center에서 리뷰를 수집하는 Playwright 기반 스크래퍼"""

    SELLER_CENTER_URL = "https://seller-us.tiktok.com"
    RATING_PAGE_URL = "https://seller-us.tiktok.com/product/rating?shop_region=US"
    LOGIN_URL = "https://seller-us.tiktok.com/account/login"

    # Rate limiting
    MIN_DELAY = 2.0
    MAX_DELAY = 4.0

    def __init__(
        self,
        email: str,
        password: str,
        data_dir: str = "data/tiktok",
        headless: bool = True,
        gmail_imap_email: str = "",
        gmail_imap_app_password: str = "",
    ):
        """
        Args:
            email: TikTok Seller Center 로그인 이메일
            password: 비밀번호
            data_dir: 데이터 저장 디렉토리 (쿠키, 로그 등)
            headless: 헤드리스 모드 여부
            gmail_imap_email: Gmail IMAP 이메일 주소 (인증 코드 자동 읽기용)
            gmail_imap_app_password: Gmail App Password (2FA 후 생성)
        """
        self.email = email
        self.password = password
        self.data_dir = data_dir
        self.headless = headless
        self.gmail_imap_email = gmail_imap_email
        self.gmail_imap_app_password = gmail_imap_app_password

        self._playwright = None
        self._browser = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None

        # 데이터 디렉토리 생성
        Path(data_dir).mkdir(parents=True, exist_ok=True)

    # =========================================================================
    # Browser Lifecycle
    # =========================================================================

    async def start(self) -> bool:
        """Playwright 브라우저 시작 및 로그인"""
        logger.info("Playwright 브라우저 시작...")

        self._playwright = await async_playwright().start()

        # 영구 브라우저 프로필 사용 (캡차 회피: 동일 브라우저로 인식)
        profile_dir = os.path.join(self.data_dir, "browser_profile")
        Path(profile_dir).mkdir(parents=True, exist_ok=True)

        # headless 모드 결정: DISPLAY 환경변수가 있으면 headed(Xvfb) 사용
        use_headless = self.headless
        display = os.environ.get("DISPLAY", "")
        if use_headless and display:
            logger.info(f"DISPLAY={display} 감지 → Xvfb headed 모드로 전환")
            use_headless = False

        self._context = await self._playwright.chromium.launch_persistent_context(
            profile_dir,
            headless=use_headless,
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
                # SSO iframe 크로스 오리진 지원
                "--disable-features=IsolateOrigins,site-per-process,SameSiteByDefaultCookies,CookiesWithoutSameSiteMustBeSecure,ThirdPartyCookieBlocking,ImprovedCookieControls",
                "--disable-site-isolation-trials",
                "--disable-web-security",
            ],
            ignore_default_args=["--enable-automation"],
            bypass_csp=True,
        )
        self._browser = None  # persistent context는 browser 객체 없음

        self._page = self._context.pages[0] if self._context.pages else await self._context.new_page()

        # playwright-stealth 적용 (봇 탐지 우회)
        try:
            from playwright_stealth import stealth_async
            await stealth_async(self._page)
            logger.info("playwright-stealth 적용 완료")
        except ImportError:
            logger.warning("playwright-stealth 미설치 - 수동 스텔스 적용")
            await self._apply_manual_stealth()

        # 추가 CDP 흔적 제거 (stealth가 커버하지 못하는 영역)
        await self._context.add_init_script("""
            // CDP Runtime.enable 흔적 제거
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;

            // permissions.query 오버라이드 (headless 탐지 방지)
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );

            // WebGL vendor/renderer spoofing
            const getParameter = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(parameter) {
                if (parameter === 37445) return 'Intel Inc.';
                if (parameter === 37446) return 'Intel Iris OpenGL Engine';
                return getParameter.call(this, parameter);
            };
        """)

        # 로그인 시도
        logged_in = await self._ensure_logged_in()
        return logged_in

    async def _apply_manual_stealth(self):
        """playwright-stealth 미설치 시 수동 스텔스 적용 (폴백)"""
        await self._context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.chrome = {
                runtime: { onConnect: { addListener: function() {} } },
                loadTimes: function() { return {} },
                csi: function() { return {} },
            };
            Object.defineProperty(navigator, 'plugins', {
                get: () => [
                    { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
                    { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
                    { name: 'Native Client', filename: 'internal-nacl-plugin' },
                ]
            });
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
            Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
            Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
            Object.defineProperty(navigator, 'maxTouchPoints', { get: () => 0 });
        """)

    async def close(self):
        """브라우저 종료"""
        try:
            if self._context:
                await self._context.close()
        except Exception as e:
            logger.debug(f"브라우저 컨텍스트 종료 중 오류 (무시): {e}")
        try:
            if self._playwright:
                await self._playwright.stop()
        except Exception as e:
            logger.debug(f"Playwright 종료 중 오류 (무시): {e}")
        logger.info("브라우저 종료 완료")

    # =========================================================================
    # Cookie / Session Management
    # =========================================================================
    # 영구 브라우저 프로필 사용 → 쿠키/localStorage/sessionStorage 자동 유지
    # 별도 쿠키 파일 저장 불필요

    # =========================================================================
    # Login
    # =========================================================================

    async def _ensure_logged_in(self) -> bool:
        """로그인 상태 확인 및 필요시 로그인 수행 (최대 2회 시도)"""
        page = self._page

        # Rating 페이지로 이동하여 세션 확인
        logger.info("Seller Center 접속 시도...")
        await page.goto(self.RATING_PAGE_URL, wait_until="domcontentloaded", timeout=30000)

        # SSO 리다이렉트 완료 대기
        await page.wait_for_timeout(5000)
        for _ in range(4):
            await page.wait_for_timeout(5000)
            if await self._is_logged_in():
                logger.info("기존 세션으로 로그인 확인됨 (캡차/로그인 건너뜀)")
                await self._dismiss_popups()
                return True
            current_url = page.url
            if "/account/login" in current_url or "/account/register" in current_url:
                break

        logger.info("세션 만료. 재로그인 진행...")

        # 최대 2회 로그인 시도
        for attempt in range(1, 3):
            logger.info(f"로그인 시도 {attempt}/2")
            success = await self._do_login()
            if success:
                return True
            if attempt < 2:
                logger.info("로그인 실패 - 5초 후 재시도")
                await page.wait_for_timeout(5000)

        # 2회 모두 실패 시 Slack 알림
        self._notify_captcha_failure()
        return False

    async def _is_logged_in(self) -> bool:
        """현재 페이지에서 로그인 상태 확인 (URL + 페이지 콘텐츠 기반).

        중요: URL 패턴만으로 판단하지 않는다. TikTok은 미인증 상태에서도
        /product/rating URL을 유지하면서 공개 마케팅 페이지를 보여줄 수 있다.
        반드시 페이지 콘텐츠도 함께 검증한다.
        """
        page = self._page
        current_url = page.url

        # 로그인/회원가입 페이지면 확실히 미로그인
        if "/account/login" in current_url or "/account/register" in current_url:
            return False

        # 페이지 콘텐츠 기반 확인 (공개 페이지 vs 인증 페이지 구분)
        try:
            body_text = await page.evaluate("() => document.body.innerText.substring(0, 1000)")

            # 공개 마케팅 페이지 감지 (복수 시그널)
            public_signals = ["Join now", "Sign up", "Get $", "New Seller Rewards"]
            public_count = sum(1 for s in public_signals if s in body_text)
            if public_count >= 2:
                logger.info(f"공개 페이지 감지 (미로그인, 시그널={public_count}). URL: {current_url}")
                return False

            # 로그인 폼 감지
            if "Log in" in body_text and "Forgot the password" in body_text:
                logger.info(f"로그인 폼 감지 (미로그인). URL: {current_url}")
                return False
        except Exception:
            pass

        # 비밀번호 입력창이 있으면 미로그인
        password_input = await page.query_selector('input[type="password"]')
        if password_input:
            return False

        # Seller Center 인증 요소 확인 (사이드바 네비게이션 등)
        # 이것이 있으면 확실히 로그인 상태
        seller_indicators = await page.query_selector(
            '[class*="sidebar"], [class*="Sidebar"], '
            '[class*="navigation"], [class*="Navigation"], '
            '[class*="shopName"], [class*="ShopName"]'
        )
        if seller_indicators:
            return True

        # URL 패턴 + 페이지 길이 기반 판단
        # 인증된 Seller Center 페이지는 최소 5000자 이상의 콘텐츠를 가짐
        if "seller-us.tiktok.com" in current_url:
            logged_in_paths = ["/homepage", "/product/", "/order/", "/dashboard", "/finance/"]
            for path in logged_in_paths:
                if path in current_url:
                    try:
                        body_len = await page.evaluate("() => document.body.innerText.length")
                        if body_len > 5000:
                            return True
                        logger.debug(f"URL 매칭이지만 콘텐츠 짧음 ({body_len}자). 미인증 가능.")
                    except Exception:
                        pass

        return False

    async def _do_login(self) -> bool:
        """이메일/비밀번호 로그인 수행 (www.tiktok.com 우선 → seller center 폴백)

        순서 이유: Seller Center 캡차 실패가 rate limit을 유발하므로,
        캡차 없이 로그인할 수 있는 www.tiktok.com을 먼저 시도한다.
        """
        # 방법 1: www.tiktok.com 선행 로그인 (캡차 없이 로그인 가능성 높음)
        logger.info("=== 방법 1: www.tiktok.com 선행 로그인 시도 ===")
        success = await self._do_tiktok_com_login()
        if success:
            return True

        # 방법 2: Seller Center 직접 로그인 (SSO iframe + 캡차)
        logger.info("=== 방법 2: Seller Center 직접 로그인 폴백 ===")
        success = await self._do_seller_center_login()
        if success:
            return True

        logger.error("모든 로그인 방법 실패")
        return False

    async def _do_seller_center_login(self) -> bool:
        """Seller Center 직접 로그인 (기존 방식)"""
        page = self._page

        try:
            await page.goto(self.LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)

            # "Log in" 탭으로 전환 (기본이 Sign up일 수 있음)
            for selector in [
                'a[href*="/account/login"]',
                'div:has-text("Log in") >> nth=0',
                'span:has-text("Log in")',
                'button:has-text("Log in")',
            ]:
                login_tab = await page.query_selector(selector)
                if login_tab:
                    await login_tab.click()
                    logger.info(f"Log in 탭 클릭: {selector}")
                    await page.wait_for_timeout(2000)
                    break

            # Email 탭 선택
            email_tab = await page.query_selector('[data-tid="emailTab"], [id*="email"]')
            if email_tab:
                await email_tab.click()
                await page.wait_for_timeout(1000)

            # 이메일 입력 (React controlled component 대응)
            email_input = await page.query_selector(
                'input[name="email"], input[type="email"], input[placeholder*="email" i]'
            )
            if email_input:
                await self._react_safe_input(email_input, self.email)
                logger.info(f"이메일 입력: {self.email}")
            else:
                logger.error("이메일 입력 필드를 찾을 수 없음")
                return False

            # 비밀번호 입력 (React controlled component 대응)
            password_input = await page.query_selector(
                'input[name="password"], input[type="password"]'
            )
            if password_input:
                await self._react_safe_input(password_input, self.password)
                logger.info("비밀번호 입력 완료")
            else:
                logger.error("비밀번호 입력 필드를 찾을 수 없음")
                return False

            # 입력값 검증 (React state 동기화 확인)
            email_value = await email_input.evaluate("el => el.value")
            password_value = await password_input.evaluate("el => el.value")
            if email_value != self.email or password_value != self.password:
                logger.warning(f"입력값 불일치 감지 - 재입력 시도 (email: '{email_value}' vs '{self.email}')")
                await self._react_safe_input(email_input, self.email, force_events=True)
                await self._react_safe_input(password_input, self.password, force_events=True)

            # 로그인 버튼 클릭 (다양한 방법 시도)
            login_clicked = await self._click_login_button()
            if not login_clicked:
                logger.error("로그인 버튼을 찾을 수 없음")
                return False

            await page.wait_for_timeout(5000)
            logger.info(f"로그인 버튼 클릭 후 URL: {page.url}")

            # 네트워크 요청 확인: 인증 API 호출이 발생했는지 체크
            # SSO iframe이 로드되지 않으면 인증 API 호출 0건
            auth_api_detected = await page.evaluate("""
                () => {
                    const entries = performance.getEntriesByType('resource');
                    const authCalls = entries.filter(e =>
                        e.name.includes('/passport/') ||
                        e.name.includes('/api/login') ||
                        e.name.includes('/sso/') ||
                        e.name.includes('/ucenter_web/')
                    );
                    return authCalls.length;
                }
            """)
            logger.info(f"인증 API 호출 수: {auth_api_detected}")

            if auth_api_detected == 0:
                logger.warning("인증 API 호출 0건 - SSO iframe 로딩 실패 가능성 높음")
                # 10초 더 대기 후 재확인
                await page.wait_for_timeout(10000)
                if not await self._is_logged_in():
                    logger.warning("Seller Center 직접 로그인 실패 (SSO iframe 문제)")
                    return False

            # 캡차 + 인증 코드 처리 (rate limit 방지: 최대 2라운드)
            for captcha_round in range(2):
                captcha_passed = await self._handle_captcha()

                if not captcha_passed:
                    if not self.headless or os.environ.get("DISPLAY", ""):
                        # headed 또는 Xvfb 모드: 수동 캡차 풀기 대기 (최대 60초)
                        logger.info("캡차를 수동으로 풀어주세요 (최대 60초 대기)")
                        for _ in range(12):
                            await page.wait_for_timeout(5000)
                            if await self._needs_verification() or await self._is_logged_in():
                                break
                    else:
                        logger.error("Headless 모드에서 캡차 자동 풀기 실패")
                        self._notify_captcha_failure()
                        return False

                await page.wait_for_timeout(3000)
                current_url = page.url
                logger.info(f"캡차 라운드 {captcha_round + 1} 완료. URL: {current_url}")

                if "/account/login" not in current_url:
                    break

                has_captcha_text = False
                try:
                    body = await page.evaluate("() => document.body.innerText")
                    has_captcha_text = "Drag the slider" in body or "drag the slider" in body
                except Exception:
                    pass

                if has_captcha_text:
                    logger.warning(f"캡차가 여전히 존재 (라운드 {captcha_round + 1}). 재시도...")
                    await page.wait_for_timeout(2000)
                    continue

                if await self._needs_verification():
                    break
                await page.wait_for_timeout(3000)

            logger.info(f"캡차 처리 후 최종 URL: {page.url}")

            # 인증 코드 처리
            if await self._needs_verification():
                logger.info("이메일 인증 코드 필요 - 대기 중...")
                verified = await self._wait_for_verification(timeout=300)
                if not verified:
                    logger.error("인증 코드 타임아웃")
                    return False

            # 로그인 완료 대기
            for i in range(10):
                await page.wait_for_timeout(3000)
                current_url = page.url

                if "/account/register" in current_url:
                    logger.info("register 리다이렉트 감지 - Seller Center 메인으로 재이동")
                    await page.goto(self.SELLER_CENTER_URL, wait_until="domcontentloaded", timeout=30000)
                    await page.wait_for_timeout(5000)
                    if await self._is_logged_in():
                        logger.info("로그인 성공! (register 리다이렉트 우회)")
                        return True
                    continue

                if await self._is_logged_in():
                    logger.info("로그인 성공!")
                    return True

                if "/account/login" in current_url and await self._needs_verification():
                    logger.info("로그인 대기 중 인증 코드 감지")
                    verified = await self._wait_for_verification(timeout=300)
                    if verified:
                        continue

                logger.info(f"  로그인 대기 중... URL: {current_url[:80]}")

            # Rating 페이지로 직접 이동 시도
            await page.goto(self.RATING_PAGE_URL, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(8000)

            if await self._is_logged_in():
                logger.info("로그인 성공! (Rating 페이지 이동 후)")
                return True

            logger.warning(f"Seller Center 직접 로그인 실패. URL: {page.url}")
            await page.screenshot(path=f"{self.data_dir}/debug_seller_login_failed.png", full_page=True)
            return False

        except Exception as e:
            logger.error(f"Seller Center 로그인 오류: {e}")
            return False

    async def _react_safe_input(self, element, text: str, force_events: bool = False):
        """
        React controlled component에 안전하게 텍스트 입력.
        fill()이 React state를 업데이트하지 못하는 문제를 우회합니다.
        """
        page = self._page

        # 요소 클릭으로 포커스 확보
        await element.click()
        await page.wait_for_timeout(200)

        # 기존 값 클리어 (Ctrl+A → Delete)
        await page.keyboard.press("Control+a")
        await page.keyboard.press("Delete")
        await page.wait_for_timeout(100)

        if force_events:
            # JS로 React 이벤트 시스템 직접 트리거
            await element.evaluate("""
                (el, text) => {
                    const nativeInputValueSetter = Object.getOwnPropertyDescriptor(
                        window.HTMLInputElement.prototype, 'value'
                    ).set;
                    nativeInputValueSetter.call(el, text);
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                }
            """, text)
        else:
            # press_sequentially로 실제 키보드 이벤트 발생 (React가 인식)
            await element.press_sequentially(text, delay=50)

        await page.wait_for_timeout(300)

    async def _click_login_button(self) -> bool:
        """로그인 버튼 클릭 (다양한 방법 시도)"""
        page = self._page

        # 방법 1: 셀렉터로 직접 클릭
        for selector in [
            'button[type="submit"]',
            'button:has-text("Log in")',
            'button:has-text("Continue")',
        ]:
            btn = await page.query_selector(selector)
            if btn and await btn.is_visible():
                await btn.click()
                logger.info(f"로그인 버튼 클릭: {selector}")
                return True

        # 방법 2: Enter 키
        logger.info("로그인 버튼을 찾지 못함 - Enter 키로 시도")
        await page.keyboard.press("Enter")
        return True

    def _notify_captcha_failure(self):
        """캡차 실패 시 Slack 알림 전송"""
        try:
            from src.slack_notifier import SlackNotifier
            notifier = SlackNotifier()
            notifier.send_error_alert(
                "TikTok Seller Center 캡차 풀기 실패!\n"
                "headless 모드에서 캡차가 나타났습니다.\n"
                "Xvfb headed 모드 또는 Euler Stream API 설정을 확인해주세요."
            )
        except Exception as e:
            logger.warning(f"Slack 알림 전송 실패: {e}")

    async def _do_tiktok_com_login(self) -> bool:
        """www.tiktok.com에서 선행 로그인 후 Seller Center로 이동 (SSO iframe 우회)"""
        page = self._page
        TIKTOK_LOGIN_URL = "https://www.tiktok.com/login/phone-or-email/email"

        try:
            logger.info(f"www.tiktok.com 로그인 페이지 이동: {TIKTOK_LOGIN_URL}")
            await page.goto(TIKTOK_LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)

            # 쿠키 배너 닫기
            for cookie_sel in [
                'button:has-text("Accept all")',
                'button:has-text("Accept")',
                'button:has-text("Decline optional cookies")',
            ]:
                cookie_btn = await page.query_selector(cookie_sel)
                if cookie_btn and await cookie_btn.is_visible():
                    await cookie_btn.click()
                    logger.info(f"쿠키 배너 닫기: {cookie_sel}")
                    await page.wait_for_timeout(1000)
                    break

            # 현재 URL 확인 (리다이렉트 될 수 있음)
            current_url = page.url
            logger.info(f"로그인 페이지 URL: {current_url}")

            # 이메일 입력 필드 찾기
            email_input = None
            for sel in [
                'input[name="username"]',
                'input[placeholder*="email" i]',
                'input[placeholder*="Email" i]',
                'input[type="text"]',
            ]:
                email_input = await page.query_selector(sel)
                if email_input and await email_input.is_visible():
                    logger.info(f"이메일 필드 발견: {sel}")
                    break
                email_input = None

            if not email_input:
                logger.error("www.tiktok.com 이메일 필드를 찾을 수 없음")
                await page.screenshot(path=f"{self.data_dir}/debug_tiktok_login_page.png", full_page=True)
                return False

            # 이메일 입력 (타이핑 방식으로 봇 탐지 우회)
            await email_input.click()
            await page.wait_for_timeout(300)
            await email_input.fill("")
            for char in self.email:
                await page.keyboard.press(char)
                await page.wait_for_timeout(random.randint(30, 80))
            logger.info(f"이메일 입력 완료: {self.email}")

            await page.wait_for_timeout(500)

            # 비밀번호 입력 필드 찾기
            password_input = await page.query_selector('input[type="password"]')
            if not password_input:
                logger.error("www.tiktok.com 비밀번호 필드를 찾을 수 없음")
                return False

            # 비밀번호 입력 (타이핑 방식)
            await password_input.click()
            await page.wait_for_timeout(300)
            for char in self.password:
                await page.keyboard.press(char)
                await page.wait_for_timeout(random.randint(30, 80))
            logger.info("비밀번호 입력 완료")

            await page.wait_for_timeout(1000)

            # Log in 버튼 클릭
            login_btn = None
            for btn_sel in [
                'button[data-e2e="login-button"]',
                'button[type="submit"]',
                'button:has-text("Log in")',
            ]:
                btn = await page.query_selector(btn_sel)
                if btn and await btn.is_visible():
                    # "Log in with" 같은 소셜 로그인 버튼 제외
                    btn_text = await btn.text_content()
                    if btn_text and "with" not in btn_text.lower():
                        login_btn = btn
                        logger.info(f"로그인 버튼 발견: {btn_sel} (텍스트: {btn_text})")
                        break

            if not login_btn:
                # 폼 제출로 폴백
                logger.info("로그인 버튼 미발견 - Enter 키로 제출")
                await page.keyboard.press("Enter")
            else:
                await login_btn.click()
                logger.info("로그인 버튼 클릭")

            await page.wait_for_timeout(5000)
            logger.info(f"로그인 버튼 클릭 후 URL: {page.url}")

            # 에러 메시지 확인 (rate limit, 잘못된 자격증명 등)
            error_msg = await self._check_login_error(page)
            if error_msg:
                logger.error(f"www.tiktok.com 로그인 에러: {error_msg}")
                await page.screenshot(path=f"{self.data_dir}/debug_tiktok_com_error.png", full_page=True)
                # Rate limit 시 TikTok 쿠키 정리 (다음 실행에서 깨끗한 상태로 시작)
                if "Rate limit" in error_msg:
                    await self._clear_tiktok_cookies()
                return False

            # 캡차 처리 (www.tiktok.com에서도 슬라이더 캡차 가능)
            for captcha_round in range(2):  # rate limit 방지: 최대 2라운드
                captcha_passed = await self._handle_captcha()
                if not captcha_passed:
                    if not self.headless:
                        logger.info("캡차를 수동으로 풀어주세요 (최대 60초 대기)")
                        for _ in range(12):
                            await page.wait_for_timeout(5000)
                            url = page.url
                            if "/login" not in url or await self._needs_verification():
                                break
                    else:
                        logger.warning("Headless 모드에서 캡차 자동 풀기 실패")
                        break

                await page.wait_for_timeout(3000)
                current_url = page.url
                logger.info(f"캡차 라운드 {captcha_round + 1} 완료. URL: {current_url}")

                # 에러 메시지 재확인
                error_msg = await self._check_login_error(page)
                if error_msg:
                    logger.error(f"캡차 후 로그인 에러: {error_msg}")
                    return False

                # 로그인 페이지를 벗어났으면 성공
                if "/login" not in current_url:
                    break

                # 인증 코드 필요 확인
                if await self._needs_verification():
                    break

            # 인증 코드 처리
            if await self._needs_verification():
                logger.info("이메일 인증 코드 필요 (www.tiktok.com) - 대기 중...")
                verified = await self._wait_for_verification(timeout=300)
                if not verified:
                    logger.error("인증 코드 타임아웃")
                    return False

            # 로그인 성공 확인: 세션 쿠키 체크
            await page.wait_for_timeout(3000)
            cookies = await self._context.cookies()
            session_cookies = {c["name"]: c["value"] for c in cookies
                             if c["name"] in ("sid_tt", "sessionid", "sessionid_ss", "sid_guard")}
            logger.info(f"세션 쿠키 확인: {list(session_cookies.keys())}")

            has_session = bool(session_cookies.get("sessionid") or session_cookies.get("sid_tt"))

            if not has_session:
                # 추가 대기 (로그인 처리 중일 수 있음)
                for _ in range(6):
                    await page.wait_for_timeout(5000)
                    cookies = await self._context.cookies()
                    session_cookies = {c["name"]: c["value"] for c in cookies
                                     if c["name"] in ("sid_tt", "sessionid", "sessionid_ss", "sid_guard")}
                    has_session = bool(session_cookies.get("sessionid") or session_cookies.get("sid_tt"))
                    if has_session:
                        break
                    current_url = page.url
                    logger.info(f"  세션 대기 중... URL: {current_url[:80]}")

            if not has_session:
                logger.error("www.tiktok.com 로그인 실패 - 세션 쿠키 없음")
                await page.screenshot(path=f"{self.data_dir}/debug_tiktok_com_login_failed.png", full_page=True)
                return False

            logger.info("www.tiktok.com 로그인 성공! 세션 쿠키 획득됨")

            # Seller Center로 이동 (SSO 자동 인증 기대)
            logger.info("Seller Center Rating 페이지로 이동...")
            await page.goto(self.RATING_PAGE_URL, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(5000)

            # SSO 리다이렉트 완료 대기
            for i in range(8):
                current_url = page.url
                if await self._is_logged_in():
                    logger.info("Seller Center 로그인 성공! (www.tiktok.com SSO)")
                    return True

                if "/account/register" in current_url:
                    logger.info("register 리다이렉트 - Seller Center 메인으로 재이동")
                    await page.goto(self.SELLER_CENTER_URL, wait_until="domcontentloaded", timeout=30000)
                    await page.wait_for_timeout(5000)
                    if await self._is_logged_in():
                        logger.info("Seller Center 로그인 성공! (register 우회)")
                        return True

                await page.wait_for_timeout(3000)
                logger.info(f"  SSO 대기 중... URL: {current_url[:80]}")

            # 마지막 시도: Rating 페이지 직접 이동
            await page.goto(self.RATING_PAGE_URL, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(8000)

            if await self._is_logged_in():
                logger.info("Seller Center 로그인 성공! (최종 시도)")
                return True

            logger.error(f"www.tiktok.com 로그인 후 Seller Center 접근 실패. URL: {page.url}")
            await page.screenshot(path=f"{self.data_dir}/debug_tiktok_sso_failed.png", full_page=True)
            return False

        except Exception as e:
            logger.error(f"www.tiktok.com 로그인 오류: {e}")
            return False

    async def _clear_tiktok_cookies(self):
        """TikTok 관련 쿠키를 모두 삭제 (rate limit 상태 초기화)"""
        try:
            if self._context:
                cookies = await self._context.cookies()
                tiktok_cookies = [c for c in cookies
                                 if "tiktok" in c.get("domain", "").lower()]
                if tiktok_cookies:
                    await self._context.clear_cookies()
                    logger.info(f"TikTok 쿠키 {len(tiktok_cookies)}개 삭제 (rate limit 초기화)")
        except Exception as e:
            logger.debug(f"쿠키 삭제 중 오류 (무시): {e}")

    @staticmethod
    async def _check_login_error(page) -> Optional[str]:
        """로그인 페이지에서 에러 메시지를 감지. 에러가 있으면 메시지 반환, 없으면 None."""
        try:
            body_text = await page.evaluate("() => document.body.innerText.substring(0, 3000)")
            # Rate limit
            rate_limit_patterns = [
                "Maximum number of attempts reached",
                "Too many attempts",
                "try again later",
                "rate limit",
                "temporarily locked",
                "temporarily blocked",
                "account is locked",
            ]
            for pattern in rate_limit_patterns:
                if pattern.lower() in body_text.lower():
                    return f"Rate limit: {pattern}"

            # 잘못된 자격증명
            credential_patterns = [
                "Incorrect password",
                "incorrect password",
                "Wrong password",
                "account doesn't exist",
                "Account not found",
                "Invalid email",
            ]
            for pattern in credential_patterns:
                if pattern in body_text:
                    return f"Credentials: {pattern}"

        except Exception:
            pass

        return None

    async def _handle_captcha(self) -> bool:
        """캡차가 있으면 자동으로 풀기. 캡차가 남아있으면 False 반환."""
        try:
            from utils.captcha_solver import TikTokCaptchaSolver

            solver = TikTokCaptchaSolver(self._page)

            if not await solver.is_captcha_visible():
                logger.info("캡차 없음 - 통과")
                return True

            logger.info("슬라이더 캡차 감지 - 자동 풀기 시도")
            success = await solver.solve()

            if success:
                logger.info("캡차 자동 풀기 성공")
                return True

            # 솔버가 실패 보고했지만, 실제로 캡차가 사라졌을 수 있음
            await self._page.wait_for_timeout(2000)
            if not await solver.is_captcha_visible():
                logger.info("캡차 자동 풀기 성공 (지연 확인)")
                return True

            logger.warning("캡차 자동 풀기 실패 - 캡차가 여전히 표시됨")
            return False

        except ImportError:
            logger.warning("캡차 솔버 모듈 없음 (Pillow 설치 필요)")
            return True  # 캡차 없을 수도 있으므로 진행
        except Exception as e:
            logger.warning(f"캡차 처리 중 오류: {e}")
            return True

    async def _dismiss_popups(self):
        """공지사항, 알림, 모달 등의 팝업을 안전하게 닫기.

        주의: 광범위한 CSS 셀렉터는 로그아웃/세션종료 버튼을 오클릭할 수 있으므로
        Arco Design 모달과 명확한 텍스트 버튼만 대상으로 한다.
        """
        page = self._page
        url_before = page.url
        try:
            # 안전한 셀렉터만 사용 (광범위한 [class*="close"] 제거)
            safe_selectors = [
                # Arco Design 모달 닫기 (TikTok Seller Center 주력 UI)
                '.arco-modal-close-icon',
                # 명확한 텍스트 버튼
                'button:has-text("Got it")',
                'button:has-text("I understand")',
                'button:has-text("Dismiss")',
            ]

            dismissed = 0
            for selector in safe_selectors:
                try:
                    elements = await page.query_selector_all(selector)
                    for el in elements:
                        if await el.is_visible():
                            await el.click()
                            dismissed += 1
                            await page.wait_for_timeout(500)
                            # 클릭 후 URL이 바뀌었으면 즉시 중단
                            if page.url != url_before:
                                logger.warning(f"팝업 클릭으로 URL 변경 감지: {page.url}")
                                return
                except Exception:
                    pass

            # Arco 모달이 남아있으면 ESC 시도
            if dismissed == 0:
                modal = await page.query_selector('.arco-modal-wrapper')
                if modal and await modal.is_visible():
                    await page.keyboard.press("Escape")
                    dismissed += 1
                    await page.wait_for_timeout(500)

            if dismissed > 0:
                logger.info(f"팝업/모달 {dismissed}개 닫음")
                await page.wait_for_timeout(1000)

        except Exception as e:
            logger.debug(f"팝업 닫기 중 오류 (무시): {e}")

    async def _needs_verification(self) -> bool:
        """이메일 인증 코드 입력이 필요한지 확인"""
        page = self._page

        # 1. 텍스트 기반 감지 (각각 개별 체크 - text= 셀렉터는 콤마 구분 불가)
        for text_sel in [
            'text="Log in verification"',
            'text="Verification code has been sent"',
            'text="verification code"',
            'text="Can\'t receive the code"',
        ]:
            el = await page.query_selector(text_sel)
            if el:
                logger.info(f"인증 코드 페이지 감지 (텍스트: {text_sel})")
                return True

        # 2. 입력 필드 기반 감지 (CSS 셀렉터는 콤마 가능)
        code_input = await page.query_selector(
            'input[type="tel"], input[aria-label*="code" i], '
            'input[placeholder*="code" i], input[data-index]'
        )
        if code_input:
            logger.info("인증 코드 페이지 감지 (입력 필드)")
            return True

        # 3. 6개 개별 입력 필드 패턴 감지
        all_inputs = await page.query_selector_all(
            'input[maxlength="1"], input[type="tel"]'
        )
        if len(all_inputs) >= 6:
            logger.info(f"인증 코드 페이지 감지 (입력 필드 {len(all_inputs)}개)")
            return True

        return False

    async def _wait_for_verification(self, timeout: int = 300) -> bool:
        """
        이메일 인증 코드 입력 대기.

        우선순위:
        1. 환경변수 TIKTOK_VERIFICATION_CODE가 있으면 자동 입력
        2. Gmail IMAP으로 인증 코드 이메일 자동 읽기 (App Password 설정 시)
        3. 수동 입력 대기 (headless=False 전용)
        """
        page = self._page

        # 1. 환경변수에서 인증 코드 확인
        code = os.environ.get("TIKTOK_VERIFICATION_CODE", "")

        if code:
            logger.info(f"환경변수에서 인증 코드 감지: {code}")
            await self._input_verification_code(code)
            await page.wait_for_timeout(5000)
            return await self._is_logged_in()

        # 2. Gmail IMAP으로 인증 코드 자동 읽기
        if self.gmail_imap_email and self.gmail_imap_app_password:
            code = await self._get_code_from_gmail()
            if code:
                logger.info(f"Gmail IMAP에서 인증 코드 획득: {code}")
                await self._input_verification_code(code)
                await page.wait_for_timeout(5000)
                if await self._is_logged_in():
                    return True
                # 로그인 상태 확인을 위해 추가 대기
                for _ in range(6):
                    await page.wait_for_timeout(3000)
                    if await self._is_logged_in():
                        return True
                logger.warning("Gmail 코드 입력 후에도 로그인 확인 실패")

        # 3. 수동 입력 대기 (headless가 아닐 때)
        if not self.headless:
            logger.info(f"브라우저에서 인증 코드를 수동으로 입력해주세요 (최대 {timeout}초 대기)")
            start = time.time()
            while time.time() - start < timeout:
                await page.wait_for_timeout(3000)
                if await self._is_logged_in():
                    return True
                if "seller-us.tiktok.com" in page.url and "/account/" not in page.url:
                    return True
            return False

        logger.error("Headless 모드에서 인증 코드를 자동으로 입력할 수 없습니다")
        return False

    async def _get_code_from_gmail(self) -> Optional[str]:
        """Gmail IMAP을 사용하여 TikTok 인증 코드를 읽어옵니다."""
        try:
            from utils.gmail_code_reader import GmailVerificationCodeReader

            reader = GmailVerificationCodeReader(
                imap_email=self.gmail_imap_email,
                imap_app_password=self.gmail_imap_app_password,
            )

            logger.info("Gmail IMAP으로 인증 코드 이메일 폴링 시작...")
            code = await reader.async_wait_for_verification_code(timeout=120, poll_interval=5)
            return code

        except Exception as e:
            logger.error(f"Gmail IMAP 인증 코드 읽기 실패: {e}")
            return None

    async def _input_verification_code(self, code: str):
        """6자리 인증 코드를 입력"""
        page = self._page
        logger.info(f"인증 코드 입력 시도: {code}")

        # 디버깅: 페이지 내 모든 input 요소 파악
        input_info = await page.evaluate("""
            () => {
                const inputs = document.querySelectorAll('input');
                return Array.from(inputs).map(el => ({
                    type: el.type,
                    name: el.name,
                    id: el.id,
                    maxlength: el.maxLength,
                    cls: (el.className || '').substring(0, 100),
                    placeholder: el.placeholder,
                    visible: el.offsetParent !== null,
                }));
            }
        """)
        logger.info(f"페이지 내 input 요소: {input_info}")

        # 방법 1: 모든 visible input 중 코드 입력용 필드 찾기
        # (maxlength=1인 개별 필드 또는 maxlength=6인 단일 필드)
        code_inputs = await page.evaluate("""
            () => {
                const inputs = Array.from(document.querySelectorAll('input'));
                const visible = inputs.filter(el => el.offsetParent !== null);
                // maxlength=1 패턴 (6개 개별 필드)
                const singleChar = visible.filter(el => el.maxLength === 1);
                if (singleChar.length >= 6) return { type: 'individual', count: singleChar.length };
                // type=tel 패턴
                const telInputs = visible.filter(el => el.type === 'tel');
                if (telInputs.length >= 6) return { type: 'tel', count: telInputs.length };
                // 그 외 코드 입력 가능한 필드
                const codeInput = visible.find(el =>
                    el.maxLength === 6 || el.type === 'tel' || el.type === 'number'
                );
                if (codeInput) return { type: 'single', index: visible.indexOf(codeInput) };
                return { type: 'unknown', count: visible.length };
            }
        """)
        logger.info(f"코드 입력 필드 분석: {code_inputs}")

        # 방법 2: 가장 확실한 접근 - 첫 번째 보이는 input 클릭 후 키보드 입력
        # OTP 컴포넌트는 첫 필드에 포커스 후 숫자 입력 시 자동 이동
        first_input = await page.evaluate("""
            () => {
                const inputs = Array.from(document.querySelectorAll('input'));
                const visible = inputs.filter(el => el.offsetParent !== null);
                // 이메일/비밀번호 제외
                const codeInputs = visible.filter(el =>
                    el.type !== 'email' && el.type !== 'password' &&
                    !el.name.includes('email') && !el.name.includes('password')
                );
                if (codeInputs.length > 0) {
                    codeInputs[0].focus();
                    codeInputs[0].click();
                    return true;
                }
                return false;
            }
        """)

        if first_input:
            await page.wait_for_timeout(300)
            # 한 글자씩 키보드로 입력 (OTP 자동 이동 트리거)
            for char in code:
                await page.keyboard.press(char)
                await page.wait_for_timeout(150)
            logger.info("인증 코드 키보드 입력 완료")
        else:
            logger.error("인증 코드 입력 필드를 찾을 수 없음")

    # =========================================================================
    # Review Scraping
    # =========================================================================

    async def scrape_reviews(
        self,
        start_date: date,
        end_date: date,
        existing_ids: Optional[set] = None,
        max_pages: int = 50,
    ) -> list[dict]:
        """
        리뷰 페이지에서 HTML 파싱으로 리뷰 수집

        Args:
            start_date: 수집 시작일
            end_date: 수집 종료일
            existing_ids: 기존 수집된 review_id set (중복 방지)
            max_pages: 최대 페이지 수

        Returns:
            파싱된 리뷰 리스트
        """
        if existing_ids is None:
            existing_ids = set()

        page = self._page
        all_reviews = []
        error_count = 0

        # Rating 페이지로 이동 (이미 해당 페이지에 있으면 건너뜀 - 이중 네비게이션 방지)
        logger.info(f"리뷰 수집 시작: {start_date} ~ {end_date}")
        if "/product/rating" not in page.url:
            await page.goto(self.RATING_PAGE_URL, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)
        else:
            logger.info("이미 Rating 페이지에 있음 - 네비게이션 건너뜀")

        # 공지사항/알림 팝업 닫기 (안전한 셀렉터만 사용)
        await self._dismiss_popups()

        # 팝업 처리 후 URL 변경 감지 → 재이동
        current_url = page.url
        if "/product/rating" not in current_url:
            if "/account/login" in current_url or "/account/register" in current_url:
                logger.error(f"팝업 처리 후 세션 만료. URL: {current_url}")
                return all_reviews
            logger.warning(f"Rating 페이지 이탈 감지. 재이동. URL: {current_url}")
            await page.goto(self.RATING_PAGE_URL, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)

        # Rating 페이지에서도 캡차가 나올 수 있음
        await self._handle_captcha()

        # 캡차 처리 후에도 URL 변경 감지
        current_url = page.url
        if "/account/login" in current_url or "/account/register" in current_url:
            logger.error(f"캡차 처리 후 세션 만료. URL: {current_url}")
            return all_reviews
        if "/product/rating" not in current_url:
            logger.warning(f"캡차 후 Rating 이탈. 재이동. URL: {current_url}")
            await page.goto(self.RATING_PAGE_URL, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)

        # 최종 로그인 상태 확인
        if not await self._is_logged_in():
            logger.error(f"세션 만료. URL: {page.url}")
            return all_reviews

        for page_num in range(1, max_pages + 1):
            try:
                # 리뷰 요소 대기
                await page.wait_for_timeout(2000)

                # HTML에서 리뷰 파싱
                reviews = await self._parse_reviews_from_page()

                if not reviews:
                    if page_num == 1:
                        # 첫 페이지에서 리뷰 0건이면 디버깅 정보 출력
                        debug_info = await page.evaluate("""
                            () => {
                                const body = document.body;
                                const allClasses = Array.from(document.querySelectorAll('[class]'))
                                    .map(el => el.className)
                                    .filter(c => typeof c === 'string')
                                    .filter(c => c.toLowerCase().includes('rating') ||
                                                 c.toLowerCase().includes('review') ||
                                                 c.toLowerCase().includes('list') ||
                                                 c.toLowerCase().includes('star') ||
                                                 c.toLowerCase().includes('item'))
                                    .slice(0, 50);
                                return {
                                    url: window.location.href,
                                    title: document.title,
                                    bodyLength: body.innerText.length,
                                    relevantClasses: allClasses,
                                    bodyPreview: body.innerText.substring(0, 500),
                                };
                            }
                        """)
                        logger.warning(f"[Page 1] 리뷰 0건 - 디버깅 정보: {json.dumps(debug_info, ensure_ascii=False, indent=2)}")
                        # 스크린샷 저장
                        await page.screenshot(path=f"{self.data_dir}/debug_no_reviews.png", full_page=True)
                    logger.info(f"[Page {page_num}] 리뷰를 찾을 수 없음. 수집 종료.")
                    break

                # 날짜 필터링 + 중복 제거
                new_reviews = []
                reached_cutoff = False

                for review in reviews:
                    review_date_str = review.get("date", "")
                    if not review_date_str:
                        new_reviews.append(review)
                        continue

                    try:
                        review_date = self._parse_date(review_date_str)
                    except ValueError:
                        new_reviews.append(review)
                        continue

                    if review_date < start_date:
                        logger.info(f"  날짜 기준 초과 ({review_date} < {start_date}). 수집 중단.")
                        reached_cutoff = True
                        break
                    elif review_date > end_date:
                        continue

                    review_id = review.get("review_id", "")
                    if review_id and review_id in existing_ids:
                        continue

                    new_reviews.append(review)
                    if review_id:
                        existing_ids.add(review_id)

                if new_reviews:
                    all_reviews.extend(new_reviews)
                    logger.info(
                        f"[Page {page_num}] +{len(new_reviews)}개 리뷰 | "
                        f"총 누적: {len(all_reviews)}개"
                    )
                else:
                    logger.info(f"[Page {page_num}] 새 리뷰 없음")

                if reached_cutoff:
                    break

                error_count = 0

                # 다음 페이지로 이동
                has_next = await self._go_next_page()
                if not has_next:
                    logger.info("마지막 페이지 도달. 수집 종료.")
                    break

                # Rate limiting
                delay = random.uniform(self.MIN_DELAY, self.MAX_DELAY)
                await page.wait_for_timeout(int(delay * 1000))

            except Exception as e:
                error_count += 1
                logger.error(f"[Page {page_num}] 오류: {e}")
                if error_count >= 3:
                    logger.error("연속 3회 오류. 수집 중단.")
                    break
                await page.wait_for_timeout(3000)

        logger.info(f"리뷰 수집 완료: 총 {len(all_reviews)}개")
        return all_reviews

    async def _parse_reviews_from_page(self) -> list[dict]:
        """현재 페이지의 리뷰를 JavaScript로 파싱 (다단계 폴백 셀렉터)"""
        page = self._page

        # 디버깅: 첫 페이지 HTML 저장 (셀렉터 분석용)
        try:
            html_path = os.path.join(self.data_dir, "rating_page.html")
            if not os.path.exists(html_path):
                html = await page.content()
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(html)
                logger.info(f"Rating 페이지 HTML 저장: {html_path}")
        except Exception as e:
            logger.debug(f"HTML 저장 실패 (무시): {e}")

        reviews = await page.evaluate("""
            () => {
                // 다단계 리뷰 컨테이너 탐색
                // CSS Modules는 클래스명에 해시를 추가하므로 부분 매칭 사용
                const selectors = [
                    '[class*="ratingListItem"]',
                    '[class*="RatingListItem"]',
                    '[class*="rating-list-item"]',
                    '[class*="reviewItem"]',
                    '[class*="ReviewItem"]',
                    '[class*="review-item"]',
                    '[class*="reviewCard"]',
                    '[class*="ReviewCard"]',
                ];

                let items = [];
                for (const sel of selectors) {
                    items = document.querySelectorAll(sel);
                    if (items.length > 0) break;
                }

                // 폴백: 별점(SVG star)을 포함한 반복 요소 탐색
                if (items.length === 0) {
                    // 테이블 행 기반 (Arco Design Table)
                    const tableRows = document.querySelectorAll('.arco-table-tr, tr[class*="Row"]');
                    if (tableRows.length > 0) {
                        items = tableRows;
                    }
                }

                // 최종 폴백: 별점 SVG가 포함된 상위 컨테이너 역추적
                if (items.length === 0) {
                    const starEls = document.querySelectorAll('[class*="star"], [class*="Star"], svg[class*="star"]');
                    const containers = new Set();
                    starEls.forEach(el => {
                        // 별점에서 3~5단계 상위로 올라가서 리뷰 컨테이너 탐색
                        let parent = el;
                        for (let i = 0; i < 5 && parent; i++) {
                            parent = parent.parentElement;
                            if (parent && parent.children.length >= 3) {
                                // 텍스트 콘텐츠가 50자 이상이고 날짜 패턴 포함
                                const text = parent.textContent || '';
                                if (text.length > 50 && /\\d{4}|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec/.test(text)) {
                                    containers.add(parent);
                                    break;
                                }
                            }
                        }
                    });
                    items = Array.from(containers);
                }

                const results = [];

                // 발견된 셀렉터 정보 (디버깅용)
                if (items.length === 0) {
                    // DOM 구조 스냅샷 저장 (디버깅)
                    const bodyClasses = Array.from(document.querySelectorAll('[class]'))
                        .map(el => el.className)
                        .filter(c => typeof c === 'string' && (
                            c.toLowerCase().includes('rating') ||
                            c.toLowerCase().includes('review') ||
                            c.toLowerCase().includes('star') ||
                            c.toLowerCase().includes('list')
                        ))
                        .slice(0, 30);
                    console.log('리뷰 관련 클래스:', JSON.stringify(bodyClasses));
                    return results;
                }

                items.forEach(item => {
                    try {
                        // 별점: 다단계 탐색
                        let activeStars = 0;
                        // 방법 1: activeStar 클래스
                        const starContainer = item.querySelector('[class*="ratingStar"], [class*="RatingStar"], [class*="star-container"], [class*="StarContainer"]');
                        if (starContainer) {
                            activeStars = starContainer.querySelectorAll('[class*="activeStar"], [class*="ActiveStar"], [class*="active-star"], [class*="filled"], [class*="Filled"]').length;
                        }
                        // 방법 2: SVG fill 색상으로 별점 계산
                        if (activeStars === 0) {
                            const svgs = item.querySelectorAll('svg');
                            svgs.forEach(svg => {
                                const fill = svg.getAttribute('fill') || '';
                                const cls = svg.className?.baseVal || svg.className || '';
                                if (fill === '#FFC107' || fill === '#FFB400' || fill.includes('gold') ||
                                    cls.includes('active') || cls.includes('Active') || cls.includes('filled')) {
                                    activeStars++;
                                }
                            });
                        }
                        // 방법 3: aria-label에서 별점 추출 (예: "4 stars")
                        if (activeStars === 0) {
                            const ratingEl = item.querySelector('[aria-label*="star" i]');
                            if (ratingEl) {
                                const match = ratingEl.getAttribute('aria-label').match(/(\\d)/);
                                if (match) activeStars = parseInt(match[1]);
                            }
                        }

                        // 날짜: 다단계 탐색
                        let dateText = '';
                        const dateEl = item.querySelector(
                            '[class*="reviewTime"], [class*="ReviewTime"], [class*="review-time"], ' +
                            '[class*="date"], [class*="Date"], [class*="time"], [class*="Time"]'
                        );
                        if (dateEl) {
                            dateText = dateEl.textContent.trim();
                        } else {
                            // 폴백: 날짜 패턴 텍스트 노드 탐색
                            const walker = document.createTreeWalker(item, NodeFilter.SHOW_TEXT);
                            while (walker.nextNode()) {
                                const t = walker.currentNode.textContent.trim();
                                if (/^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\s+\\d{1,2},\\s+\\d{4}$/.test(t) ||
                                    /^\\d{4}-\\d{2}-\\d{2}$/.test(t)) {
                                    dateText = t;
                                    break;
                                }
                            }
                        }

                        // 리뷰 텍스트
                        let reviewText = '';
                        const textEl = item.querySelector(
                            '[class*="reviewText"], [class*="ReviewText"], [class*="review-text"], ' +
                            '[class*="reviewContent"], [class*="ReviewContent"], [class*="review-content"], ' +
                            '[class*="commentText"], [class*="CommentText"]'
                        );
                        if (textEl) {
                            reviewText = textEl.textContent.trim();
                        }

                        // 응답 수
                        const replyCountEl = item.querySelector('[class*="replyCount"], [class*="ReplyCount"]');
                        const replyCountText = replyCountEl ? replyCountEl.textContent.trim() : '0';

                        // 주문 ID
                        let orderId = '';
                        const orderIdEl = item.querySelector(
                            '[class*="productItemInfoOrderIdText"], [class*="OrderIdText"], ' +
                            '[class*="orderId"], [class*="OrderId"], [class*="order-id"]'
                        );
                        if (orderIdEl) {
                            orderId = orderIdEl.textContent.trim();
                        } else {
                            // 폴백: "Order ID" 텍스트 근처 탐색
                            const allText = item.querySelectorAll('span, div, p');
                            for (const el of allText) {
                                const t = el.textContent.trim();
                                if (t.includes('Order ID') || t.includes('Order:')) {
                                    orderId = t;
                                    break;
                                }
                            }
                        }

                        // 제품 ID
                        let productId = '';
                        const productIdEl = item.querySelector(
                            '[class*="productItemInfoProductId"], [class*="ProductId"], ' +
                            '[class*="productId"], [class*="product-id"]'
                        );
                        if (productIdEl) {
                            productId = productIdEl.textContent.trim();
                        }

                        // 제품명
                        let productName = '';
                        const productNameEl = item.querySelector(
                            '[class*="productItemInfoName"], [class*="ProductName"], ' +
                            '[class*="productName"], [class*="product-name"]'
                        );
                        if (productNameEl) {
                            productName = productNameEl.textContent.trim();
                        }

                        // SKU/변형
                        const skuEl = item.querySelector(
                            '[class*="productItemInfoSku"], [class*="Sku"], [class*="sku"], [class*="variant"]'
                        );
                        const sku = skuEl ? skuEl.textContent.trim() : '';

                        // 사용자명
                        const usernameEl = item.querySelector(
                            '[class*="userNameText"], [class*="UserNameText"], [class*="userName"], ' +
                            '[class*="UserName"], [class*="user-name"], [class*="buyerName"], [class*="BuyerName"]'
                        );
                        const username = usernameEl ? usernameEl.textContent.trim() : '';

                        // 판매자 답변
                        const replyEl = item.querySelector(
                            '[class*="sellerReply"], [class*="SellerReply"], [class*="seller-reply"], ' +
                            '[class*="replyContent"], [class*="ReplyContent"]'
                        );
                        const sellerReply = replyEl ? replyEl.textContent.trim() : '';

                        // 이미지 URL
                        const images = [];
                        const imgEls = item.querySelectorAll('img');
                        imgEls.forEach(img => {
                            const src = img.src || '';
                            // 아바타/아이콘 제외, 리뷰 이미지만
                            if (src && !src.includes('avatar') && !src.includes('icon') &&
                                (src.includes('review') || src.includes('media') || src.includes('image') ||
                                 img.width > 50 || img.naturalWidth > 50)) {
                                images.push(src);
                            }
                        });

                        // 비디오 여부
                        const hasVideo = item.querySelector(
                            '[class*="videoIcon"], [class*="VideoIcon"], [class*="playIcon"], ' +
                            '[class*="PlayIcon"], video, [class*="video"]'
                        ) !== null;

                        results.push({
                            star: activeStars,
                            date: dateText,
                            content: reviewText,
                            reply_count: replyCountText,
                            order_id: orderId,
                            product_id: productId,
                            product_name: productName,
                            sku: sku,
                            username: username,
                            seller_reply: sellerReply,
                            image_urls: images,
                            has_video: hasVideo
                        });
                    } catch (e) {
                        // 개별 리뷰 파싱 실패 시 건너뜀
                    }
                });

                return results;
            }
        """)

        # 후처리: review_id 생성 및 collected_at 추가
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        parsed = []

        for r in reviews:
            # review_id: order_id + product_id 해시 (고유 식별자)
            id_source = f"{r.get('order_id', '')}-{r.get('product_id', '')}-{r.get('username', '')}"
            review_id = hashlib.md5(id_source.encode()).hexdigest()[:16]

            parsed.append({
                "review_id": review_id,
                "collected_at": now_str,
                "product_name": r.get("product_name", ""),
                "product_id": self._clean_id_text(r.get("product_id", "")),
                "order_id": self._clean_id_text(r.get("order_id", "")),
                "author": r.get("username", ""),
                "star": r.get("star", 0),
                "content": r.get("content", ""),
                "date": r.get("date", ""),
                "sku": r.get("sku", ""),
                "seller_reply": r.get("seller_reply", ""),
                "reply_count": r.get("reply_count", "0"),
                "image_urls": ";".join(r.get("image_urls", [])),
                "has_video": r.get("has_video", False),
            })

        return parsed

    async def _go_next_page(self) -> bool:
        """다음 페이지로 이동. 성공 시 True, 마지막 페이지면 False"""
        page = self._page

        try:
            # captcha_container 제거 (포인터 이벤트 차단 방지)
            await page.evaluate("""
                () => {
                    const captcha = document.getElementById('captcha_container');
                    if (captcha) captcha.style.pointerEvents = 'none';
                }
            """)

            # JavaScript로 Next 버튼 클릭 (captcha overlay 우회)
            clicked = await page.evaluate("""
                () => {
                    // Next 버튼 찾기 (Arco Design 페이지네이션)
                    const items = document.querySelectorAll('li');
                    for (const item of items) {
                        const title = item.getAttribute('title') || '';
                        const ariaLabel = item.getAttribute('aria-label') || '';
                        const text = item.textContent.trim();

                        if (title === 'Next' || ariaLabel === 'Next') {
                            // disabled 체크
                            const classes = item.className || '';
                            if (classes.includes('disabled') || item.getAttribute('aria-disabled') === 'true') {
                                return false;
                            }
                            item.click();
                            return true;
                        }
                    }
                    return false;
                }
            """)

            if clicked:
                await page.wait_for_timeout(2000)
                return True

            return False

        except Exception as e:
            logger.warning(f"다음 페이지 이동 실패: {e}")
            return False

    # =========================================================================
    # Utilities
    # =========================================================================

    @staticmethod
    def _clean_id_text(text: str) -> str:
        """ID 텍스트에서 라벨 제거 (예: 'Order ID: 123' → '123')"""
        if ":" in text:
            return text.split(":", 1)[1].strip()
        return text.strip()

    @staticmethod
    def _parse_date(date_str: str) -> date:
        """
        TikTok 날짜 문자열 파싱.
        형식 예: 'Jan 15, 2026', '2026-01-15', 'Feb 8, 2026'
        """
        date_str = date_str.strip()

        formats = [
            "%b %d, %Y",       # Jan 15, 2026
            "%B %d, %Y",       # January 15, 2026
            "%Y-%m-%d",        # 2026-01-15
            "%m/%d/%Y",        # 01/15/2026
            "%d/%m/%Y",        # 15/01/2026
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        raise ValueError(f"날짜 파싱 불가: {date_str}")

    # =========================================================================
    # Main Entry Point
    # =========================================================================

    async def scrape(
        self,
        start_date: date,
        end_date: date,
        existing_ids: Optional[set] = None,
    ) -> dict:
        """
        전체 스크래핑 프로세스 실행

        Args:
            start_date: 수집 시작일
            end_date: 수집 종료일
            existing_ids: 기존 수집된 review_id set

        Returns:
            결과 dict
        """
        logger.info("=" * 60)
        logger.info("TikTok Shop 리뷰 스크래핑 시작")
        logger.info(f"날짜 범위: {start_date} ~ {end_date}")
        logger.info("=" * 60)

        start_time = time.time()

        try:
            # 브라우저 시작 및 로그인
            logged_in = await self.start()
            if not logged_in:
                logger.error("로그인 실패. 스크래핑 중단.")
                return {
                    "status": "failed",
                    "error": "Login failed",
                    "reviews": [],
                    "total_reviews": 0,
                }

            # 리뷰 수집
            reviews = await self.scrape_reviews(
                start_date=start_date,
                end_date=end_date,
                existing_ids=existing_ids,
            )

            elapsed = time.time() - start_time

            logger.info("=" * 60)
            logger.info(f"스크래핑 완료: {len(reviews)}개 리뷰")
            logger.info(f"소요 시간: {elapsed:.1f}초")
            logger.info("=" * 60)

            return {
                "status": "success",
                "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "reviews": reviews,
                "total_reviews": len(reviews),
                "date_range": {
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat(),
                },
                "elapsed_seconds": round(elapsed, 1),
            }

        except Exception as e:
            logger.error(f"스크래핑 오류: {e}", exc_info=True)
            return {
                "status": "failed",
                "error": str(e),
                "reviews": [],
                "total_reviews": 0,
            }

        finally:
            await self.close()
