"""
TikTok Shop Seller Center Review Scraper - Patchright 기반 HTML 파싱

Patchright: Chromium 바이너리 레벨에서 자동화 마커를 제거하는 Playwright 포크.
playwright-stealth 등 JS 레벨 패치 불필요.
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

from patchright.async_api import async_playwright, Page, BrowserContext

logger = logging.getLogger(__name__)


class TikTokShopScraper:
    """TikTok Shop Seller Center에서 리뷰를 수집하는 Patchright 기반 스크래퍼"""

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
        """Patchright 브라우저 시작 및 로그인"""
        logger.info("Patchright 브라우저 시작...")

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
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        self._browser = None  # persistent context는 browser 객체 없음

        self._page = self._context.pages[0] if self._context.pages else await self._context.new_page()

        # Patchright는 Chromium 바이너리 레벨에서 자동화 마커를 제거하므로
        # playwright-stealth, 수동 stealth, CDP 흔적 제거 등이 불필요

        # JSON 쿠키 복원 (영구 프로필 보완)
        await self._restore_cookies()

        # 로그인 시도
        logged_in = await self._ensure_logged_in()

        # 로그인 성공 시 쿠키 백업
        if logged_in:
            await self._save_cookies()

        return logged_in

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

    @property
    def _cookie_file(self) -> str:
        return os.path.join(self.data_dir, "cookies.json")

    async def _save_cookies(self):
        """현재 브라우저 쿠키를 JSON 파일로 백업"""
        try:
            cookies = await self._context.cookies()
            with open(self._cookie_file, "w", encoding="utf-8") as f:
                json.dump(cookies, f, ensure_ascii=False, indent=2)
            logger.info(f"쿠키 백업 완료: {len(cookies)}개 → {self._cookie_file}")
        except Exception as e:
            logger.warning(f"쿠키 백업 실패: {e}")

    async def _restore_cookies(self) -> bool:
        """JSON 백업에서 쿠키 복원 (영구 프로필 보완)"""
        try:
            if not os.path.exists(self._cookie_file):
                logger.info("쿠키 백업 파일 없음 - 건너뜀")
                return False

            # 파일 수정 시간 확인: 7일 이상 된 쿠키는 무시
            mtime = os.path.getmtime(self._cookie_file)
            age_days = (time.time() - mtime) / 86400
            if age_days > 7:
                logger.info(f"쿠키 백업이 {age_days:.1f}일 전 - 만료됨, 건너뜀")
                return False

            with open(self._cookie_file, "r", encoding="utf-8") as f:
                cookies = json.load(f)

            if not cookies:
                return False

            await self._context.add_cookies(cookies)
            logger.info(f"쿠키 복원 완료: {len(cookies)}개 ({age_days:.1f}일 전 백업)")
            return True

        except Exception as e:
            logger.warning(f"쿠키 복원 실패: {e}")
            return False

    # =========================================================================
    # Login
    # =========================================================================

    async def _ensure_logged_in(self) -> bool:
        """로그인 상태 확인 및 필요시 로그인 수행 (최대 3회, 지수 백오프)"""
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
                # 팝업 닫기 후 TikTok이 세션을 만료시킬 수 있으므로 URL 재확인
                post_url = page.url
                if "/account/login" in post_url or "/account/register" in post_url:
                    logger.warning(f"팝업 처리 후 세션 만료 감지. URL: {post_url}")
                    break  # 재로그인 플로우로 전환
                return True
            current_url = page.url
            if "/account/login" in current_url or "/account/register" in current_url:
                break

        logger.info("세션 만료. 재로그인 진행...")

        # 최대 3회 로그인 시도 (지수 백오프: 5초, 10초, 20초)
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            logger.info(f"로그인 시도 {attempt}/{max_attempts}")
            success = await self._do_login()
            if success:
                # 로그인 성공 시 쿠키 백업
                await self._save_cookies()
                return True
            if attempt < max_attempts:
                backoff = 5 * (2 ** (attempt - 1))  # 5초, 10초
                logger.info(f"로그인 실패 - {backoff}초 후 재시도")
                await page.wait_for_timeout(backoff * 1000)

        # 모든 시도 실패 시 Slack 알림
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

        # URL 패턴 기반 판단 (지연된 리다이렉트 방지를 위해 재확인)
        if "seller-us.tiktok.com" in current_url:
            logged_in_paths = ["/homepage", "/product/", "/order/", "/dashboard", "/finance/"]
            for path in logged_in_paths:
                if path in current_url:
                    # TikTok은 비인증 상태에서도 내부 URL을 잠시 유지한 뒤
                    # JS 비동기로 register 페이지로 리다이렉트할 수 있음.
                    # 3초 대기 후 URL을 재확인하여 false positive 방지.
                    await page.wait_for_timeout(3000)
                    rechecked_url = page.url
                    if "/account/login" in rechecked_url or "/account/register" in rechecked_url:
                        logger.info(f"URL 재확인: 세션 만료 감지 → 미로그인. URL: {rechecked_url[:80]}")
                        return False
                    logger.info(f"URL 매칭으로 로그인 확인: {rechecked_url[:80]}")
                    return True

        return False

    async def _do_login(self) -> bool:
        """이메일/비밀번호 로그인 수행 (순수 키보드 이벤트 사용)"""
        page = self._page

        try:
            # 로그인 페이지로 이동 (register 리다이렉트 대비)
            try:
                await page.goto(self.LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
            except Exception as e:
                # register로 리다이렉트될 수 있음
                logger.info(f"로그인 페이지 이동 중 리다이렉트: {e}")
                await page.wait_for_timeout(3000)

            await page.wait_for_timeout(3000)

            # register 페이지면 "Log in" 링크 클릭
            current_url = page.url
            if "/account/register" in current_url:
                logger.info("Register 페이지 감지 → Log in 링크 클릭")
                # 상단 네비게이션의 "Log in" 링크
                login_link = await page.query_selector('a[href*="/account/login"]')
                if login_link:
                    await login_link.click()
                    await page.wait_for_timeout(3000)
                else:
                    # 텍스트로 찾기
                    for sel in ['text="Log in"', 'a:has-text("Log in")']:
                        el = await page.query_selector(sel)
                        if el:
                            await el.click()
                            await page.wait_for_timeout(3000)
                            break

            # "Log in" 탭 활성화 (Sign up 폼이 기본일 수 있음)
            for selector in [
                'div[role="tab"]:has-text("Log in")',
                'div:has-text("Log in") >> nth=0',
                'span:has-text("Log in")',
            ]:
                try:
                    login_tab = await page.query_selector(selector)
                    if login_tab and await login_tab.is_visible():
                        await login_tab.click()
                        logger.info(f"Log in 탭 클릭: {selector}")
                        await page.wait_for_timeout(2000)
                        break
                except Exception:
                    continue

            # Email 탭 선택 (Phone이 기본일 수 있음)
            email_tab = await page.query_selector('[data-tid="emailTab"], [id*="email"]')
            if email_tab:
                await email_tab.click()
                await page.wait_for_timeout(1000)

            # === 인간 유사 입력: press_sequentially + 랜덤 딜레이 ===
            # fill()은 즉시 입력이라 TikTok 행동 분석에 봇으로 감지됨.
            # 실제 키보드 이벤트(press_sequentially)로 입력해야 함.

            # 이메일 필드 찾기
            email_selectors = [
                'input[name="email"]:visible',
                'input[type="email"]:visible',
                'input[placeholder*="email" i]:visible',
            ]
            email_filled = False
            for sel in email_selectors:
                try:
                    locator = page.locator(sel).first
                    if await locator.count() > 0:
                        email_filled = await self._human_type_field(locator, self.email, "이메일")
                        if email_filled:
                            break
                except Exception as e:
                    logger.debug(f"이메일 셀렉터 {sel} 실패: {e}")

            if not email_filled:
                logger.error("이메일 입력 실패")
                await page.screenshot(path=f"{self.data_dir}/debug_email_fail.png")
                return False

            # 이메일 → 비밀번호 필드 이동 사이 인간적 대기
            await page.wait_for_timeout(random.randint(500, 1200))

            # 비밀번호 필드 찾기
            pw_selectors = [
                'input[type="password"]:visible',
                'input[name="password"]:visible',
            ]
            pw_filled = False
            for sel in pw_selectors:
                try:
                    locator = page.locator(sel).first
                    if await locator.count() > 0:
                        pw_filled = await self._human_type_field(locator, self.password, "비밀번호")
                        if pw_filled:
                            break
                except Exception as e:
                    logger.debug(f"비밀번호 셀렉터 {sel} 실패: {e}")

            if not pw_filled:
                logger.error("비밀번호 입력 실패")
                return False

            # 비밀번호 입력 → Continue 클릭 사이 인간적 대기
            await page.wait_for_timeout(random.randint(800, 1500))

            # 스크린샷: Continue 클릭 전
            await page.screenshot(path=f"{self.data_dir}/debug_before_continue.png")

            # Continue 버튼 클릭 (마우스 이동 + 클릭)
            continue_btn = page.locator('button:has-text("Continue"):visible').first
            if await continue_btn.count() > 0:
                is_disabled = await continue_btn.evaluate("el => el.disabled")
                logger.info(f"Continue 버튼: disabled={is_disabled}")
                # 버튼 위로 마우스 이동 후 짧은 대기 → 클릭 (인간 행동 모방)
                await continue_btn.hover()
                await page.wait_for_timeout(random.randint(200, 500))
                await continue_btn.click()
                logger.info("Continue 버튼 클릭 완료")
            else:
                logger.info("Continue 버튼 미발견 - Enter 키 사용")
                await page.keyboard.press("Enter")

            # 제출 후 대기
            await page.wait_for_timeout(5000)
            logger.info(f"제출 후 URL: {page.url}")
            await page.screenshot(path=f"{self.data_dir}/debug_after_continue.png")

            # 에러 메시지 확인
            try:
                page_text = await page.evaluate("() => document.body.innerText.substring(0, 1500)")
                error_keywords = ['incorrect', 'invalid', 'wrong password', 'too many',
                                  'locked', 'suspended', 'try again later']
                for keyword in error_keywords:
                    if keyword.lower() in page_text.lower():
                        logger.error(f"로그인 에러: '{keyword}'")
                        break
                logger.info(f"제출 후 화면: {page_text[:400]}")
            except Exception:
                pass

            # 캡차 + 인증 코드 반복 처리 (캡차가 여러 번 나올 수 있음)
            for captcha_round in range(3):
                # 캡차 처리 (슬라이더 퍼즐 캡차가 나타날 수 있음)
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

                # 캡차 후 페이지 전환 대기
                await page.wait_for_timeout(3000)
                current_url = page.url
                logger.info(f"캡차 라운드 {captcha_round + 1} 완료. URL: {current_url}")

                # 캡차 후 실제로 페이지가 전환되었는지 확인
                if "/account/login" not in current_url:
                    break

                # 여전히 로그인 페이지 → 캡차가 실제로 안 풀린 것
                # 페이지에 캡차가 남아있는지 확인
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

                # 인증 코드 확인
                if await self._needs_verification():
                    break
                # 짧은 대기 후 다시 확인
                await page.wait_for_timeout(3000)

            logger.info(f"캡차 처리 후 최종 URL: {page.url}")

            # 인증 코드 처리
            if await self._needs_verification():
                logger.info("이메일 인증 코드 필요 - 대기 중...")
                verified = await self._wait_for_verification(timeout=300)
                if not verified:
                    logger.error("인증 코드 타임아웃")
                    return False

            # 로그인 완료 대기 (SSO 리다이렉트 포함)
            for i in range(10):
                await page.wait_for_timeout(3000)
                current_url = page.url

                # register 리다이렉트 감지 → Seller Center 메인으로 재이동
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

                # 여전히 로그인 페이지에서 인증 코드가 새로 나타났을 수 있음
                if "/account/login" in current_url and await self._needs_verification():
                    logger.info("로그인 대기 중 인증 코드 감지")
                    verified = await self._wait_for_verification(timeout=300)
                    if verified:
                        continue

                logger.info(f"  로그인 대기 중... URL: {current_url[:80]}")

            # Rating 페이지로 직접 이동 시도
            await page.goto(self.RATING_PAGE_URL, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(8000)  # 충분히 대기

            if await self._is_logged_in():
                logger.info("로그인 성공! (Rating 페이지 이동 후)")
                return True

            logger.error(f"로그인 후에도 인증 확인 실패. URL: {page.url}")
            await page.screenshot(path=f"{self.data_dir}/debug_login_failed.png", full_page=True)
            return False

        except Exception as e:
            logger.error(f"로그인 오류: {e}")
            return False

    async def _react_safe_input(self, element, text: str, force_events: bool = False):
        """
        React controlled component에 안전하게 텍스트 입력.

        전략 (순서대로 시도):
        1. fill() - Playwright 기본 (대부분의 경우 동작)
        2. triple-click + keyboard.type() - 실제 키보드 이벤트
        3. Native setter + React 이벤트 디스패치 - 최후 수단
        """
        page = self._page

        if not force_events:
            # 방법 1: fill() 시도 (가장 신뢰할 수 있는 방법)
            try:
                await element.fill(text)
                await page.wait_for_timeout(200)
                value = await element.evaluate("el => el.value")
                if value == text:
                    return
                logger.info(f"fill() 후 값 불일치: '{value}' (keyboard.type 시도)")
            except Exception as e:
                logger.info(f"fill() 실패: {e}")

            # 방법 2: click + select all + keyboard.type()
            await element.click(click_count=3)  # triple-click으로 전체 선택
            await page.wait_for_timeout(100)
            await page.keyboard.type(text, delay=30)
            await page.wait_for_timeout(200)

            value = await element.evaluate("el => el.value")
            if value == text:
                return
            logger.info(f"keyboard.type() 후 값 불일치: '{value}' (native setter 시도)")

        # 방법 3: Native setter + 이벤트 디스패치 (React 강제 업데이트)
        await element.evaluate("""
            (el, text) => {
                // React 내부 상태 키를 찾아서 직접 업데이트
                const reactPropsKey = Object.keys(el).find(k => k.startsWith('__reactProps$'));
                if (reactPropsKey && el[reactPropsKey] && el[reactPropsKey].onChange) {
                    // React onChange 핸들러 직접 호출
                    const nativeInputValueSetter = Object.getOwnPropertyDescriptor(
                        window.HTMLInputElement.prototype, 'value'
                    ).set;
                    nativeInputValueSetter.call(el, text);
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                } else {
                    // React props를 못 찾으면 일반 setter 사용
                    const nativeInputValueSetter = Object.getOwnPropertyDescriptor(
                        window.HTMLInputElement.prototype, 'value'
                    ).set;
                    nativeInputValueSetter.call(el, text);
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                    // React 16+ 호환 이벤트도 발생
                    const inputEvent = new InputEvent('input', {
                        bubbles: true,
                        cancelable: false,
                        inputType: 'insertText',
                        data: text,
                    });
                    el.dispatchEvent(inputEvent);
                }
            }
        """, text)
        await page.wait_for_timeout(300)

        value = await element.evaluate("el => el.value")
        logger.info(f"native setter 후 값: '{value}' (expected: '{text[:20]}...')")

    async def _click_login_button(self) -> bool:
        """로그인 버튼 클릭 (다양한 방법 시도)"""
        page = self._page

        # 방법 1: 셀렉터로 직접 클릭
        # TikTok Seller Center: "Continue"가 실제 제출 버튼
        for selector in [
            'button:has-text("Continue")',
            'button[type="submit"]',
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

    async def _human_type_field(self, locator, text: str, label: str) -> bool:
        """인간 유사 키보드 입력으로 필드에 텍스트를 입력.

        TikTok의 행동 분석 우회를 위해:
        1. 필드 클릭 → 짧은 대기
        2. 기존 텍스트 선택 삭제
        3. press_sequentially (80~200ms 랜덤 딜레이/키)
        4. 입력값 검증
        """
        page = self._page
        try:
            # 필드 클릭 (포커스)
            await locator.click()
            await page.wait_for_timeout(random.randint(200, 500))

            # 기존 텍스트 전체 선택 후 삭제
            await page.keyboard.press("Control+a")
            await page.wait_for_timeout(100)
            await page.keyboard.press("Backspace")
            await page.wait_for_timeout(random.randint(100, 300))

            # 글자별 입력 (랜덤 딜레이)
            delay = random.randint(80, 150)
            await locator.press_sequentially(text, delay=delay)
            await page.wait_for_timeout(random.randint(200, 500))

            # 입력값 검증
            val = await locator.input_value()
            if val == text:
                logger.info(f"{label} 입력 성공 (press_sequentially, delay={delay}ms)")
                return True

            # fallback: fill() 시도
            logger.warning(f"{label} press_sequentially 후 값 불일치 '{val}' → fill() 시도")
            await locator.fill(text)
            await page.wait_for_timeout(300)
            val = await locator.input_value()
            if val == text:
                logger.info(f"{label} fill() 폴백 성공")
                return True

            logger.error(f"{label} 입력 실패: '{val}'")
            return False
        except Exception as e:
            logger.error(f"{label} 입력 오류: {e}")
            return False

    async def _recover_session(self) -> bool:
        """세션 만료 시 재로그인을 시도하여 세션 복구.

        Returns:
            True if session was recovered successfully
        """
        logger.info("세션 복구 시도: 재로그인 진행...")
        page = self._page

        success = await self._do_login()
        if success:
            await self._save_cookies()
            # Rating 페이지로 복귀
            await page.goto(self.RATING_PAGE_URL, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)
            if await self._is_logged_in():
                logger.info("세션 복구 성공")
                return True

        logger.error("세션 복구 실패: 재로그인 불가")
        self._notify_captcha_failure()
        return False

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

        # 팝업 처리 후 URL 변경 감지 → 세션 복구 시도
        current_url = page.url
        if "/product/rating" not in current_url:
            if "/account/login" in current_url or "/account/register" in current_url:
                logger.warning(f"팝업 처리 후 세션 만료 감지. 재로그인 시도. URL: {current_url}")
                if not await self._recover_session():
                    return all_reviews
            else:
                logger.warning(f"Rating 페이지 이탈 감지. 재이동. URL: {current_url}")
                await page.goto(self.RATING_PAGE_URL, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(3000)

        # Rating 페이지에서도 캡차가 나올 수 있음
        await self._handle_captcha()

        # 캡차 처리 후에도 URL 변경 감지
        current_url = page.url
        if "/account/login" in current_url or "/account/register" in current_url:
            logger.warning(f"캡차 처리 후 세션 만료. 재로그인 시도. URL: {current_url}")
            if not await self._recover_session():
                return all_reviews
        elif "/product/rating" not in current_url:
            logger.warning(f"캡차 후 Rating 이탈. 재이동. URL: {current_url}")
            await page.goto(self.RATING_PAGE_URL, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)

        # 최종 로그인 상태 확인
        if not await self._is_logged_in():
            logger.warning(f"최종 로그인 확인 실패. 재로그인 시도. URL: {page.url}")
            if not await self._recover_session():
                return all_reviews

        for page_num in range(1, max_pages + 1):
            try:
                # 리뷰 요소 대기
                await page.wait_for_timeout(2000)

                # HTML에서 리뷰 파싱
                reviews = await self._parse_reviews_from_page()

                if not reviews:
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
        """현재 페이지의 리뷰를 JavaScript로 파싱"""
        page = self._page

        # DOM 디버깅: 리뷰 관련 클래스 확인 및 HTML 저장
        debug_info = await page.evaluate("""
            () => {
                const allEls = document.querySelectorAll('*');
                const classSet = new Set();
                for (const el of allEls) {
                    const cls = el.className;
                    if (typeof cls === 'string') {
                        cls.split(/\\s+/).forEach(c => {
                            const lower = c.toLowerCase();
                            if (lower.includes('rating') || lower.includes('review') ||
                                lower.includes('star') || lower.includes('comment') ||
                                lower.includes('feedback') || lower.includes('list') ||
                                lower.includes('table') || lower.includes('row') ||
                                lower.includes('card') || lower.includes('item')) {
                                classSet.add(c);
                            }
                        });
                    }
                }
                // ratingListItem 셀렉터 직접 테스트
                const ratingItems = document.querySelectorAll('[class*="ratingListItem"]');
                // 테이블 구조 확인
                const tables = document.querySelectorAll('table');
                const arcoTables = document.querySelectorAll('[class*="arco-table"]');
                return {
                    url: window.location.href,
                    totalElements: allEls.length,
                    matchingClasses: Array.from(classSet).sort().slice(0, 80),
                    ratingListItemCount: ratingItems.length,
                    tableCount: tables.length,
                    arcoTableCount: arcoTables.length,
                    bodyTextPreview: document.body.innerText.substring(0, 2000)
                };
            }
        """)
        logger.info(f"[DOM 디버깅] URL: {debug_info.get('url', 'N/A')}")
        logger.info(f"[DOM 디버깅] 총 요소: {debug_info.get('totalElements', 0)}, "
                     f"ratingListItem: {debug_info.get('ratingListItemCount', 0)}, "
                     f"table: {debug_info.get('tableCount', 0)}, "
                     f"arco-table: {debug_info.get('arcoTableCount', 0)}")
        logger.info(f"[DOM 디버깅] 관련 클래스: {debug_info.get('matchingClasses', [])}")
        body_preview = debug_info.get('bodyTextPreview', '')[:500]
        logger.info(f"[DOM 디버깅] Body 미리보기: {body_preview}")

        # HTML 파일 저장 (최초 1회)
        try:
            html_path = os.path.join(self.data_dir, "rating_page.html")
            if not os.path.exists(html_path):
                full_html = await page.evaluate("() => document.documentElement.outerHTML")
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(full_html)
                logger.info(f"[DOM 디버깅] HTML 저장: {html_path} ({len(full_html)} chars)")
            # 스크린샷도 저장
            ss_path = os.path.join(self.data_dir, "debug_rating_current.png")
            await page.screenshot(path=ss_path, full_page=True)
        except Exception as e:
            logger.debug(f"디버깅 파일 저장 실패: {e}")

        reviews = await page.evaluate("""
            () => {
                const items = document.querySelectorAll('[class*="ratingListItem"]');
                const results = [];

                items.forEach(item => {
                    try {
                        // 별점: activeStar SVG 개수
                        const starContainer = item.querySelector('[class*="ratingStar"]');
                        const activeStars = starContainer
                            ? starContainer.querySelectorAll('[class*="activeStar"]').length
                            : 0;

                        // 날짜
                        const dateEl = item.querySelector('[class*="reviewTime"]');
                        const dateText = dateEl ? dateEl.textContent.trim() : '';

                        // 리뷰 텍스트
                        const textEl = item.querySelector('[class*="reviewText"]');
                        const reviewText = textEl ? textEl.textContent.trim() : '';

                        // 응답 수
                        const replyCountEl = item.querySelector('[class*="replyCount"]');
                        const replyCountText = replyCountEl ? replyCountEl.textContent.trim() : '0';

                        // 주문 ID
                        const orderIdEl = item.querySelector('[class*="productItemInfoOrderIdText"]');
                        const orderId = orderIdEl ? orderIdEl.textContent.trim() : '';

                        // 제품 ID
                        const productIdEl = item.querySelector('[class*="productItemInfoProductId"]');
                        const productId = productIdEl ? productIdEl.textContent.trim() : '';

                        // 제품명
                        const productNameEl = item.querySelector('[class*="productItemInfoName"]');
                        const productName = productNameEl ? productNameEl.textContent.trim() : '';

                        // SKU/변형
                        const skuEl = item.querySelector('[class*="productItemInfoSku"]');
                        const sku = skuEl ? skuEl.textContent.trim() : '';

                        // 사용자명
                        const usernameEl = item.querySelector('[class*="userNameText"]');
                        const username = usernameEl ? usernameEl.textContent.trim() : '';

                        // 판매자 답변
                        const replyEl = item.querySelector('[class*="sellerReply"]');
                        const sellerReply = replyEl ? replyEl.textContent.trim() : '';

                        // 이미지 URL
                        const images = [];
                        const imgEls = item.querySelectorAll('[class*="reviewImage"] img, [class*="mediaImage"] img');
                        imgEls.forEach(img => {
                            if (img.src) images.push(img.src);
                        });

                        // 비디오 여부
                        const hasVideo = item.querySelector('[class*="videoIcon"], [class*="playIcon"]') !== null;

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
