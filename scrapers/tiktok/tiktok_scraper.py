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

        self._context = await self._playwright.chromium.launch_persistent_context(
            profile_dir,
            headless=self.headless,
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
            ],
            ignore_default_args=["--enable-automation"],
        )
        self._browser = None  # persistent context는 browser 객체 없음

        # 스텔스: 자동화 흔적 제거
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
        """)

        self._page = self._context.pages[0] if self._context.pages else await self._context.new_page()

        # 로그인 시도
        logged_in = await self._ensure_logged_in()
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
    # 영구 브라우저 프로필 사용 → 쿠키/localStorage/sessionStorage 자동 유지
    # 별도 쿠키 파일 저장 불필요

    # =========================================================================
    # Login
    # =========================================================================

    async def _ensure_logged_in(self) -> bool:
        """로그인 상태 확인 및 필요시 로그인 수행"""
        page = self._page

        # Rating 페이지로 이동하여 세션 확인
        logger.info("Seller Center 접속 시도...")
        await page.goto(self.RATING_PAGE_URL, wait_until="domcontentloaded", timeout=30000)

        # SSO 리다이렉트 완료 대기
        await page.wait_for_timeout(5000)
        for _ in range(4):
            await page.wait_for_timeout(5000)
            if await self._is_logged_in():
                logger.info("기존 세션으로 로그인 확인됨")
                await self._dismiss_popups()
                return True
            current_url = page.url
            if "/account/login" in current_url or "/account/register" in current_url:
                break

        logger.info("세션 만료. 재로그인 진행...")
        return await self._do_login()

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
        """이메일/비밀번호 로그인 수행"""
        page = self._page

        try:
            # 로그인 페이지로 이동
            await page.goto(self.LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)

            # "Log in" 탭으로 전환 (기본이 Sign up일 수 있음)
            # 여러 셀렉터 시도
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

            # 이메일 입력
            email_input = await page.query_selector(
                'input[name="email"], input[type="email"], input[placeholder*="email" i]'
            )
            if email_input:
                await email_input.fill(self.email)
                logger.info(f"이메일 입력: {self.email}")
            else:
                logger.error("이메일 입력 필드를 찾을 수 없음")
                return False

            # 비밀번호 입력
            password_input = await page.query_selector(
                'input[name="password"], input[type="password"]'
            )
            if password_input:
                await password_input.fill(self.password)
                logger.info("비밀번호 입력 완료")
            else:
                logger.error("비밀번호 입력 필드를 찾을 수 없음")
                return False

            # 로그인 버튼 클릭 ("Continue" 버튼이 실제 제출 버튼)
            # 주의: button:has-text("Log in")은 "Log in with Google" 등을 먼저 매칭할 수 있음
            login_button = None
            for btn_sel in [
                'button:has-text("Continue")',
                'button[type="submit"]',
            ]:
                login_button = await page.query_selector(btn_sel)
                if login_button:
                    logger.info(f"로그인 버튼 발견: {btn_sel}")
                    break

            if login_button:
                await login_button.click()
                logger.info("로그인 버튼 클릭")
            else:
                logger.error("로그인 버튼을 찾을 수 없음")
                return False

            # 로그인 버튼 클릭 후 캡차/인증 코드 대기
            await page.wait_for_timeout(5000)
            logger.info(f"로그인 버튼 클릭 후 URL: {page.url}")

            # 캡차 + 인증 코드 반복 처리 (캡차가 여러 번 나올 수 있음)
            for captcha_round in range(3):
                # 캡차 처리 (슬라이더 퍼즐 캡차가 나타날 수 있음)
                captcha_passed = await self._handle_captcha()

                if not captcha_passed:
                    if not self.headless:
                        logger.info("캡차를 수동으로 풀어주세요 (최대 60초 대기)")
                        for _ in range(12):
                            await page.wait_for_timeout(5000)
                            if await self._needs_verification() or await self._is_logged_in():
                                break
                    else:
                        logger.error("Headless 모드에서 캡차 자동 풀기 실패")
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
