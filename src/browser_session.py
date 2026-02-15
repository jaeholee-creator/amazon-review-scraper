"""
BrowserSession - Single Page Architecture for Amazon Review Scraping

debug_firefox.py의 성공 패턴을 정확히 복제:
- 하나의 Page 인스턴스를 유지, new_page() 금지
- goto()로만 페이지 이동
- page.content() 최소화 (CAPTCHA 체크 시 query_selector 사용)
- 네트워크 인터셉터로 CSRF 자동 캡처
"""

import asyncio
import json
import os
import random
from datetime import date

from playwright.async_api import async_playwright, Page, BrowserContext
from bs4 import BeautifulSoup

from src.parser import ReviewParser

# TOTP 자동 OTP 생성
try:
    import pyotp
    HAS_PYOTP = True
except ImportError:
    HAS_PYOTP = False


class BrowserSession:
    """단일 Page 기반 Amazon 브라우저 세션."""

    def __init__(self, region: str = 'us'):
        """
        Args:
            region: 'us' 또는 'uk'
        """
        self._region = region.lower()

        # Region에 따라 동적으로 설정 로드
        if self._region == 'uk':
            from config.settings_uk import (
                DATA_DIR,
                AMAZON_BASE_URL,
                AMAZON_EMAIL_UK,
                AMAZON_PASSWORD_UK,
            )
            self._data_dir = DATA_DIR
            self._base_url = AMAZON_BASE_URL
            self._email = AMAZON_EMAIL_UK
            self._password = AMAZON_PASSWORD_UK
            self._locale = 'en-GB'
            self._timezone = 'Europe/London'
        else:  # us (default)
            from config.settings import (
                DATA_DIR,
                AMAZON_BASE_URL,
                AMAZON_EMAIL,
                AMAZON_PASSWORD,
            )
            self._data_dir = DATA_DIR
            self._base_url = AMAZON_BASE_URL
            self._email = AMAZON_EMAIL
            self._password = AMAZON_PASSWORD
            self._locale = 'en-US'
            self._timezone = 'America/New_York'

        self._cookies_file = f'{self._data_dir}/cookies_{self._region}.json'

        # TOTP 시크릿 로드 (Amazon 2FA 자동 OTP)
        self._totp_secret = os.getenv('AMAZON_TOTP_SECRET', '').replace(' ', '')

        self._playwright = None
        self._browser = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None
        self._last_csrf: str = ''
        self._parser = ReviewParser()
        os.makedirs(self._data_dir, exist_ok=True)

    # =========================================================================
    # Lifecycle
    # =========================================================================

    async def start(self):
        """Playwright Firefox 시작 + 단일 Page 생성."""
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=['--disable-dev-shm-usage', '--no-sandbox'],
        )
        self._context = await self._browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            locale=self._locale,
            timezone_id=self._timezone,
        )
        self._page = await self._context.new_page()  # 유일한 new_page()
        self._page.on('request', self._on_request)   # 1회만 등록
        print(f"   Browser started (Chromium, {self._region.upper()}, single page)")

    async def close(self):
        """브라우저 리소스 정리."""
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        self._page = None
        self._context = None
        self._browser = None
        self._playwright = None

    # =========================================================================
    # Network Interceptor
    # =========================================================================

    def _on_request(self, request):
        """모든 요청에서 CSRF 토큰 자동 캡처."""
        token = request.headers.get('anti-csrftoken-a2z', '')
        if token:
            self._last_csrf = token

    # =========================================================================
    # Login
    # =========================================================================

    async def login(self) -> bool:
        """저장된 쿠키 또는 신규 로그인으로 세션 확보."""
        page = self._page

        # 1) 저장된 쿠키 로드 시도
        if os.path.exists(self._cookies_file):
            try:
                with open(self._cookies_file, 'r') as f:
                    cookies = json.load(f)
                await self._context.add_cookies(cookies)
                print(f"   Loaded saved cookies ({len(cookies)} entries)")

                # 같은 페이지에서 홈페이지 방문 → 로그인 확인
                await page.goto(self._base_url, wait_until='domcontentloaded')
                await page.wait_for_timeout(3000)

                if await self._is_logged_in(page):
                    # 같은 페이지에서 리뷰 페이지 검증
                    review_url = f'{self._base_url}/product-reviews/B0B2RM68G2?pageNumber=1&sortBy=recent'
                    await page.goto(review_url, wait_until='networkidle', timeout=30000)
                    await page.wait_for_timeout(3000)

                    url = page.url
                    bot_detected = await page.query_selector('text="automated access"')
                    has_reviews = await page.query_selector('[data-hook="review"]')

                    if '/ap/' not in url and not bot_detected and has_reviews:
                        print("   Session valid (homepage + review page)")
                        return True

                    print(f"   Saved session: review access failed (redirect={'/ap/' in url})")
                else:
                    print("   Saved session expired")
            except Exception as e:
                print(f"   Failed to load cookies: {e}")

        # 2) 만료된 쿠키 제거 후 신규 로그인
        await self._context.clear_cookies()
        print("   Cleared expired cookies")

        if not await self._do_login():
            raise Exception("Auto-login failed - check credentials or CAPTCHA")

        # 3) 로그인 후 리뷰 페이지 검증 (같은 페이지에서)
        review_url = f'{self._base_url}/product-reviews/B0B2RM68G2?pageNumber=1&sortBy=recent'
        print("   Verifying review page access...")
        await page.goto(review_url, wait_until='networkidle', timeout=30000)
        await page.wait_for_timeout(3000)

        url = page.url
        if '/ap/' in url:
            raise Exception("Review page redirected to sign-in after login")

        bot_detected = await page.query_selector('text="automated access"')
        if bot_detected:
            raise Exception("Bot detected on review page")

        has_reviews = await page.query_selector('[data-hook="review"]')
        print(f"   Review access: {has_reviews is not None}")

        # 4) 쿠키 저장
        await self._save_cookies()
        return True

    async def _do_login(self) -> bool:
        """
        debug_firefox.py 플로우 정확 복제:
        Homepage → Sign In 클릭 → Email → Password → 완료
        page.content() 사용 금지 (CAPTCHA 체크는 query_selector로)
        """
        page = self._page
        try:
            print("   Attempting automatic login...")

            # Step 1: 홈페이지
            await page.goto(self._base_url, wait_until='domcontentloaded')
            await page.wait_for_timeout(3000)

            # Step 2: Sign In 클릭
            sign_in = await page.query_selector('#nav-link-accountList')
            if sign_in:
                await sign_in.click()
                await page.wait_for_timeout(3000)
            else:
                await page.goto(
                    f'{self._base_url}/gp/sign-in.html',
                    wait_until='domcontentloaded',
                )
                await page.wait_for_timeout(3000)

            # CAPTCHA 체크 (query_selector로 - page.content() 사용 안함)
            captcha = await page.query_selector('input[id*="captcha"], #captchacharacters, [class*="captcha"]')
            if captcha:
                print("   CAPTCHA detected")
                return False

            # Step 3: 이메일 입력
            email_field = await page.query_selector('#ap_email_login')
            if not email_field:
                email_field = await page.query_selector('#ap_email')
            pw_field = await page.query_selector('#ap_password')

            if email_field:
                print("   Email field found -> full login flow")
                await email_field.fill(self._email)
                try:
                    await page.click('#continue', timeout=5000)
                except Exception:
                    print("   Continue button not found")
                    return False
                await page.wait_for_timeout(3000)

                # CAPTCHA 재체크 (query_selector로)
                captcha = await page.query_selector('input[id*="captcha"], #captchacharacters, [class*="captcha"]')
                if captcha:
                    print("   CAPTCHA detected after email")
                    return False

                pw_field = await page.query_selector('#ap_password')
                if not pw_field:
                    print("   Password field not found after email")
                    return False

            elif pw_field:
                print("   Password-only page detected")
            else:
                print("   Neither email nor password field found")
                return False

            # Step 4: 비밀번호 입력 + Sign In
            await pw_field.fill(self._password)
            try:
                await page.click('#signInSubmit', timeout=5000)
            except Exception:
                print("   Sign in button not found")
                return False
            await page.wait_for_timeout(4000)
            print(f"   After signin URL: {page.url}")

            # OTP/2FA 자동 처리 (pyotp + TOTP 시크릿)
            otp_field = await page.query_selector('#auth-mfa-otpcode, input[name="otpCode"]')
            if otp_field:
                if HAS_PYOTP and self._totp_secret:
                    totp = pyotp.TOTP(self._totp_secret)
                    otp_code = totp.now()
                    print(f"   2FA detected - auto-entering OTP: {otp_code}")
                    await otp_field.fill(otp_code)

                    # "Don't require OTP on this browser" 체크박스
                    remember_cb = await page.query_selector('#auth-mfa-remember-device, input[name="rememberDevice"]')
                    if remember_cb:
                        await remember_cb.check()
                        print("   Checked 'remember device'")

                    # Submit OTP
                    submit_btn = await page.query_selector('#auth-signin-button, button[type="submit"]')
                    if submit_btn:
                        await submit_btn.click()
                        # MFA 페이지 탈출 대기 (최대 15초, 1초 간격 폴링)
                        for _ in range(15):
                            await page.wait_for_timeout(1000)
                            current = page.url
                            if '/ap/mfa' not in current and '/ap/signin' not in current:
                                break
                        print(f"   OTP submitted. URL: {page.url}")
                else:
                    print("   2FA/OTP required but no TOTP secret configured")
                    await page.screenshot(path=f'{self._data_dir}/debug_2fa.png')
                    return False

            # "approve notification" 처리 (앱 승인 요청)
            approve_text = await page.query_selector('text="Approve the notification"')
            if approve_text:
                print("   App approval notification detected - waiting 30s...")
                await page.wait_for_timeout(30000)

            # Continue shopping 처리
            cont = await page.query_selector('text="Continue shopping"')
            if cont:
                await cont.click()
                await page.wait_for_timeout(2000)

            # CAPTCHA 확인 (로그인 후)
            captcha = await page.query_selector('input[id*="captcha"], #captchacharacters, [class*="captcha"]')
            funcaptcha = await page.query_selector('#arkose-iframe, #enforcement-frame')

            if captcha or funcaptcha:
                captcha_type = "FunCaptcha" if funcaptcha else "Image CAPTCHA"
                print(f"   {captcha_type} detected after password submission")
                await page.screenshot(path=f'{self._data_dir}/debug_captcha.png')
                print("   Manual solving required: python3 manual_login.py")
                return False

            # Step 5: 로그인 확인
            if await self._is_logged_in(page):
                print("   Auto-login successful!")
                await self._save_cookies()
                return True

            # URL 기반 확인 (로그인 페이지가 아니면 성공으로 간주)
            current_url = page.url
            print(f"   Current URL: {current_url}")
            if '/ap/' not in current_url and '/ax/' not in current_url:
                print("   Login likely successful (not on auth page)")
                await self._save_cookies()
                return True

            # 디버그: 실패 시 스크린샷 저장
            await page.screenshot(path=f'{self._data_dir}/debug_login_fail.png')
            print(f"   Login verification failed (screenshot saved)")
            return False

        except Exception as e:
            print(f"   Auto-login error: {e}")
            return False

    async def _is_logged_in(self, page: Page) -> bool:
        """Hello 텍스트로 로그인 상태 확인."""
        try:
            await page.wait_for_selector('#nav-link-accountList', timeout=5000)
            account_text = await page.inner_text('#nav-link-accountList')
            return 'Hello' in account_text and 'Sign in' not in account_text
        except Exception:
            return False

    async def _save_cookies(self):
        """현재 컨텍스트 쿠키를 파일에 저장."""
        cookies = await self._context.cookies()
        with open(self._cookies_file, 'w') as f:
            json.dump(cookies, f)
        print(f"   Session saved ({len(cookies)} cookies)")

    # =========================================================================
    # CSRF Capture
    # =========================================================================

    async def capture_csrf(self, asin: str) -> dict:
        """
        같은 페이지에서 리뷰 페이지 방문 → CSRF 토큰 캡처.

        Returns:
            {
                'csrf_token': str,
                'html_reviews': list,
                'redirected': bool,
            }
        """
        page = self._page
        result = {'csrf_token': '', 'html_reviews': [], 'redirected': False}

        try:
            # CSRF 초기화 (이전 제품의 토큰 제거)
            self._last_csrf = ''

            # 같은 페이지에서 리뷰 페이지로 이동
            review_url = (
                f'{self._base_url}/product-reviews/{asin}'
                f'?pageNumber=1&sortBy=recent'
                f'&reviewerType=all_reviews&filterByStar=all_stars'
            )
            await page.goto(review_url, wait_until='networkidle', timeout=30000)

            # 로그인 리다이렉트 체크
            if '/ap/' in page.url:
                print(f"   Redirected to sign-in")
                result['redirected'] = True
                return result

            # 인터셉터가 자동 업데이트한 CSRF 확인
            csrf = self._last_csrf

            # CSRF 없으면 Next 버튼 클릭으로 추가 요청 유도
            if not csrf:
                next_btn = await page.query_selector('li.a-last a')
                if next_btn:
                    await next_btn.click()
                    await page.wait_for_timeout(3000)
                    csrf = self._last_csrf

            result['csrf_token'] = csrf

            # CSRF 없는 경우 → HTML에서 리뷰 파싱
            if not csrf:
                html = await page.content()
                soup = BeautifulSoup(html, 'html.parser')
                result['html_reviews'] = self._parser.parse_reviews(soup)

            mode = 'API' if csrf else f'HTML ({len(result["html_reviews"])} reviews)'
            print(f"   CSRF: {mode}")

        except Exception as e:
            print(f"   Page visit error: {e}")

        return result

    # =========================================================================
    # Re-login
    # =========================================================================

    async def re_login(self, max_attempts: int = 3) -> bool:
        """같은 페이지에서 재로그인. 최대 max_attempts회 시도."""
        for attempt in range(1, max_attempts + 1):
            print(f"   Re-login attempt {attempt}/{max_attempts}...")
            await self._context.clear_cookies()
            if await self._do_login():
                return True
            await self._page.wait_for_timeout(2000)

        print("   Re-login failed after all attempts")
        return False

    # =========================================================================
    # Cookie Helper
    # =========================================================================

    def get_cookie_str(self) -> str:
        """동기적으로 마지막 캐시된 쿠키 문자열 반환 (API 호출용)."""
        return self._cookie_str_cache

    async def update_cookies(self) -> str:
        """비동기로 쿠키 갱신 후 문자열 반환."""
        cookies = await self._context.cookies()
        self._cookie_str_cache = '; '.join(
            f"{c['name']}={c['value']}" for c in cookies
        )
        return self._cookie_str_cache

    _cookie_str_cache: str = ''

    # =========================================================================
    # HTML Scraping (Full page crawling - no API)
    # =========================================================================

    async def scrape_reviews_html(
        self,
        asin: str,
        start_date: date,
        end_date: date,
        existing_ids: set,
        max_pages: int = 100,
    ) -> tuple[list, str, str | None]:
        """
        Playwright HTML 크롤링으로 리뷰 수집.
        API 사용 없이 같은 페이지에서 goto()로 리뷰 페이지를 순회.

        Args:
            asin: 제품 ASIN
            start_date: 수집 시작일 (date)
            end_date: 수집 종료일 (date)
            existing_ids: 이미 수집된 리뷰 ID set (중복 방지)
            max_pages: 최대 페이지 수

        Returns:
            (reviews, status, error_msg) - collect_via_api와 동일 시그니처
        """
        page = self._page
        all_reviews = []
        error_count = 0

        # Region별 딜레이 설정 로드
        if self._region == 'uk':
            from config.settings_uk import MIN_DELAY, MAX_DELAY
        else:
            from config.settings import MIN_DELAY, MAX_DELAY

        # 첫 페이지: URL로 이동
        first_url = (
            f'{self._base_url}/product-reviews/{asin}'
            f'?pageNumber=1&sortBy=recent'
            f'&reviewerType=all_reviews&filterByStar=all_stars'
        )
        await page.goto(first_url, wait_until='networkidle', timeout=30000)

        # 로그인 리다이렉트 체크
        if '/ap/' in page.url:
            print(f"   Session expired. Re-login...")
            if not await self.re_login():
                return all_reviews, 'failed', 'Session expired'
            await page.goto(first_url, wait_until='networkidle', timeout=30000)
            if '/ap/' in page.url:
                return all_reviews, 'failed', 'Auth redirect after re-login'

        for page_num in range(1, max_pages + 1):
            try:
                # 리뷰 요소 대기
                try:
                    await page.wait_for_selector('[data-hook="review"]', timeout=8000)
                except Exception:
                    print(f"   No reviews on page {page_num}. End reached.")
                    break

                html = await page.content()

                # CAPTCHA 감지
                captcha_indicators = [
                    'Enter the characters you see below',
                    'Type the characters',
                    'solve this puzzle',
                    'api.arkoselabs.com',
                ]
                if any(indicator in html for indicator in captcha_indicators):
                    return all_reviews, 'partial' if all_reviews else 'failed', 'CAPTCHA detected'

                # HTML 파싱
                soup = BeautifulSoup(html, 'html.parser')
                reviews = self._parser.parse_reviews(soup)

                if not reviews:
                    break

                # 날짜 필터링 + 중복 제거
                new_reviews = []
                reached_cutoff = False

                for review in reviews:
                    review_date = review.get('date_parsed')
                    if not review_date:
                        continue

                    review_date_only = review_date.date() if hasattr(review_date, 'date') else review_date
                    review_id = review.get('review_id', '')

                    if review_date_only < start_date:
                        print(f"   Date cutoff ({review_date_only} < {start_date}). Stopping.")
                        reached_cutoff = True
                        break
                    elif review_date_only > end_date:
                        continue

                    if review_id and review_id in existing_ids:
                        continue

                    review['asin'] = asin
                    new_reviews.append(review)
                    if review_id:
                        existing_ids.add(review_id)

                if new_reviews:
                    all_reviews.extend(new_reviews)
                    print(f"   [Page {page_num}] +{len(new_reviews)} reviews | Total: {len(all_reviews)}")
                else:
                    print(f"   [Page {page_num}] No matching reviews")

                if reached_cutoff:
                    break

                error_count = 0

                # "Next page" 버튼 클릭으로 다음 페이지 이동
                await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
                next_link = await page.query_selector('li.a-last a')
                if not next_link:
                    print(f"   No next page button. End reached.")
                    break

                try:
                    await next_link.click()
                    await page.wait_for_load_state('networkidle', timeout=30000)
                except Exception:
                    # DOM 갱신으로 element 참조 무효화 시 selector로 재시도
                    next_link = await page.query_selector('li.a-last a')
                    if next_link:
                        await next_link.click()
                        await page.wait_for_load_state('networkidle', timeout=30000)
                    else:
                        print(f"   No next page button after retry. End reached.")
                        break

            except Exception as e:
                error_count += 1
                print(f"   Error on page {page_num}: {e}")
                if error_count >= 3:
                    return all_reviews, 'partial' if all_reviews else 'failed', str(e)
                await asyncio.sleep(3)

        status = 'success' if all_reviews or error_count == 0 else 'failed'
        return all_reviews, status, None
