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
        gmail_service_account_file: str = "",
        gmail_delegated_user: str = "",
    ):
        """
        Args:
            email: TikTok Seller Center 로그인 이메일
            password: 비밀번호
            data_dir: 데이터 저장 디렉토리 (쿠키, 로그 등)
            headless: 헤드리스 모드 여부
            gmail_service_account_file: Gmail API Service Account JSON 파일 경로
            gmail_delegated_user: Gmail Domain-Wide Delegation 대상 사용자 이메일
        """
        self.email = email
        self.password = password
        self.data_dir = data_dir
        self.headless = headless
        self.cookies_file = os.path.join(data_dir, "tiktok_cookies.json")
        self.gmail_service_account_file = gmail_service_account_file
        self.gmail_delegated_user = gmail_delegated_user

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
        self._browser = await self._playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )

        # 컨텍스트 생성 (쿠키 포함)
        self._context = await self._browser.new_context(
            viewport={"width": 1440, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
        )

        # 저장된 쿠키 로드
        await self._load_cookies()

        self._page = await self._context.new_page()

        # 로그인 시도
        logged_in = await self._ensure_logged_in()
        if logged_in:
            await self._save_cookies()
        return logged_in

    async def close(self):
        """브라우저 종료"""
        if self._context:
            await self._save_cookies()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        logger.info("브라우저 종료 완료")

    # =========================================================================
    # Cookie Management
    # =========================================================================

    async def _load_cookies(self):
        """저장된 쿠키 로드"""
        if not os.path.exists(self.cookies_file):
            logger.info("저장된 쿠키 없음")
            return

        try:
            with open(self.cookies_file, "r") as f:
                cookies = json.load(f)
            await self._context.add_cookies(cookies)
            logger.info(f"쿠키 로드 완료 ({len(cookies)}개)")
        except Exception as e:
            logger.warning(f"쿠키 로드 실패: {e}")

    async def _save_cookies(self):
        """현재 쿠키 저장"""
        try:
            cookies = await self._context.cookies()
            with open(self.cookies_file, "w") as f:
                json.dump(cookies, f)
            logger.info(f"쿠키 저장 완료 ({len(cookies)}개)")
        except Exception as e:
            logger.warning(f"쿠키 저장 실패: {e}")

    # =========================================================================
    # Login
    # =========================================================================

    async def _ensure_logged_in(self) -> bool:
        """로그인 상태 확인 및 필요시 로그인 수행"""
        page = self._page

        # Rating 페이지로 이동 시도
        logger.info("Seller Center 접속 시도...")
        await page.goto(self.RATING_PAGE_URL, wait_until="domcontentloaded", timeout=30000)

        # SSO 리다이렉트 등 완료 대기 (최대 15초)
        for _ in range(5):
            await page.wait_for_timeout(3000)
            if await self._is_logged_in():
                logger.info("기존 세션으로 로그인 확인됨")
                return True
            # 로그인 페이지로 확실히 리다이렉트된 경우 즉시 중단
            current_url = page.url
            if "/account/login" in current_url or "/account/signup" in current_url:
                break

        logger.info("로그인 필요. 로그인 진행...")
        return await self._do_login()

    async def _is_logged_in(self) -> bool:
        """현재 페이지에서 로그인 상태 확인"""
        page = self._page
        current_url = page.url

        # Seller Center 내부 페이지에 있으면 로그인 상태
        if "seller-us.tiktok.com" in current_url and "/account/" not in current_url:
            return True

        return False

    async def _do_login(self) -> bool:
        """이메일/비밀번호 로그인 수행"""
        page = self._page

        try:
            # 로그인 페이지로 이동
            await page.goto(self.LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)

            # "Log in" 링크 클릭 (signup 페이지일 경우)
            login_link = await page.query_selector('a[href*="login"], span:has-text("Log in")')
            if login_link:
                await login_link.click()
                await page.wait_for_timeout(2000)

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

            # 로그인 버튼 클릭
            login_button = await page.query_selector(
                'button[type="submit"], button:has-text("Log in"), button:has-text("Continue")'
            )
            if login_button:
                await login_button.click()
                logger.info("로그인 버튼 클릭")
            else:
                logger.error("로그인 버튼을 찾을 수 없음")
                return False

            # 이메일 인증 코드 처리 (최대 300초 대기)
            await page.wait_for_timeout(3000)

            if await self._needs_verification():
                logger.info("이메일 인증 코드 필요 - 대기 중...")
                verified = await self._wait_for_verification(timeout=300)
                if not verified:
                    logger.error("인증 코드 타임아웃")
                    return False

            # 로그인 완료 대기 (SSO 리다이렉트 포함)
            for _ in range(6):
                await page.wait_for_timeout(3000)
                if await self._is_logged_in():
                    logger.info("로그인 성공!")
                    return True
                logger.info(f"  로그인 대기 중... URL: {page.url[:80]}")

            # Rating 페이지로 직접 이동 시도
            await page.goto(self.RATING_PAGE_URL, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(5000)

            if await self._is_logged_in():
                logger.info("로그인 성공! (Rating 페이지 이동 후)")
                return True

            logger.error(f"로그인 후에도 인증 확인 실패. URL: {page.url}")
            return False

        except Exception as e:
            logger.error(f"로그인 오류: {e}")
            return False

    async def _needs_verification(self) -> bool:
        """이메일 인증 코드 입력이 필요한지 확인"""
        page = self._page
        # 인증 코드 입력 필드가 있는지 확인
        code_input = await page.query_selector(
            'input[type="tel"], input[aria-label*="code" i], input[placeholder*="code" i]'
        )
        return code_input is not None

    async def _wait_for_verification(self, timeout: int = 300) -> bool:
        """
        이메일 인증 코드 입력 대기.

        우선순위:
        1. 환경변수 TIKTOK_VERIFICATION_CODE가 있으면 자동 입력
        2. Gmail API로 인증 코드 이메일 자동 읽기 (Service Account 설정 시)
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

        # 2. Gmail API로 인증 코드 자동 읽기
        if self.gmail_service_account_file and self.gmail_delegated_user:
            code = await self._get_code_from_gmail()
            if code:
                logger.info(f"Gmail API에서 인증 코드 획득: {code}")
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
        """Gmail API를 사용하여 TikTok 인증 코드를 읽어옵니다."""
        try:
            from utils.gmail_code_reader import GmailVerificationCodeReader

            reader = GmailVerificationCodeReader(
                service_account_file=self.gmail_service_account_file,
                delegated_user_email=self.gmail_delegated_user,
            )

            logger.info("Gmail API로 인증 코드 이메일 폴링 시작...")
            code = reader.wait_for_verification_code(timeout=120, poll_interval=5)
            return code

        except ImportError:
            logger.warning(
                "google-api-python-client가 설치되지 않았습니다. "
                "pip install google-api-python-client 실행 필요"
            )
            return None
        except Exception as e:
            logger.error(f"Gmail API 인증 코드 읽기 실패: {e}")
            return None

    async def _input_verification_code(self, code: str):
        """6자리 인증 코드를 개별 필드에 입력"""
        page = self._page

        # 인증 코드 입력 필드들 찾기
        code_inputs = await page.query_selector_all(
            'input[type="tel"], input[data-index]'
        )

        if len(code_inputs) >= 6:
            # 개별 필드에 한 글자씩 입력
            for i, char in enumerate(code[:6]):
                await code_inputs[i].fill(char)
                await page.wait_for_timeout(100)
        else:
            # 단일 필드일 경우 키보드로 입력
            first_input = await page.query_selector(
                'input[type="tel"], input[aria-label*="code" i]'
            )
            if first_input:
                await first_input.click()
                await page.keyboard.type(code, delay=100)

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

        # Rating 페이지로 이동
        logger.info(f"리뷰 수집 시작: {start_date} ~ {end_date}")
        await page.goto(self.RATING_PAGE_URL, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)

        # 로그인 상태 재확인
        if not await self._is_logged_in():
            logger.error("세션 만료. 재로그인 필요.")
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
