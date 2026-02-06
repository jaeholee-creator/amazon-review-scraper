"""
Amazon Review Scraper - Main Scraping Logic

Handles pagination, date filtering, and review collection.
Implements rate limiting and error handling.
"""

import asyncio
import random
from datetime import datetime, date
from bs4 import BeautifulSoup
from playwright.async_api import BrowserContext, Page

from config.settings import (
    ASIN,
    REVIEWS_URL_TEMPLATE,
    CUTOFF_DATE,
    MIN_DELAY,
    MAX_DELAY,
    PAGE_BREAK_INTERVAL,
    PAGE_BREAK_DURATION,
    MAX_RETRIES,
    RETRY_DELAY,
    get_reviews_url,
)
from src.parser import ReviewParser
from src.utils import (
    save_reviews_to_csv,
    save_checkpoint,
    load_checkpoint,
    print_progress
)


class ReviewScraper:
    """Amazon product review scraper with date filtering."""
    
    def __init__(self, browser_context: BrowserContext, test_mode: bool = False):
        """
        Initialize scraper.
        
        Args:
            browser_context: Authenticated Playwright browser context
            test_mode: If True, only scrape first 10 pages
        """
        self.context = browser_context
        self.parser = ReviewParser()
        self.test_mode = test_mode
        self.max_pages = 10 if test_mode else 5000  # Safety limit
        
        self.all_reviews = []
        self.current_page = 1
        self.reached_cutoff = False
        self.error_count = 0
    
    async def scrape_reviews(self) -> list:
        """
        Scrape all reviews within date range.
        
        Returns:
            List of review dictionaries
        """
        # Load checkpoint if exists
        checkpoint = load_checkpoint()
        if checkpoint:
            self.current_page = checkpoint.get('last_page', 1)
            print(f"📂 Resuming from page {self.current_page}")
        
        page = await self.context.new_page()
        
        print("\n" + "="*60)
        print(f"🚀 Starting Amazon Review Scraper")
        print(f"   Product ASIN: {ASIN}")
        print(f"   Date Filter: {CUTOFF_DATE.strftime('%Y-%m-%d')} ~ Today")
        print(f"   Mode: {'TEST (10 pages max)' if self.test_mode else 'FULL'}")
        print("="*60 + "\n")
        
        try:
            while self.current_page <= self.max_pages and not self.reached_cutoff:
                success = await self._scrape_page(page, self.current_page)
                
                if not success:
                    self.error_count += 1
                    if self.error_count >= MAX_RETRIES:
                        print(f"\n❌ Max retries ({MAX_RETRIES}) reached. Stopping.")
                        break
                    continue
                
                self.error_count = 0  # Reset on success
                self.current_page += 1
                
                # Rate limiting
                delay = random.uniform(MIN_DELAY, MAX_DELAY)
                await asyncio.sleep(delay)
                
                # Periodic break
                if self.current_page % PAGE_BREAK_INTERVAL == 0:
                    print(f"\n☕ Taking a {PAGE_BREAK_DURATION}s break...")
                    await asyncio.sleep(PAGE_BREAK_DURATION)
        
        except KeyboardInterrupt:
            print("\n\n⚠️ Interrupted by user.")
        
        finally:
            # Save checkpoint
            save_checkpoint(self.current_page)
            await page.close()
        
        print("\n" + "="*60)
        print(f"✅ Scraping Complete!")
        print(f"   Total reviews collected: {len(self.all_reviews)}")
        print(f"   Pages scraped: {self.current_page - 1}")
        print("="*60)
        
        return self.all_reviews
    
    async def _scrape_page(self, page: Page, page_num: int) -> bool:
        """
        Scrape a single page of reviews.
        
        Args:
            page: Playwright page object
            page_num: Page number to scrape
            
        Returns:
            True if successful, False otherwise
        """
        url = REVIEWS_URL_TEMPLATE.format(page=page_num)
        
        try:
            response = await page.goto(url, wait_until='domcontentloaded', timeout=20000)
            
            if '/ap/signin' in page.url:
                print("\n❌ Session expired! Please restart and re-login.")
                return False
            
            html = await page.content()
            
            captcha_indicators = [
                'Enter the characters you see below',
                'Type the characters',
                'solve this puzzle',
                'api.arkoselabs.com'
            ]
            if any(indicator in html for indicator in captcha_indicators):
                print("\n🛑 CAPTCHA detected! Stopping scraper.")
                print("   Please wait 30 minutes and try again.")
                return False
            
            try:
                await page.wait_for_selector('[data-hook="review"]', timeout=5000)
            except:
                print(f"\n⚠️ No reviews found on page {page_num}. May have reached end.")
                self.reached_cutoff = True
                return True
            
            # Parse reviews
            soup = BeautifulSoup(html, 'html.parser')
            reviews = self.parser.parse_reviews(soup)
            
            if not reviews:
                print(f"\n⚠️ No reviews parsed from page {page_num}. Stopping.")
                self.reached_cutoff = True
                return True
            
            # Check date filter
            new_reviews = []
            for review in reviews:
                review_date = review.get('date_parsed')
                if review_date and review_date >= CUTOFF_DATE:
                    new_reviews.append(review)
                elif review_date and review_date < CUTOFF_DATE:
                    print(f"\n📅 Reached cutoff date ({CUTOFF_DATE.strftime('%Y-%m-%d')}). Stopping.")
                    self.reached_cutoff = True
                    break
            
            if new_reviews:
                self.all_reviews.extend(new_reviews)
                # Save immediately
                save_reviews_to_csv(new_reviews, append=True)
                print_progress(page_num, len(new_reviews), len(self.all_reviews))
            
            return True
            
        except Exception as e:
            print(f"\n❌ Error on page {page_num}: {e}")
            await asyncio.sleep(RETRY_DELAY)
            return False


class DailyReviewScraper:
    
    STATUS_SUCCESS = 'success'
    STATUS_PARTIAL = 'partial'
    STATUS_FAILED = 'failed'
    
    def __init__(
        self,
        browser_context: BrowserContext,
        asin: str,
        date_start,
        date_end,
        test_mode: bool = False,
        collected_ids=None
    ):
        self.context = browser_context
        self.asin = asin
        self.start_date = date_start if isinstance(date_start, date) else date_start.date()
        self.end_date = date_end if isinstance(date_end, date) else date_end.date()
        self.parser = ReviewParser()
        self.test_mode = test_mode
        self.max_pages = 10 if test_mode else 100
        self.collected_ids = collected_ids or set()
        
        self.all_reviews = []
        self.new_review_ids = set()
        self.current_page = 1
        self.reached_cutoff = False
        self.error_count = 0
        self.status = self.STATUS_SUCCESS
        self.error_message = None
    
    async def scrape_reviews(self) -> tuple:
        page = await self.context.new_page()
        
        print(f"\n🚀 Scraping reviews for ASIN: {self.asin}")
        print(f"   Date range: {self.start_date} ~ {self.end_date}")
        print(f"   Known IDs to skip: {len(self.collected_ids)}")
        print(f"   Mode: {'TEST (10 pages max)' if self.test_mode else 'FULL'}\n")
        
        try:
            while self.current_page <= self.max_pages and not self.reached_cutoff:
                success = await self._scrape_page(page, self.current_page)
                
                if not success:
                    self.error_count += 1
                    if self.error_count >= MAX_RETRIES:
                        self.status = self.STATUS_PARTIAL if self.all_reviews else self.STATUS_FAILED
                        self.error_message = f"Max retries ({MAX_RETRIES}) reached"
                        break
                    continue
                
                self.error_count = 0
                self.current_page += 1
                
                delay = random.uniform(MIN_DELAY, MAX_DELAY)
                await asyncio.sleep(delay)
                
                if self.current_page % PAGE_BREAK_INTERVAL == 0:
                    print(f"   ☕ Taking a {PAGE_BREAK_DURATION}s break...")
                    await asyncio.sleep(PAGE_BREAK_DURATION)
        
        except Exception as e:
            self.status = self.STATUS_PARTIAL if self.all_reviews else self.STATUS_FAILED
            self.error_message = str(e)
        
        finally:
            await page.close()
        
        print(f"\n✅ Collected {len(self.all_reviews)} reviews (Status: {self.status})")
        
        return self.all_reviews, self.status, self.error_message
    
    async def _scrape_page(self, page: Page, page_num: int) -> bool:
        url = get_reviews_url(self.asin, page_num)
        
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=20000)
            
            if '/ap/signin' in page.url:
                self.error_message = "Session expired"
                return False
            
            html = await page.content()
            
            captcha_indicators = [
                'Enter the characters you see below',
                'Type the characters',
                'solve this puzzle',
                'api.arkoselabs.com'
            ]
            if any(indicator in html for indicator in captcha_indicators):
                self.error_message = "CAPTCHA detected"
                return False
            
            try:
                await page.wait_for_selector('[data-hook="review"]', timeout=5000)
            except:
                print(f"   ⚠️ No reviews on page {page_num}. End reached.")
                self.reached_cutoff = True
                return True
            
            soup = BeautifulSoup(html, 'html.parser')
            reviews = self.parser.parse_reviews(soup)
            
            if not reviews:
                self.reached_cutoff = True
                return True
            
            new_reviews = []
            past_target_date_count = 0  # 범위 밖 리뷰 카운트
            
            for review in reviews:
                review_date = review.get('date_parsed')
                if not review_date:
                    continue
                
                review_date_only = review_date.date() if hasattr(review_date, 'date') else review_date
                review_id = review.get('review_id', '')
                
                # 범위 밖의 리뷰는 스킵하되, 계속 순회
                if review_date_only < self.start_date or review_date_only > self.end_date:
                    past_target_date_count += 1
                    # 연속으로 10개 이상 범위 밖 리뷰가 나오면 중단 (더 이상 범위 내 리뷰 없음)
                    if past_target_date_count >= 10:
                        self.reached_cutoff = True
                        break
                    continue
                
                # 범위 내 리뷰 발견 시 카운트 리셋
                past_target_date_count = 0
                
                if review_id and review_id in self.collected_ids:
                    continue
                
                review['asin'] = self.asin
                new_reviews.append(review)
                if review_id:
                    self.new_review_ids.add(review_id)
            
            if new_reviews:
                self.all_reviews.extend(new_reviews)
                print(f"   [Page {page_num}] +{len(new_reviews)} reviews | Total: {len(self.all_reviews)}")
            else:
                print(f"   [Page {page_num}] No matching reviews")
            
            return True
            
        except Exception as e:
            print(f"   ❌ Error on page {page_num}: {e}")
            await asyncio.sleep(RETRY_DELAY)
            return False
