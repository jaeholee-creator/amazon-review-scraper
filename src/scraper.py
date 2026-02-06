"""
Amazon Review Scraper - Main Scraping Logic

Handles pagination, date filtering, and review collection.
Implements rate limiting and error handling.
"""

import asyncio
import random
from datetime import datetime
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
    RETRY_DELAY
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
            # Navigate to page
            response = await page.goto(url, wait_until='networkidle', timeout=30000)
            
            # Check for login redirect
            if '/ap/signin' in page.url:
                print("\n❌ Session expired! Please restart and re-login.")
                return False
            
            # Check for CAPTCHA
            html = await page.content()
            if 'Enter the characters you see below' in html or 'Type the characters' in html:
                print("\n🛑 CAPTCHA detected! Stopping scraper.")
                print("   Please wait 30 minutes and try again.")
                return False
            
            # Wait for reviews to load
            try:
                await page.wait_for_selector('[data-hook="review"]', timeout=10000)
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
