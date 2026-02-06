"""
Amazon Authentication Module

Handles login and session management using Playwright.
Supports manual login on first run, then auto-login via saved cookies.
"""

import json
import os
import random
from playwright.async_api import async_playwright, BrowserContext
from config.settings import (
    COOKIES_FILE, 
    DATA_DIR, 
    USER_AGENTS,
    AMAZON_BASE_URL,
    LOGIN_URL
)


class AmazonAuth:
    """Amazon authentication handler with cookie persistence."""
    
    def __init__(self):
        self.cookies_file = COOKIES_FILE
        self.playwright = None
        self.browser = None
        self._ensure_data_dir()
    
    def _ensure_data_dir(self):
        """Create data directory if it doesn't exist."""
        os.makedirs(DATA_DIR, exist_ok=True)
    
    async def login_and_get_context(self) -> BrowserContext:
        """
        Login to Amazon and return authenticated browser context.
        
        First run: Opens browser for manual login
        Subsequent runs: Uses saved cookies for auto-login
        
        Returns:
            BrowserContext: Authenticated Playwright browser context
        """
        self.playwright = await async_playwright().start()
        
        # Launch browser (visible for first login)
        self.browser = await self.playwright.chromium.launch(
            headless=False,  # Visible for login
            args=['--disable-blink-features=AutomationControlled']
        )
        
        # Create context with random user agent
        context = await self.browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={'width': 1920, 'height': 1080},
            locale='en-US'
        )
        
        # Try to load saved cookies
        if os.path.exists(self.cookies_file):
            try:
                with open(self.cookies_file, 'r') as f:
                    cookies = json.load(f)
                await context.add_cookies(cookies)
                print("📦 Loaded saved cookies")
                
                # Verify login status
                page = await context.new_page()
                await page.goto(AMAZON_BASE_URL)
                
                if await self._is_logged_in(page):
                    print("✅ Auto-login successful!")
                    await page.close()
                    return context
                else:
                    print("⚠️ Saved session expired. Manual login required.")
                    await page.close()
            except Exception as e:
                print(f"⚠️ Failed to load cookies: {e}")
        
        # Manual login required
        print("\n" + "="*50)
        print("🔐 AMAZON LOGIN REQUIRED")
        print("="*50)
        print("A browser window will open.")
        print("Please log in to your Amazon account.")
        print("The scraper will continue automatically after login.")
        print("="*50 + "\n")
        
        page = await context.new_page()
        await page.goto(LOGIN_URL)
        
        # Wait for user to complete login (max 5 minutes)
        try:
            # Wait until redirected to main page or product page
            await page.wait_for_function(
                """() => {
                    return window.location.hostname === 'www.amazon.com' && 
                           !window.location.pathname.includes('/ap/');
                }""",
                timeout=300000  # 5 minutes
            )
            print("✅ Login detected!")
            
            # Wait a bit for page to stabilize
            await page.wait_for_timeout(2000)
            
            # Save cookies
            cookies = await context.cookies()
            with open(self.cookies_file, 'w') as f:
                json.dump(cookies, f)
            print(f"💾 Session saved to {self.cookies_file}")
            
            await page.close()
            return context
            
        except Exception as e:
            print(f"❌ Login timeout or error: {e}")
            raise
    
    async def _is_logged_in(self, page) -> bool:
        """Check if user is logged in by examining account link."""
        try:
            await page.wait_for_selector('#nav-link-accountList', timeout=5000)
            account_text = await page.inner_text('#nav-link-accountList')
            # Check for greeting ("Hello, [Name]")
            return 'Hello' in account_text and 'Sign in' not in account_text
        except:
            return False
    
    async def close(self):
        """Clean up browser resources."""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
