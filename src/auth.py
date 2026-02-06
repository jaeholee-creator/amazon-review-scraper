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
    LOGIN_URL,
    AMAZON_EMAIL,
    AMAZON_PASSWORD
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
        
        has_cookies = os.path.exists(self.cookies_file)
        
        self.browser = await self.playwright.chromium.launch(
            headless=has_cookies,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox'
            ]
        )
        
        context = await self.browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={
                'width': random.randint(1366, 1920),
                'height': random.randint(768, 1080)
            },
            locale='en-US',
            timezone_id='America/Los_Angeles',
            geolocation={'longitude': -122.4194, 'latitude': 37.7749},
            permissions=['geolocation']
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
        
    async def _auto_login(self, page) -> bool:
        """
        Automatically log in using email and password.
        Returns True if successful, False otherwise.
        """
        try:
            print("🔐 Attempting automatic login...")
            
            # Go to login page
            await page.goto(LOGIN_URL)
            await page.wait_for_timeout(1000)
            
            # Check for CAPTCHA
            html = await page.content()
            if 'arkoselabs' in html or 'captcha' in html.lower():
                print("❌ CAPTCHA detected - cannot auto-login")
                return False
            
            # Fill email
            try:
                await page.fill('#ap_email', AMAZON_EMAIL, timeout=5000)
            except:
                print("❌ Email field not found")
                return False
            
            # Click continue
            try:
                await page.click('#continue', timeout=5000)
            except:
                print("❌ Continue button not found")
                return False
            
            await page.wait_for_timeout(1000)
            
            # Check for CAPTCHA again
            html = await page.content()
            if 'arkoselabs' in html or 'captcha' in html.lower():
                print("❌ CAPTCHA detected after email - cannot auto-login")
                return False
            
            # Fill password
            try:
                await page.fill('#ap_password', AMAZON_PASSWORD, timeout=5000)
            except:
                print("❌ Password field not found")
                return False
            
            # Click sign in
            try:
                await page.click('#signInSubmit', timeout=5000)
            except:
                print("❌ Sign in button not found")
                return False
            
            # Wait for login to complete
            try:
                await page.wait_for_function(
                    """() => {
                        return window.location.hostname === 'www.amazon.com' && 
                               !window.location.pathname.includes('/ap/');
                    }""",
                    timeout=15000
                )
                print("✅ Auto-login successful!")
                return True
            except:
                print("❌ Login verification failed")
                return False
                
        except Exception as e:
            print(f"❌ Auto-login error: {e}")
            return False


                # Try automatic login first
        page = await context.new_page()
        
        if await self._auto_login(page):
            # Auto-login successful
            await page.wait_for_timeout(2000)
            
            # Save cookies
            cookies = await context.cookies()
            with open(self.cookies_file, 'w') as f:
                json.dump(cookies, f)
            print(f"💾 Session saved to {self.cookies_file}")
            
            await page.close()
            return context
        
        # Auto-login failed, show error
        print("\n" + "="*50)
        print("❌ AUTOMATIC LOGIN FAILED")
        print("="*50)
        print("Possible reasons:")
        print("  1. CAPTCHA detected (Amazon security)")
        print("  2. Invalid credentials in .env file")
        print("  3. Network error")
        print("="*50)
        print("\nPlease check your .env file and try again.")
        print("="*50 + "\n")
        
        await page.close()
        raise Exception("Auto-login failed - cannot proceed without manual intervention")
    
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
