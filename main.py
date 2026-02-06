"""
Amazon Review Scraper - Main Entry Point

Usage:
    python main.py           # Full scrape (1 month of reviews)
    python main.py --test    # Test mode (10 pages only)
    python main.py --clear   # Clear checkpoint and start fresh
"""

import asyncio
import sys
import time
from datetime import datetime

from src.auth import AmazonAuth
from src.scraper import ReviewScraper
from src.utils import clear_checkpoint, format_duration, ensure_data_dir
from config.settings import ASIN, PRODUCT_NAME, DAYS_TO_SCRAPE


async def main():
    """Main entry point."""
    # Parse arguments
    test_mode = '--test' in sys.argv
    clear_mode = '--clear' in sys.argv
    
    if clear_mode:
        clear_checkpoint()
        print("Starting fresh...")
    
    # Ensure data directory exists
    ensure_data_dir()
    
    print("\n" + "#" * 60)
    print("#" + " " * 58 + "#")
    print("#     🕷️  AMAZON REVIEW SCRAPER                           #")
    print("#" + " " * 58 + "#")
    print("#" * 60)
    print(f"\n📦 Product: {PRODUCT_NAME}")
    print(f"🔗 ASIN: {ASIN}")
    print(f"📅 Collecting reviews from last {DAYS_TO_SCRAPE} days")
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if test_mode:
        print("\n⚠️  TEST MODE: Only scraping first 10 pages")
    print()
    
    start_time = time.time()
    auth = None
    
    try:
        # Step 1: Login
        print("\n📍 Step 1: Authentication")
        auth = AmazonAuth()
        context = await auth.login_and_get_context()
        
        # Step 2: Scrape reviews
        print("\n📍 Step 2: Scraping Reviews")
        scraper = ReviewScraper(
            browser_context=context,
            test_mode=test_mode
        )
        
        reviews = await scraper.scrape_reviews()
        
        # Step 3: Summary
        elapsed = time.time() - start_time
        print("\n📍 Step 3: Summary")
        print(f"\n⏱️ Total time: {format_duration(elapsed)}")
        print(f"📊 Reviews collected: {len(reviews)}")
        print(f"💾 Saved to: data/reviews.csv")
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Scraping interrupted by user.")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise
        
    finally:
        if auth:
            await auth.close()
        print("\n👋 Browser closed. Goodbye!")


if __name__ == '__main__':
    asyncio.run(main())
