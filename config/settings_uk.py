"""
Amazon UK Review Scraper - Configuration Settings
"""
from datetime import datetime, timedelta
import pytz
import os
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# TIMEZONE
# =============================================================================
KST = pytz.timezone('Asia/Seoul')
UK_GMT = pytz.timezone('Europe/London')  # UK uses GMT/BST


# =============================================================================
# AMAZON UK CREDENTIALS (from .env file)
# =============================================================================
AMAZON_EMAIL_UK = os.getenv('AMAZON_EMAIL_UK', '')
AMAZON_PASSWORD_UK = os.getenv('AMAZON_PASSWORD_UK', '')

if not AMAZON_EMAIL_UK or not AMAZON_PASSWORD_UK:
    import logging
    logging.getLogger(__name__).warning(
        "Amazon UK credentials not set. Amazon UK scraper will not work. "
        "Set AMAZON_EMAIL_UK and AMAZON_PASSWORD_UK in .env or environment variables."
    )

# =============================================================================
# DATE FILTER
# =============================================================================
COLLECTION_WINDOW_DAYS = 3  # 최근 3일간 리뷰 수집

def get_collection_date_range():
    """
    Amazon.co.uk uses UK time (GMT/BST).
    Collect reviews from the last 2 days to account for review approval delays.
    Returns: (start_date, end_date) as date objects
    """
    from datetime import date
    now_uk = datetime.now(UK_GMT)
    end_date = now_uk.date()
    start_date = end_date - timedelta(days=COLLECTION_WINDOW_DAYS)
    return start_date, end_date


def get_collection_date_range_str():
    start_date, end_date = get_collection_date_range()
    return f"{start_date.isoformat()} ~ {end_date.isoformat()}"


def get_run_date_str():
    return datetime.now(UK_GMT).strftime('%Y-%m-%d')


def get_reviews_url(asin: str, page: int = 1) -> str:
    """
    Generate UK review URL for a given ASIN.

    Filters:
    - sortBy=recent: Sort by most recent
    - reviewerType=all_reviews: All reviewers
    - filterByStar=all_stars: All star ratings
    """
    return f'{AMAZON_BASE_URL}/product-reviews/{asin}/ref=cm_cr_arp_d_viewopt_sr?ie=UTF8&filterByStar=all_stars&reviewerType=all_reviews&sortBy=recent&pageNumber={page}'

# =============================================================================
# RATE LIMITING
# =============================================================================
MIN_DELAY = 2.0
MAX_DELAY = 3.5
MAX_RETRIES = 3

# =============================================================================
# USER AGENTS
# =============================================================================
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0',
]

# =============================================================================
# FILE PATHS
# =============================================================================
DATA_DIR = 'data'
SCRAPER_STATE_FILE = f'{DATA_DIR}/scraper_state_uk.json'

# =============================================================================
# AMAZON UK URLS
# =============================================================================
AMAZON_BASE_URL = 'https://www.amazon.co.uk'
LOGIN_URL = f'{AMAZON_BASE_URL}/ap/signin'
PRODUCTS_CSV = 'config/products_uk.csv'

# =============================================================================
# GOOGLE SHEETS
# =============================================================================
GOOGLE_SHEETS_URL = 'https://docs.google.com/spreadsheets/d/1NVUVShv5tAveINA9DdB2D21z71L3tF0In5JVK6LYX9s/edit'
SHEET_NAME = 'UK_amazone'

# =============================================================================
# SLACK CONFIGURATION
# =============================================================================
SLACK_BOT_TOKEN = os.getenv('SLACK_BOT_TOKEN', '')
SLACK_CHANNEL_ID = os.getenv('SLACK_CHANNEL_ID', '')
