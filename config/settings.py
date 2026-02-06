"""
Amazon Review Scraper - Configuration Settings
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
US_EST = pytz.timezone('US/Eastern')


# =============================================================================
# AMAZON CREDENTIALS (from .env file)
# =============================================================================
AMAZON_EMAIL = os.getenv('AMAZON_EMAIL', '')
AMAZON_PASSWORD = os.getenv('AMAZON_PASSWORD', '')

if not AMAZON_EMAIL or not AMAZON_PASSWORD:
    raise ValueError(
        "Missing Amazon credentials!\n"
        "Please create a .env file with:\n"
        "  AMAZON_EMAIL=your_email@example.com\n"
        "  AMAZON_PASSWORD=your_password"
    )

# =============================================================================
# TARGET PRODUCT (Default - for single product mode)
# =============================================================================
ASIN = 'B0B879FZBZ'  # BIODANCE Bio-Collagen Real Deep Mask
PRODUCT_NAME = 'BIODANCE Bio-Collagen Mask'

# =============================================================================
# DATE FILTER (Default - for single product mode)
# =============================================================================
DAYS_TO_SCRAPE = 30  # Collect reviews from last N days
CUTOFF_DATE = datetime.now() - timedelta(days=DAYS_TO_SCRAPE)


# =============================================================================
# DYNAMIC DATE FUNCTIONS (for daily batch mode)
# =============================================================================
COLLECTION_WINDOW_DAYS = 2

def get_yesterday_kst():
    now_kst = datetime.now(KST)
    yesterday = now_kst - timedelta(days=1)
    start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
    return start.replace(tzinfo=None), end.replace(tzinfo=None)


def get_yesterday_date_str():
    now_kst = datetime.now(KST)
    yesterday = now_kst - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')


def get_collection_date_range():
    """
    Amazon.com은 US EST 기준으로 리뷰 날짜를 표시.
    KST와 EST의 시차(14시간) + 리뷰 승인 지연을 고려하여
    2일 윈도우로 수집 범위를 설정.
    Returns: (start_date, end_date) as date objects
    """
    from datetime import date
    now_est = datetime.now(US_EST)
    end_date = now_est.date()
    start_date = end_date - timedelta(days=COLLECTION_WINDOW_DAYS)
    return start_date, end_date


def get_collection_date_range_str():
    start_date, end_date = get_collection_date_range()
    return f"{start_date.isoformat()} ~ {end_date.isoformat()}"


def get_run_date_str():
    return datetime.now(US_EST).strftime('%Y-%m-%d')


def get_reviews_url(asin: str, page: int = 1) -> str:
    """
    동적으로 ASIN별 리뷰 URL 생성
    
    필터 설정:
    - sortBy=recent: 최신순 정렬
    - reviewerType=all_reviews: 모든 리뷰어
    - filterByStar=all_stars: 모든 별점
    - filterByMediaType=all_content: 모든 미디어 타입 (텍스트, 이미지, 비디오)
    - filterByLanguage=all_languages: 모든 언어
    """
    return f'{AMAZON_BASE_URL}/product-reviews/{asin}?pageNumber={page}&sortBy=recent&reviewerType=all_reviews&filterByStar=all_stars&filterByMediaType=all_content&filterByLanguage=all_languages'

# =============================================================================
# RATE LIMITING (Optimized for speed while avoiding detection)
# =============================================================================
MIN_DELAY = 2.0  # Minimum delay between requests (seconds) - optimized from 4.0
MAX_DELAY = 3.5  # Maximum delay between requests (seconds) - optimized from 7.0
PAGE_BREAK_INTERVAL = 15  # Take a break every N pages - optimized from 10
PAGE_BREAK_DURATION = 20  # Break duration (seconds) - optimized from 30

# =============================================================================
# RETRY SETTINGS
# =============================================================================
MAX_RETRIES = 3
RETRY_DELAY = 10  # seconds
CAPTCHA_WAIT = 1800  # 30 minutes if CAPTCHA detected

# =============================================================================
# USER AGENTS (Rotation - Updated 2026)
# =============================================================================
USER_AGENTS = [
    # Chrome (Windows/Mac)
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    # Firefox (Windows/Mac)
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0',
    # Safari
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
    # Edge
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0',
]

# =============================================================================
# FILE PATHS
# =============================================================================
DATA_DIR = 'data'
COOKIES_FILE = f'{DATA_DIR}/cookies.json'
REVIEWS_FILE = f'{DATA_DIR}/reviews.csv'
CHECKPOINT_FILE = f'{DATA_DIR}/checkpoint.json'
SCRAPER_STATE_FILE = f'{DATA_DIR}/scraper_state.json'

# =============================================================================
# AMAZON URLS
# =============================================================================
AMAZON_BASE_URL = 'https://www.amazon.com'
REVIEWS_URL_TEMPLATE = f'{AMAZON_BASE_URL}/product-reviews/{ASIN}/ref=cm_cr_arp_d_paging_btm_next_{{page}}?pageNumber={{page}}&sortBy=recent'
LOGIN_URL = f'{AMAZON_BASE_URL}/ap/signin'
PRODUCT_URL = f'{AMAZON_BASE_URL}/dp/{ASIN}'

# =============================================================================
# SLACK CONFIGURATION
# =============================================================================
SLACK_BOT_TOKEN = os.getenv('SLACK_BOT_TOKEN', '')
SLACK_CHANNEL_ID = os.getenv('SLACK_CHANNEL_ID', 'C0ACH02BLG5')

# =============================================================================
# TOP PRODUCTS FOR DAILY SCRAPING
# =============================================================================
TOP_5_ASINS = [
    'B0B2RM68G2',  # Bio-Collagen Real Deep Mask (4ea) - 34,600 reviews
    'B0B879FZBZ',  # Bio-Collagen Real Deep Mask (16ea) - 34,600 reviews
    'B0FGJLJGFD',  # Rejuvenating Caviar PDRN Real Deep Mask (4ea) - 34,600 reviews
    'B0CWGSP1WY',  # Hydro Cera-nol Real Deep Mask (4ea) - 34,600 reviews
    'B0DDXV5KV4',  # Collagen Gel Toner Pads (60 Pads) - 1,561 reviews
]

PRODUCT_NAMES = {
    'B0B2RM68G2': 'Bio-Collagen Real Deep Mask (4ea)',
    'B0B879FZBZ': 'Bio-Collagen Real Deep Mask (16ea)',
    'B0FGJLJGFD': 'Rejuvenating Caviar PDRN Real Deep Mask (4ea)',
    'B0CWGSP1WY': 'Hydro Cera-nol Real Deep Mask (4ea)',
    'B0DDXV5KV4': 'Collagen Gel Toner Pads (60 Pads)',
}
