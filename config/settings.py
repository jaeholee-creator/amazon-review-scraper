"""
Amazon Review Scraper - Configuration Settings
"""
from datetime import datetime, timedelta

# =============================================================================
# TARGET PRODUCT
# =============================================================================
ASIN = 'B0B879FZBZ'  # BIODANCE Bio-Collagen Real Deep Mask
PRODUCT_NAME = 'BIODANCE Bio-Collagen Mask'

# =============================================================================
# DATE FILTER
# =============================================================================
DAYS_TO_SCRAPE = 30  # Collect reviews from last N days
CUTOFF_DATE = datetime.now() - timedelta(days=DAYS_TO_SCRAPE)

# =============================================================================
# RATE LIMITING (Conservative settings for account safety)
# =============================================================================
MIN_DELAY = 4.0  # Minimum delay between requests (seconds)
MAX_DELAY = 7.0  # Maximum delay between requests (seconds)
PAGE_BREAK_INTERVAL = 10  # Take a break every N pages
PAGE_BREAK_DURATION = 30  # Break duration (seconds)

# =============================================================================
# RETRY SETTINGS
# =============================================================================
MAX_RETRIES = 3
RETRY_DELAY = 10  # seconds
CAPTCHA_WAIT = 1800  # 30 minutes if CAPTCHA detected

# =============================================================================
# USER AGENTS (Rotation)
# =============================================================================
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
]

# =============================================================================
# FILE PATHS
# =============================================================================
DATA_DIR = 'data'
COOKIES_FILE = f'{DATA_DIR}/cookies.json'
REVIEWS_FILE = f'{DATA_DIR}/reviews.csv'
CHECKPOINT_FILE = f'{DATA_DIR}/checkpoint.json'

# =============================================================================
# AMAZON URLS
# =============================================================================
AMAZON_BASE_URL = 'https://www.amazon.com'
REVIEWS_URL_TEMPLATE = f'{AMAZON_BASE_URL}/product-reviews/{ASIN}/ref=cm_cr_arp_d_paging_btm_next_{{page}}?pageNumber={{page}}&sortBy=recent'
LOGIN_URL = f'{AMAZON_BASE_URL}/ap/signin'
PRODUCT_URL = f'{AMAZON_BASE_URL}/dp/{ASIN}'
