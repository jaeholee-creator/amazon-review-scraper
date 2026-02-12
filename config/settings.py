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
    import logging
    logging.getLogger(__name__).warning(
        "Amazon credentials not set. Amazon US scraper will not work. "
        "Set AMAZON_EMAIL and AMAZON_PASSWORD in .env or environment variables."
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
COLLECTION_WINDOW_DAYS = 3  # 최근 3일간 리뷰 수집

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
    """
    return f'{AMAZON_BASE_URL}/product-reviews/{asin}/ref=cm_cr_arp_d_viewopt_sr?ie=UTF8&filterByStar=all_stars&reviewerType=all_reviews&sortBy=recent&pageNumber={page}'

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
# GOOGLE SHEETS
# =============================================================================
GOOGLE_SHEETS_URL = 'https://docs.google.com/spreadsheets/d/1NVUVShv5tAveINA9DdB2D21z71L3tF0In5JVK6LYX9s/edit'
SHEET_NAME = 'US_amazone'
PRODUCTS_CSV = 'config/products.csv'

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

# =============================================================================
# LOAD ALL PRODUCTS FROM CSV
# =============================================================================
def get_all_asins_from_csv():
    """
    products.csv에서 모든 ASIN 로드 (전체 제품 크롤링용)

    Returns:
        list: ASIN 리스트
    """
    import csv
    import os

    csv_path = os.path.join(os.path.dirname(__file__), 'products.csv')
    asins = []
    names = {}

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                asin = row.get('asin', '').strip()
                name = row.get('name', '').strip()
                if asin:
                    asins.append(asin)
                    if name:
                        names[asin] = name
        return asins, names
    except FileNotFoundError:
        import logging
        logging.getLogger(__name__).warning(f"products.csv not found at {csv_path}, falling back to TOP_5_ASINS")
        return TOP_5_ASINS, PRODUCT_NAMES
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Error loading products.csv: {e}, falling back to TOP_5_ASINS")
        return TOP_5_ASINS, PRODUCT_NAMES

# 전체 제품 로드 (기본 동작)
ALL_ASINS, ALL_PRODUCT_NAMES = get_all_asins_from_csv()

# =============================================================================
# SHOPEE CONFIGURATION
# =============================================================================
SHOPEE_SHOPS = {
    'sg': {
        'userid': '951704668',
        'shopid': '951591050',
        'country': 'sg',
        'sheet_name': 'SG_shopee',
        'timezone': pytz.timezone('Asia/Singapore')
    },
    'ph': {
        'userid': '952208252',
        'shopid': '952094055',
        'country': 'ph',
        'sheet_name': 'PH_shopee',
        'timezone': pytz.timezone('Asia/Manila')
    }
}

# Shopee 스프레드시트 ID (기존과 동일)
SHOPEE_SPREADSHEET_ID = '1NVUVShv5tAveINA9DdB2D21z71L3tF0In5JVK6LYX9s'

def get_shopee_collection_date_range():
    """
    Shopee 리뷰 수집 날짜 범위: today() ~ today()-3

    Returns: (start_date, end_date) as datetime objects
    """
    from datetime import date
    now = datetime.now()
    end_date = now
    start_date = now - timedelta(days=COLLECTION_WINDOW_DAYS)
    return start_date, end_date


# =============================================================================
# TIKTOK SHOP CONFIGURATION
# =============================================================================
TIKTOK_EMAIL = os.getenv('TIKTOK_EMAIL', '')
TIKTOK_PASSWORD = os.getenv('TIKTOK_PASSWORD', '')

# Gmail IMAP (TikTok 인증 코드 자동 읽기용)
TIKTOK_GMAIL_IMAP_EMAIL = os.getenv('TIKTOK_GMAIL_IMAP_EMAIL', '')
TIKTOK_GMAIL_IMAP_APP_PASSWORD = os.getenv('TIKTOK_GMAIL_IMAP_APP_PASSWORD', '')

# EulerStream API (TikTok 캡차 자동 풀기, 무료 25건/일)
EULER_STREAM_API_KEY = os.getenv('EULER_STREAM_API_KEY', '')

TIKTOK_SPREADSHEET_ID = '1NVUVShv5tAveINA9DdB2D21z71L3tF0In5JVK6LYX9s'
TIKTOK_SHEET_NAME = 'US_TIkTOK'
TIKTOK_DATA_DIR = 'data/tiktok'


def get_tiktok_collection_date_range():
    """
    TikTok Shop 리뷰 수집 날짜 범위.
    US EST 기준, 최근 COLLECTION_WINDOW_DAYS일간 수집.

    Returns: (start_date, end_date) as date objects
    """
    from datetime import date
    now_est = datetime.now(US_EST)
    end_date = now_est.date()
    start_date = end_date - timedelta(days=COLLECTION_WINDOW_DAYS)
    return start_date, end_date
