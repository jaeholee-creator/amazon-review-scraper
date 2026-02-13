#!/usr/bin/env python3
"""
TikTok Shop Daily Review Scraper
매일 최근 리뷰를 수집하여 Google Sheets에 업로드
"""
import asyncio
import logging
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime

from scrapers.tiktok import TikTokShopScraper
from publishers.tiktok_sheets_publisher import TikTokGoogleSheetsPublisher
from config.settings import (
    TIKTOK_EMAIL,
    TIKTOK_PASSWORD,
    TIKTOK_SPREADSHEET_ID,
    TIKTOK_SHEET_NAME,
    TIKTOK_DATA_DIR,
    TIKTOK_GMAIL_SERVICE_ACCOUNT_FILE,
    TIKTOK_GMAIL_TARGET_EMAIL,
    TIKTOK_GMAIL_IMAP_EMAIL,
    TIKTOK_GMAIL_IMAP_APP_PASSWORD,
    get_tiktok_collection_date_range,
)

PUBLISHER_TYPE = os.environ.get('PUBLISHER_TYPE', 'bigquery')

# 로깅 설정
os.makedirs("data/tiktok", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("data/tiktok/tiktok_scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def ensure_xvfb() -> bool:
    """
    Xvfb 가상 디스플레이 설정 (headless 환경에서 headed 브라우저 실행용).
    DISPLAY가 이미 설정되어 있으면 건너뛰고, 없으면 Xvfb를 시작합니다.

    Returns:
        True if DISPLAY is available (either existing or newly started)
    """
    # 이미 DISPLAY가 설정되어 있으면 (데스크탑 환경 또는 이미 Xvfb 실행중)
    if os.environ.get("DISPLAY"):
        logger.info(f"DISPLAY 이미 설정됨: {os.environ['DISPLAY']}")
        return True

    # Xvfb가 설치되어 있는지 확인
    if not shutil.which("Xvfb"):
        logger.info("Xvfb 미설치 - headless 모드로 실행")
        return False

    # Xvfb 시작 (디스플레이 :99)
    display_num = ":99"
    try:
        # 기존 Xvfb 프로세스 확인
        result = subprocess.run(
            ["pgrep", "-f", f"Xvfb {display_num}"],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            logger.info(f"Xvfb 이미 실행 중 (DISPLAY={display_num})")
            os.environ["DISPLAY"] = display_num
            return True

        # Xvfb 시작
        subprocess.Popen(
            ["Xvfb", display_num, "-screen", "0", "1920x1080x24", "-ac"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        os.environ["DISPLAY"] = display_num
        logger.info(f"Xvfb 시작 완료 (DISPLAY={display_num})")

        # Xvfb 안정화 대기
        import time
        time.sleep(1)
        return True

    except Exception as e:
        logger.warning(f"Xvfb 시작 실패: {e} - headless 모드로 실행")
        return False


async def scrape_tiktok_reviews() -> dict:
    """
    TikTok Shop 리뷰 수집

    Returns:
        스크래핑 결과 dict
    """
    start_date, end_date = get_tiktok_collection_date_range()

    logger.info("=" * 80)
    logger.info("TikTok Shop 리뷰 수집 시작")
    logger.info(f"날짜 범위: {start_date} ~ {end_date}")
    logger.info("=" * 80)

    # headless 모드: 환경변수로 제어 (CI에서는 headless, 로컬에서는 headed 가능)
    headless = os.environ.get("TIKTOK_HEADLESS", "true").lower() == "true"

    scraper = TikTokShopScraper(
        email=TIKTOK_EMAIL,
        password=TIKTOK_PASSWORD,
        data_dir=TIKTOK_DATA_DIR,
        headless=headless,
        gmail_service_account_file=TIKTOK_GMAIL_SERVICE_ACCOUNT_FILE,
        gmail_target_email=TIKTOK_GMAIL_TARGET_EMAIL,
        gmail_imap_email=TIKTOK_GMAIL_IMAP_EMAIL,
        gmail_imap_app_password=TIKTOK_GMAIL_IMAP_APP_PASSWORD,
    )

    try:
        result = await scraper.scrape(start_date, end_date)
        logger.info(f"수집 완료: {result.get('total_reviews', 0)}개 리뷰")
        return result
    except Exception as e:
        logger.error(f"스크래핑 실패: {e}", exc_info=True)
        return {"status": "failed", "error": str(e), "reviews": [], "total_reviews": 0}


def publish_reviews(result: dict) -> dict:
    """리뷰 데이터를 BigQuery 또는 Google Sheets에 업로드"""
    if not result or not result.get("reviews"):
        logger.info("업로드할 리뷰가 없습니다")
        return {}

    if PUBLISHER_TYPE == 'bigquery':
        return _publish_to_bigquery(result)
    return _publish_to_sheets(result)


def _publish_to_bigquery(result: dict) -> dict:
    """BigQuery에 업로드"""
    logger.info("BigQuery 업로드 시작")

    try:
        from publishers.bigquery_publisher import BigQueryPublisher
        publisher = BigQueryPublisher(
            project_id=os.environ.get('GCP_PROJECT_ID', 'ax-test-jaeho'),
            dataset_id=os.environ.get('BIGQUERY_DATASET_ID', 'ax_cs'),
            table_id=os.environ.get('BIGQUERY_TABLE_ID', 'platform_reviews'),
            credentials_file='config/bigquery-service-account.json',
        )

        reviews = result.get('reviews', [])
        bq_result = publisher.publish_incremental(reviews, platform='tiktok')
        logger.info(
            f"BigQuery 업로드 완료: insert={bq_result['inserted']}, update={bq_result['updated']}"
        )
        return {'appended_reviews': bq_result['inserted']}

    except Exception as e:
        logger.error(f"BigQuery 업로드 실패: {e}", exc_info=True)
        return {}


def _publish_to_sheets(result: dict) -> dict:
    """Google Sheets에 업로드 (폴백)"""
    logger.info(f"Google Sheets 업로드 시작: {TIKTOK_SHEET_NAME}")

    try:
        publisher = TikTokGoogleSheetsPublisher(
            spreadsheet_id=TIKTOK_SPREADSHEET_ID,
            sheet_name=TIKTOK_SHEET_NAME,
            service_account_file="credentials.json",
        )

        publish_result = publisher.publish_incremental(result)
        logger.info(
            f"업로드 완료: {publish_result['appended_reviews']}개 신규 리뷰 추가"
        )
        return publish_result

    except Exception as e:
        logger.error(f"업로드 실패: {e}", exc_info=True)
        return {}


async def main():
    """메인 실행 함수"""
    start_time = time.time()

    logger.info("=" * 80)
    logger.info("TikTok Shop Daily Review Scraper 시작")
    logger.info(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)

    # Xvfb 가상 디스플레이 설정 (headed 모드로 캡차 우회)
    xvfb_available = ensure_xvfb()
    if xvfb_available:
        logger.info("Xvfb 사용 가능 → headed 모드로 실행 (캡차 우회)")
    else:
        logger.info("Xvfb 미사용 → headless + stealth 모드로 실행")

    start_date, end_date = get_tiktok_collection_date_range()
    date_str = f"{start_date} ~ {end_date}"

    # 1. 리뷰 수집
    scrape_result = await scrape_tiktok_reviews()

    # 2. 데이터 업로드 (BigQuery 또는 Google Sheets)
    publish_result = {}
    if scrape_result.get("status") == "success":
        publish_result = publish_reviews(scrape_result)
    else:
        logger.warning(f"스크래핑 실패: {scrape_result.get('error', 'Unknown')}")

    elapsed = time.time() - start_time

    # 3. 최종 요약
    total_reviews = scrape_result.get("total_reviews", 0)
    appended = publish_result.get("appended_reviews", 0)
    status = scrape_result.get("status", "unknown")

    logger.info("\n" + "=" * 80)
    logger.info("최종 요약")
    logger.info("=" * 80)
    logger.info(f"  수집: {total_reviews}개 리뷰")
    logger.info(f"  업로드: {appended}개 신규")
    logger.info(f"  상태: {status}")
    logger.info("=" * 80)
    logger.info("TikTok Shop Daily Review Scraper 완료")
    logger.info("=" * 80)

    # 4. Slack 알림
    try:
        from src.slack_notifier import SlackNotifier
        slack = SlackNotifier()

        error_msg = scrape_result.get('error', '')

        slack_results = [{
            'product_name': f'US (+{appended} new)',
            'review_count': total_reviews,
            'status': status,
            'error_message': error_msg if status != 'success' else '',
        }]

        slack.send_daily_scrape_report(
            date_str, slack_results, elapsed,
            channel_name='TikTok Shop',
        )
        logger.info("Slack 알림 전송 완료")
    except Exception as e:
        logger.error(f"Slack 알림 실패: {e}")

    return {
        "scrape": scrape_result,
        "publish": publish_result,
    }


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"실행 중 에러 발생: {e}", exc_info=True)
        sys.exit(1)
