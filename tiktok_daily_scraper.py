#!/usr/bin/env python3
"""
TikTok Shop Daily Review Scraper
매일 최근 리뷰를 수집하여 Google Sheets에 업로드
"""
import asyncio
import logging
import os
import sys
from datetime import datetime

from scrapers.tiktok import TikTokShopScraper
from publishers.tiktok_sheets_publisher import TikTokGoogleSheetsPublisher
from config.settings import (
    TIKTOK_EMAIL,
    TIKTOK_PASSWORD,
    TIKTOK_SPREADSHEET_ID,
    TIKTOK_SHEET_NAME,
    TIKTOK_DATA_DIR,
    TIKTOK_GMAIL_IMAP_EMAIL,
    TIKTOK_GMAIL_IMAP_APP_PASSWORD,
    get_tiktok_collection_date_range,
)

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


def publish_to_sheets(result: dict) -> dict:
    """
    Google Sheets에 업로드

    Args:
        result: 스크래핑 결과

    Returns:
        업로드 결과 dict
    """
    if not result or not result.get("reviews"):
        logger.info("업로드할 리뷰가 없습니다")
        return {}

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
    logger.info("=" * 80)
    logger.info("TikTok Shop Daily Review Scraper 시작")
    logger.info(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)

    # 1. 리뷰 수집
    scrape_result = await scrape_tiktok_reviews()

    # 2. Google Sheets 업로드
    publish_result = {}
    if scrape_result.get("status") == "success":
        publish_result = publish_to_sheets(scrape_result)
    else:
        logger.warning(f"스크래핑 실패: {scrape_result.get('error', 'Unknown')}")

    # 3. 최종 요약
    logger.info("\n" + "=" * 80)
    logger.info("최종 요약")
    logger.info("=" * 80)
    logger.info(f"  수집: {scrape_result.get('total_reviews', 0)}개 리뷰")
    logger.info(f"  업로드: {publish_result.get('appended_reviews', 0)}개 신규")
    logger.info(f"  상태: {scrape_result.get('status', 'unknown')}")
    logger.info("=" * 80)
    logger.info("TikTok Shop Daily Review Scraper 완료")
    logger.info("=" * 80)

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
