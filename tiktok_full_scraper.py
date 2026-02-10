#!/usr/bin/env python3
"""
TikTok Shop 전체 리뷰 1회 수집 스크립트.
모든 페이지를 순회하여 전체 리뷰를 Google Sheets에 업로드합니다.
"""
import asyncio
import logging
import os
import sys
from datetime import date, timedelta

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
)

os.makedirs("data/tiktok", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("data/tiktok/tiktok_full_scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


async def main():
    logger.info("=" * 80)
    logger.info("TikTok Shop 전체 리뷰 수집 시작 (1회성)")
    logger.info("=" * 80)

    # 전체 기간: 2년 전 ~ 오늘
    start_date = date.today() - timedelta(days=730)
    end_date = date.today()

    logger.info(f"날짜 범위: {start_date} ~ {end_date}")
    logger.info(f"최대 페이지: 200")

    headless = os.environ.get("TIKTOK_HEADLESS", "false").lower() == "true"

    scraper = TikTokShopScraper(
        email=TIKTOK_EMAIL,
        password=TIKTOK_PASSWORD,
        data_dir=TIKTOK_DATA_DIR,
        headless=headless,
        gmail_imap_email=TIKTOK_GMAIL_IMAP_EMAIL,
        gmail_imap_app_password=TIKTOK_GMAIL_IMAP_APP_PASSWORD,
    )

    try:
        # 브라우저 시작 및 로그인
        logged_in = await scraper.start()
        if not logged_in:
            logger.error("로그인 실패. 중단.")
            return

        # 전체 리뷰 수집 (max_pages 확대)
        reviews = await scraper.scrape_reviews(
            start_date=start_date,
            end_date=end_date,
            max_pages=200,
        )

        logger.info(f"수집 완료: {len(reviews)}개 리뷰")

        # Google Sheets 업로드
        if reviews:
            logger.info(f"Google Sheets 업로드 시작: {TIKTOK_SHEET_NAME}")
            publisher = TikTokGoogleSheetsPublisher(
                spreadsheet_id=TIKTOK_SPREADSHEET_ID,
                sheet_name=TIKTOK_SHEET_NAME,
                service_account_file="credentials.json",
            )
            result = publisher.publish_incremental({
                "reviews": reviews,
            })
            logger.info(
                f"업로드 완료: {result['appended_reviews']}개 신규 리뷰 추가 "
                f"(기존 {result['new_reviews'] - result['appended_reviews']}개 중복 제외)"
            )

    except Exception as e:
        logger.error(f"오류: {e}", exc_info=True)
    finally:
        await scraper.close()

    logger.info("=" * 80)
    logger.info("전체 리뷰 수집 완료")
    logger.info("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
