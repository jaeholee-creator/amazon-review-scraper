#!/usr/bin/env python3
"""
Shopee Daily Review Scraper
매일 최근 3일간의 Shopee 리뷰를 수집하여 Google Sheets에 업로드
"""
import logging
import os
import sys
import time
from datetime import datetime

from scrapers.shopee import ShopeeScraper
from publishers.shopee_sheets_publisher import ShopeeGoogleSheetsPublisher
from config.settings import (
    SHOPEE_SHOPS,
    SHOPEE_SPREADSHEET_ID,
    get_shopee_collection_date_range,
)

PUBLISHER_TYPE = os.environ.get('PUBLISHER_TYPE', 'bigquery')

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('data/shopee_scraper.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


def scrape_shopee_country(country_code: str) -> dict:
    """
    단일 국가의 Shopee 리뷰 수집

    Args:
        country_code: 국가 코드 ('sg', 'ph')

    Returns:
        스크래핑 결과 dict
    """
    shop_config = SHOPEE_SHOPS.get(country_code)
    if not shop_config:
        logger.error(f"Unknown country code: {country_code}")
        return {}

    start_date, end_date = get_shopee_collection_date_range()

    logger.info(f"=" * 80)
    logger.info(f"Shopee {country_code.upper()} 리뷰 수집 시작")
    logger.info(f"날짜 범위: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    logger.info(f"=" * 80)

    scraper = ShopeeScraper(
        country=shop_config['country'],
        userid=shop_config['userid'],
        shopid=shop_config['shopid']
    )

    try:
        result = scraper.scrape(start_date, end_date)
        logger.info(f"[{country_code.upper()}] 수집 완료: {result['total_reviews']}개 리뷰")
        return result
    except Exception as e:
        logger.error(f"[{country_code.upper()}] 스크래핑 실패: {e}", exc_info=True)
        return {}


def publish_reviews(country_code: str, result: dict) -> dict:
    """리뷰 데이터를 BigQuery 또는 Google Sheets에 업로드"""
    if not result or not result.get('reviews'):
        logger.info(f"[{country_code.upper()}] 업로드할 리뷰가 없습니다")
        return {}

    if PUBLISHER_TYPE == 'bigquery':
        return _publish_to_bigquery(country_code, result)
    return _publish_reviews(country_code, result)


def _publish_to_bigquery(country_code: str, result: dict) -> dict:
    """BigQuery에 업로드"""
    logger.info(f"[{country_code.upper()}] BigQuery 업로드 시작")

    try:
        from publishers.bigquery_publisher import BigQueryPublisher
        publisher = BigQueryPublisher(
            project_id=os.environ.get('GCP_PROJECT_ID', 'member-378109'),
            dataset_id=os.environ.get('BIGQUERY_DATASET_ID', 'jaeho'),
            table_id=os.environ.get('BIGQUERY_TABLE_ID', 'platform_reviews'),
            credentials_file='config/bigquery-service-account.json',
        )

        reviews = result.get('reviews', [])
        for r in reviews:
            r['platform_country'] = country_code.upper()
            r['author_country'] = None
        bq_result = publisher.publish_incremental(reviews, platform='shopee')
        logger.info(
            f"[{country_code.upper()}] BigQuery 업로드 완료: "
            f"insert={bq_result['inserted']}, update={bq_result['updated']}"
        )
        return {'appended_reviews': bq_result['inserted']}

    except Exception as e:
        logger.error(f"[{country_code.upper()}] BigQuery 업로드 실패: {e}", exc_info=True)
        return {}


def _publish_reviews(country_code: str, result: dict) -> dict:
    """Google Sheets에 업로드 (폴백)"""
    shop_config = SHOPEE_SHOPS.get(country_code)
    sheet_name = shop_config['sheet_name']

    logger.info(f"[{country_code.upper()}] Google Sheets 업로드 시작: {sheet_name}")

    try:
        publisher = ShopeeGoogleSheetsPublisher(
            spreadsheet_id=SHOPEE_SPREADSHEET_ID,
            sheet_name=sheet_name,
            service_account_file='credentials.json'
        )

        publish_result = publisher.publish_incremental(result)
        logger.info(
            f"[{country_code.upper()}] 업로드 완료: "
            f"{publish_result['appended_reviews']}개 신규 리뷰 추가"
        )
        return publish_result

    except Exception as e:
        logger.error(f"[{country_code.upper()}] 업로드 실패: {e}", exc_info=True)
        return {}


def main():
    """메인 실행 함수"""
    start_time = time.time()

    logger.info("=" * 80)
    logger.info("Shopee Daily Review Scraper 시작")
    logger.info(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)

    start_date, end_date = get_shopee_collection_date_range()
    date_str = f"{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}"
    results = {}

    # Singapore 수집
    logger.info("\n" + "=" * 80)
    logger.info("Singapore 리뷰 수집")
    logger.info("=" * 80)
    sg_result = scrape_shopee_country('sg')
    if sg_result:
        sg_publish = publish_reviews('sg', sg_result)
        results['sg'] = {
            'scrape': sg_result,
            'publish': sg_publish
        }

    # Philippines 수집
    logger.info("\n" + "=" * 80)
    logger.info("Philippines 리뷰 수집")
    logger.info("=" * 80)
    ph_result = scrape_shopee_country('ph')
    if ph_result:
        ph_publish = publish_reviews('ph', ph_result)
        results['ph'] = {
            'scrape': ph_result,
            'publish': ph_publish
        }

    elapsed = time.time() - start_time

    logger.info("\n" + "=" * 80)
    logger.info("최종 요약")
    logger.info("=" * 80)

    for country, data in results.items():
        scrape_data = data.get('scrape', {})
        publish_data = data.get('publish', {})

        logger.info(f"[{country.upper()}]")
        logger.info(f"  - 수집: {scrape_data.get('total_reviews', 0)}개")
        logger.info(f"  - 업로드: {publish_data.get('appended_reviews', 0)}개 신규")

    logger.info("=" * 80)
    logger.info("Shopee Daily Review Scraper 완료")
    logger.info("=" * 80)

    # Slack 알림
    try:
        from src.slack_notifier import SlackNotifier
        slack = SlackNotifier()

        slack_results = []
        for country, data in results.items():
            scrape_data = data.get('scrape', {})
            reviews = scrape_data.get('reviews', [])
            review_count = len(reviews)
            unique_products = len(set(r.get('product_name', '') for r in reviews if r.get('product_name')))
            publish_data = data.get('publish', {})
            new_count = publish_data.get('appended_reviews', 0)
            slack_results.append({
                'product_name': f'{country.upper()} - {unique_products}개 제품 (+{new_count} new)',
                'review_count': review_count,
                'status': 'success' if review_count >= 0 else 'failed',
            })

        slack.send_daily_scrape_report(
            date_str, slack_results, elapsed,
            channel_name='Shopee',
        )
        logger.info("Slack 알림 전송 완료")
    except Exception as e:
        logger.error(f"Slack 알림 실패: {e}")

    return results


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"❌ 실행 중 에러 발생: {e}", exc_info=True)
        sys.exit(1)
