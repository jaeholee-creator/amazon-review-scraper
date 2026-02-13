#!/usr/bin/env python3
"""
Shopee 시트 → BigQuery 마이그레이션 스크립트

Google Sheets의 'shopee' 시트 데이터를 BigQuery platform_reviews 테이블로 마이그레이션
Node.js 크롤러 형식 → BigQuery 표준 형식 변환
"""
import logging
import sys
from datetime import datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build
from publishers.bigquery_publisher import BigQueryPublisher

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Google Sheets 설정
SPREADSHEET_ID = '1NVUVShv5tAveINA9DdB2D21z71L3tF0In5JVK6LYX9s'
SHEET_NAME = 'shopee'
SERVICE_ACCOUNT_FILE = 'config/service-account.json'

# BigQuery 설정
BIGQUERY_CONFIG = {
    'project_id': 'ax-test-jaeho',
    'dataset_id': 'ax_cs',
    'table_id': 'platform_reviews',
    'credentials_file': 'config/bigquery-service-account.json',
}


def fetch_shopee_data():
    """Google Sheets에서 shopee 시트 전체 데이터 가져오기"""
    logger.info("Google Sheets 인증 중...")
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
    )

    service = build('sheets', 'v4', credentials=credentials)

    logger.info(f"'{SHEET_NAME}' 시트에서 데이터 가져오는 중...")
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f'{SHEET_NAME}!A:Q'
    ).execute()

    values = result.get('values', [])
    if not values:
        logger.error("시트가 비어있습니다.")
        return []

    headers = values[0]
    data_rows = values[1:]

    logger.info(f"헤더: {headers}")
    logger.info(f"총 {len(data_rows):,}개 행 로드됨")

    return headers, data_rows


def transform_to_bigquery_format(headers, data_rows):
    """
    Node.js 크롤러 형식 → BigQuery platform_reviews 형식 변환

    Node.js 컬럼: country, comment_id, order_sn, user_name, user_id, rating_star,
                  comment, product_name, product_id, model_name, images,
                  reply_comment, reply_time, submit_time, submit_date, status, low_rating_reasons

    BigQuery 컬럼: review_id, collected_at, product_name, product_id, author, author_country,
                   star, title, content, date, verified_purchase, item_type, reply_content,
                   image_urls, video_urls, likes_count, detailed_rating_*
    """
    logger.info("BigQuery 형식으로 변환 중...")

    # 헤더 인덱스 매핑
    header_map = {h: i for i, h in enumerate(headers)}

    reviews = []
    for row in data_rows:
        # 빈 행 스킵
        if not row or len(row) == 0:
            continue

        # 데이터 추출 (인덱스 에러 방지)
        def get_val(key, default=''):
            idx = header_map.get(key, -1)
            return row[idx] if idx != -1 and idx < len(row) else default

        # submit_date를 그대로 사용 (YYYY-MM-DD 문자열)
        review_date = get_val('submit_date') or get_val('submit_time') or ''

        # BigQuery 형식으로 변환
        review = {
            'review_id': get_val('comment_id'),
            'collected_at': datetime.now(),
            'product_name': get_val('product_name'),
            'product_id': get_val('product_id'),
            'author': get_val('user_name', 'Unknown'),
            'platform_country': get_val('country', ''),
            'author_country': None,  # Shopee Seller Centre는 리뷰어 국가 미제공
            'star': int(get_val('rating_star', '0')) if get_val('rating_star').isdigit() else 0,
            'title': '',  # Shopee에는 title 없음
            'content': get_val('comment'),
            'date': review_date,
            'verified_purchase': True,  # Shopee Seller Centre는 검증된 구매만 표시
            'item_type': get_val('model_name'),
            'reply_content': get_val('reply_comment'),
            'image_urls': get_val('images'),
            'video_urls': '',  # Shopee 크롤러는 비디오 수집 안 함
            'likes_count': 0,
            'detailed_rating_product': None,
            'detailed_rating_seller': None,
            'detailed_rating_delivery': None,
        }

        reviews.append(review)

    logger.info(f"총 {len(reviews):,}개 리뷰 변환 완료")
    return reviews


def upload_to_bigquery(reviews, batch_size=1000):
    """BigQuery에 배치 업로드"""
    logger.info("BigQuery에 업로드 중...")

    publisher = BigQueryPublisher(
        project_id=BIGQUERY_CONFIG['project_id'],
        dataset_id=BIGQUERY_CONFIG['dataset_id'],
        table_id=BIGQUERY_CONFIG['table_id'],
        credentials_file=BIGQUERY_CONFIG['credentials_file'],
    )

    total_inserted = 0
    total_updated = 0

    # 배치 처리
    for i in range(0, len(reviews), batch_size):
        batch = reviews[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(reviews) + batch_size - 1) // batch_size

        logger.info(f"배치 {batch_num}/{total_batches} 업로드 중... ({len(batch):,}개)")

        result = publisher.publish_incremental(batch, platform='shopee')
        total_inserted += result['inserted']
        total_updated += result['updated']

        logger.info(
            f"배치 {batch_num} 완료: insert={result['inserted']}, update={result['updated']}"
        )

    logger.info(
        f"\n{'='*80}\n"
        f"마이그레이션 완료!\n"
        f"총 삽입: {total_inserted:,}개\n"
        f"총 업데이트: {total_updated:,}개\n"
        f"{'='*80}"
    )

    return {'inserted': total_inserted, 'updated': total_updated}


def main():
    """메인 실행 함수"""
    logger.info("="*80)
    logger.info("Shopee 시트 → BigQuery 마이그레이션 시작")
    logger.info("="*80)

    # 1. Google Sheets 데이터 가져오기
    headers, data_rows = fetch_shopee_data()
    if not data_rows:
        logger.error("데이터가 없습니다. 종료합니다.")
        return

    # 2. BigQuery 형식으로 변환
    reviews = transform_to_bigquery_format(headers, data_rows)

    # 3. BigQuery에 업로드
    result = upload_to_bigquery(reviews)

    logger.info("="*80)
    logger.info("마이그레이션 성공!")
    logger.info("="*80)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"❌ 실행 중 에러 발생: {e}", exc_info=True)
        sys.exit(1)
