#!/usr/bin/env python3
"""
Shopee Google Sheets → BigQuery 적재 스크립트

Node.js 크롤러가 'shopee' 시트에 저장한 최신 데이터를 BigQuery로 적재
Airflow DAG에서 Node.js 크롤링 후 호출됨
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
SERVICE_ACCOUNT_FILE = 'credentials.json'  # 서버: credentials.json, 로컬: config/service-account.json

# BigQuery 설정
BIGQUERY_CONFIG = {
    'project_id': 'ax-test-jaeho',
    'dataset_id': 'ax_cs',
    'table_id': 'platform_reviews',
    'credentials_file': 'config/bigquery-service-account.json',
}


def fetch_recent_shopee_data(days=3):
    """
    Google Sheets에서 최근 N일간의 shopee 데이터 가져오기

    Node.js 크롤러는 최근 3일 데이터만 수집하므로,
    마지막 실행 이후 새로 추가된 데이터만 가져옴
    """
    logger.info("Google Sheets 인증 중...")
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
    )

    service = build('sheets', 'v4', credentials=credentials)

    logger.info(f"'{SHEET_NAME}' 시트에서 최근 {days}일 데이터 가져오는 중...")

    # 전체 데이터 가져오기 (submit_date 기준 필터링)
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f'{SHEET_NAME}!A:Q'
    ).execute()

    values = result.get('values', [])
    if not values:
        logger.error("시트가 비어있습니다.")
        return [], []

    headers = values[0]
    all_rows = values[1:]

    # submit_date 컬럼 인덱스 찾기
    try:
        submit_date_idx = headers.index('submit_date')
    except ValueError:
        logger.error("'submit_date' 컬럼을 찾을 수 없습니다.")
        return headers, []

    # 최근 N일 데이터 필터링
    from datetime import date, timedelta
    cutoff_date = date.today() - timedelta(days=days)

    filtered_rows = []
    for row in all_rows:
        if len(row) > submit_date_idx:
            try:
                row_date_str = row[submit_date_idx]
                row_date = datetime.strptime(row_date_str, '%Y-%m-%d').date()
                if row_date >= cutoff_date:
                    filtered_rows.append(row)
            except (ValueError, IndexError):
                continue

    logger.info(f"전체 {len(all_rows):,}개 행 중 최근 {days}일: {len(filtered_rows):,}개 행")

    return headers, filtered_rows


def transform_to_bigquery_format(headers, data_rows):
    """
    Node.js 크롤러 형식 → BigQuery platform_reviews 형식 변환
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
        # _to_date_str가 다양한 형식을 처리하므로 문자열 그대로 전달
        review_date = get_val('submit_date') or get_val('submit_time') or ''

        # rating_star 안전 변환
        rating_star = get_val('rating_star', '0')
        try:
            star_value = int(float(rating_star))
        except:
            star_value = 0

        # BigQuery 형식으로 변환
        # country 필드는 마켓플레이스(SG/TW/PH)이므로 platform_country에 저장
        review = {
            'review_id': str(get_val('comment_id')),
            'collected_at': datetime.now(),
            'product_name': get_val('product_name'),
            'product_id': str(get_val('product_id')),
            'author': get_val('user_name', 'Unknown'),
            'platform_country': get_val('country', ''),
            'author_country': None,  # Shopee Seller Centre는 리뷰어 국가 미제공
            'star': star_value,
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


def upload_to_bigquery(reviews):
    """BigQuery에 업로드 (MERGE로 중복 제거)"""
    if not reviews:
        logger.info("업로드할 리뷰가 없습니다.")
        return {'inserted': 0, 'updated': 0}

    logger.info(f"BigQuery에 {len(reviews):,}개 리뷰 업로드 중...")

    publisher = BigQueryPublisher(
        project_id=BIGQUERY_CONFIG['project_id'],
        dataset_id=BIGQUERY_CONFIG['dataset_id'],
        table_id=BIGQUERY_CONFIG['table_id'],
        credentials_file=BIGQUERY_CONFIG['credentials_file'],
    )

    result = publisher.publish_incremental(reviews, platform='shopee')

    logger.info(
        f"BigQuery 업로드 완료: insert={result['inserted']}, update={result['updated']}"
    )

    return result


def main():
    """메인 실행 함수"""
    logger.info("="*80)
    logger.info("Shopee Google Sheets → BigQuery 적재 시작")
    logger.info("="*80)

    # 1. Google Sheets에서 최근 3일 데이터 가져오기
    headers, data_rows = fetch_recent_shopee_data(days=3)
    if not data_rows:
        logger.info("새로운 데이터가 없습니다.")
        return

    # 2. BigQuery 형식으로 변환
    reviews = transform_to_bigquery_format(headers, data_rows)

    # 3. BigQuery에 업로드
    result = upload_to_bigquery(reviews)

    logger.info("="*80)
    logger.info(f"적재 완료! (insert={result['inserted']}, update={result['updated']})")
    logger.info("="*80)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"❌ 실행 중 에러 발생: {e}", exc_info=True)
        sys.exit(1)
