#!/usr/bin/env python3
"""
BigQuery 테이블 생성 스크립트
- 프로젝트: ax-test-jaeho
- 데이터셋: ax_cs
- 테이블: platform_reviews
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import bigquery
from google.oauth2.service_account import Credentials

# 서비스 계정 인증
SERVICE_ACCOUNT_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "config", "bigquery-service-account.json"
)

PROJECT_ID = "ax-test-jaeho"
DATASET_ID = "ax_cs"
TABLE_ID = "platform_reviews"

DDL = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
(
  review_id STRING NOT NULL OPTIONS(description='리뷰 고유 ID | Amazon: R123ABC | Shopee: cmtid | TikTok: MD5 해시'),
  platform STRING NOT NULL OPTIONS(description='플랫폼 구분자 | 값: amazon, shopee, tiktok'),
  collected_at TIMESTAMP NOT NULL OPTIONS(description='리뷰 수집 시각 (KST) | 파티셔닝 키'),
  product_name STRING OPTIONS(description='제품명'),
  product_id STRING OPTIONS(description='제품 ID | Amazon: ASIN | Shopee: itemid | TikTok: product_id'),
  author STRING OPTIONS(description='리뷰 작성자 이름'),
  author_country STRING OPTIONS(description='작성자 국가 | TikTok: NULL'),
  star FLOAT64 OPTIONS(description='별점 (1.0 ~ 5.0)'),
  title STRING OPTIONS(description='리뷰 제목 | TikTok: NULL'),
  content STRING OPTIONS(description='리뷰 본문'),
  date DATE OPTIONS(description='리뷰 작성 날짜'),
  verified_purchase BOOLEAN OPTIONS(description='구매 확인 여부 | TikTok: NULL'),
  item_type STRING OPTIONS(description='상품 옵션/변형 | TikTok: NULL'),
  reply_content STRING OPTIONS(description='판매자 답변 | Amazon/Shopee'),
  seller_reply STRING OPTIONS(description='판매자 답변 | TikTok 전용'),
  reply_count INT64 OPTIONS(description='답변 개수 | TikTok 전용'),
  image_urls STRING OPTIONS(description='이미지 URL 목록 | 세미콜론 구분'),
  video_urls STRING OPTIONS(description='비디오 URL 목록 | TikTok: NULL'),
  has_video BOOLEAN OPTIONS(description='비디오 포함 여부 | TikTok 전용'),
  likes_count INT64 OPTIONS(description='좋아요 수 | TikTok: NULL'),
  detailed_rating_product FLOAT64 OPTIONS(description='제품 품질 평점 | Shopee 전용'),
  detailed_rating_seller FLOAT64 OPTIONS(description='판매자 서비스 평점 | Shopee 전용'),
  detailed_rating_delivery FLOAT64 OPTIONS(description='배송 서비스 평점 | Shopee 전용'),
  order_id STRING OPTIONS(description='주문 ID | TikTok 전용'),
  sku STRING OPTIONS(description='SKU/상품 변형 | TikTok 전용'),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description='레코드 최초 생성 시각'),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description='레코드 최종 수정 시각')
)
PARTITION BY DATE(collected_at)
CLUSTER BY platform, product_id, review_id
OPTIONS(
  description='통합 플랫폼 리뷰 데이터 (Amazon, Shopee, TikTok)',
  require_partition_filter=FALSE,
  labels=[("env", "production"), ("data_type", "reviews"), ("owner", "ax_cs_team")]
)
"""


def main():
    print("=" * 60)
    print("BigQuery 테이블 생성")
    print(f"프로젝트: {PROJECT_ID}")
    print(f"데이터셋: {DATASET_ID}")
    print(f"테이블: {TABLE_ID}")
    print("=" * 60)

    # 인증
    credentials = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

    # 데이터셋 확인/생성
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        client.get_dataset(dataset_ref)
        print(f"데이터셋 '{DATASET_ID}' 이미 존재합니다.")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "asia-northeast3"
        client.create_dataset(dataset)
        print(f"데이터셋 '{DATASET_ID}' 생성 완료.")

    # 테이블 생성
    print(f"\n테이블 '{TABLE_ID}' 생성 중...")
    job = client.query(DDL)
    job.result()
    print(f"테이블 '{TABLE_ID}' 생성 완료!")

    # 검증
    table = client.get_table(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    print(f"\n검증 완료:")
    print(f"  - 컬럼 수: {len(table.schema)}")
    print(f"  - 파티셔닝: {table.time_partitioning}")
    print(f"  - 클러스터링: {table.clustering_fields}")
    print(f"  - 행 수: {table.num_rows}")

    # 컬럼 목록 출력
    print(f"\n스키마 ({len(table.schema)}개 컬럼):")
    for field in table.schema:
        null_str = "NOT NULL" if field.mode == "REQUIRED" else "NULLABLE"
        print(f"  {field.name:<30} {field.field_type:<12} {null_str}")


if __name__ == "__main__":
    main()
