"""
Biodance 제품 리뷰 증분 수집 스크립트

매일 실행하면 신규 리뷰만 추가 수집합니다.
기존 review_id가 있으면 건너뛰고, 새 리뷰만 append합니다.

사용법:
    python scrapers/biodance/run_biodance_reviews.py
"""

import logging
import os
import sys
import time

# 프로젝트 루트를 sys.path에 추가
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from scrapers.biodance.biodance_review_crawler import BiodanceReviewCrawler
from publishers.google_sheets_publisher import GoogleSheetsPublisher
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# 프로젝트 루트 기준으로 데이터 디렉토리 설정
DATA_DIR = os.path.join(project_root, "data", "biodance")


def main():
    start_time = time.time()

    crawler = BiodanceReviewCrawler()
    json_path = os.path.join(DATA_DIR, "biodance_reviews_all.json")

    logger.info("Biodance 리뷰 증분 수집을 시작합니다...")
    results, new_count = crawler.collect_incremental(json_path)

    # 로컬 파일 저장 제거 - Google Sheets만 사용
    logger.info("로컬 파일 저장 생략 (Google Sheets가 Single Source of Truth)")

    # Google Sheets 발행 (신규)
    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    service_account_file = os.getenv("GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE", "config/service-account.json")

    if spreadsheet_id:
        try:
            logger.info("=" * 60)
            logger.info("Google Sheets 업데이트 시작...")
            publisher = GoogleSheetsPublisher(
                spreadsheet_id=spreadsheet_id,
                service_account_file=service_account_file,
            )
            stats = publisher.publish_incremental(results)

            logger.info("=" * 60)
            logger.info("✅ Google Sheets 업데이트 완료!")
            logger.info("신규 리뷰: %d개", stats["new_reviews"])
            logger.info("추가된 행: %d개", stats["appended_reviews"])
            logger.info("총 리뷰: %d개", stats["total_reviews"])
            logger.info("업데이트 제품: %d개", stats["updated_products"])
            logger.info(
                "스프레드시트: https://docs.google.com/spreadsheets/d/%s/edit",
                spreadsheet_id,
            )
        except FileNotFoundError as e:
            logger.error("Google Sheets 서비스 계정 파일 없음: %s", e)
            logger.error("docs/BIODANCE_SHEETS_SETUP.md를 참고하여 서비스 계정을 생성하세요")
        except Exception as e:
            logger.error("Google Sheets 업데이트 실패: %s", e, exc_info=True)
            raise
    else:
        logger.warning("GOOGLE_SHEETS_SPREADSHEET_ID 환경변수 미설정 - Sheets 업데이트 생략")

    # 요약 출력
    logger.info("=" * 60)
    logger.info("수집 완료!")
    logger.info("이번 신규 리뷰: %d개", new_count)
    logger.info("총 제품: %d개", results["total_products"])
    logger.info("총 리뷰: %d개 (누적)", results["total_reviews"])
    logger.info("-" * 60)

    for product in results["products"]:
        review_count = len(product["reviews"])
        rating = product["average_rating"]
        logger.info(
            "  %-50s | 평점 %s | 리뷰 %3d개",
            product["product_name"],
            rating,
            review_count,
        )

    logger.info("-" * 60)
    elapsed = time.time() - start_time

    # Slack 알림
    try:
        from src.slack_notifier import SlackNotifier
        slack = SlackNotifier()

        from datetime import datetime
        date_str = datetime.now().strftime('%Y-%m-%d')

        slack_results = []
        for product in results["products"]:
            slack_results.append({
                'product_name': product["product_name"],
                'review_count': len(product["reviews"]),
                'status': 'success',
            })

        slack.send_daily_scrape_report(
            date_str, slack_results, elapsed,
            channel_name='Biodance',
        )
        logger.info("Slack 알림 전송 완료")
    except Exception as e:
        logger.error(f"Slack 알림 실패: {e}")


if __name__ == "__main__":
    main()
