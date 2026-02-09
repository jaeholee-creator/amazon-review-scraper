"""
기존 JSON 데이터를 Google Sheets로 마이그레이션

사용법:
    python scripts/migrate_to_sheets.py

환경변수:
    GOOGLE_SHEETS_SPREADSHEET_ID: 스프레드시트 ID (필수)
    GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE: service-account.json 경로 (선택)
"""

import json
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.google_sheets_publisher import GoogleSheetsPublisher
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def migrate():
    """기존 JSON 데이터를 Google Sheets로 마이그레이션"""
    # 1. JSON 파일 로드
    json_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data",
        "biodance",
        "biodance_reviews_all.json",
    )

    if not os.path.exists(json_path):
        logger.error("JSON 파일이 없습니다: %s", json_path)
        logger.error("먼저 run_biodance_reviews.py를 실행하여 데이터를 수집하세요")
        sys.exit(1)

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    logger.info("=" * 60)
    logger.info("마이그레이션 시작")
    logger.info("JSON 파일: %s", json_path)
    logger.info("총 제품: %d개", data.get("total_products", 0))
    logger.info("총 리뷰: %d개", data.get("total_reviews", 0))
    logger.info("=" * 60)

    # 2. 환경변수 확인
    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    if not spreadsheet_id:
        logger.error("GOOGLE_SHEETS_SPREADSHEET_ID 환경변수가 설정되지 않았습니다")
        logger.error(".env 파일을 확인하세요")
        sys.exit(1)

    service_account_file = os.getenv(
        "GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE", "config/service-account.json"
    )

    # 3. Google Sheets 발행
    try:
        publisher = GoogleSheetsPublisher(
            spreadsheet_id=spreadsheet_id,
            service_account_file=service_account_file,
        )
        stats = publisher.publish_incremental(data)

        logger.info("=" * 60)
        logger.info("✅ 마이그레이션 완료!")
        logger.info("업로드된 리뷰: %d개", stats["new_reviews"])
        logger.info("추가된 행: %d개", stats["appended_reviews"])
        logger.info("업데이트된 제품: %d개", stats["updated_products"])
        logger.info("=" * 60)
        logger.info(
            "스프레드시트 확인: https://docs.google.com/spreadsheets/d/%s/edit",
            spreadsheet_id,
        )
    except FileNotFoundError as e:
        logger.error("서비스 계정 파일을 찾을 수 없습니다: %s", e)
        logger.error("docs/BIODANCE_SHEETS_SETUP.md를 참고하여 서비스 계정을 생성하세요")
        sys.exit(1)
    except Exception as e:
        logger.error("마이그레이션 실패: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    migrate()
