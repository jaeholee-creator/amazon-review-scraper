"""
Shopee Reviews를 Google Sheets에 발행하는 Publisher
"""
import logging
import os
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)


class ShopeeGoogleSheetsPublisher:
    """Shopee 리뷰를 Google Sheets에 발행 (서비스 계정 인증)"""

    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    def __init__(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        service_account_file: str = "credentials.json",
    ):
        """
        Args:
            spreadsheet_id: 스프레드시트 ID
            sheet_name: 시트 이름 (예: 'SG_shopee', 'PH_shopee')
            service_account_file: 서비스 계정 JSON 키 파일 경로
        """
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.service_account_file = service_account_file
        self.client = self._authenticate()
        self.spreadsheet = self.client.open_by_key(spreadsheet_id)

    def _authenticate(self) -> gspread.Client:
        """서비스 계정 인증"""
        if not os.path.exists(self.service_account_file):
            raise FileNotFoundError(
                f"서비스 계정 JSON 파일이 없습니다: {self.service_account_file}"
            )

        try:
            credentials = Credentials.from_service_account_file(
                self.service_account_file, scopes=self.SCOPES
            )
            logger.info(f"서비스 계정 인증 완료: {self.service_account_file}")
            return gspread.authorize(credentials)
        except Exception as e:
            logger.error(f"서비스 계정 인증 실패: {e}")
            raise

    def _get_or_create_sheet(self) -> gspread.Worksheet:
        """시트 가져오기 또는 생성"""
        try:
            return self.spreadsheet.worksheet(self.sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            logger.info(f"시트 '{self.sheet_name}'를 찾을 수 없어 새로 생성합니다.")
            return self.spreadsheet.add_worksheet(
                title=self.sheet_name, rows=1000, cols=20
            )

    def _read_existing_review_ids(self) -> set[str]:
        """기존 review_id 집합 읽기"""
        sheet = self._get_or_create_sheet()

        # 헤더 행 읽기
        try:
            headers = sheet.row_values(1)
        except Exception:
            return set()

        if not headers or 'review_id' not in headers:
            return set()

        # review_id 컬럼 인덱스 찾기
        review_id_col = headers.index('review_id') + 1  # 1-based

        try:
            review_ids_column = sheet.col_values(review_id_col)[1:]  # 헤더 제외
            logger.info(f"[{self.sheet_name}] 기존 리뷰 ID {len(review_ids_column)}개 로드")
            return set(review_ids_column)
        except Exception as e:
            logger.warning(f"review_id 컬럼 읽기 실패: {e}")
            return set()

    def _format_review_row(self, review: dict, headers: list[str]) -> list[Any]:
        """리뷰 dict → Sheets 행 변환"""
        field_mapping = {
            "review_id": review.get("review_id", ""),
            "collected_at": review.get("collected_at", ""),
            "product_name": review.get("product_name", ""),
            "product_id": review.get("product_id", ""),
            "author": review.get("author", ""),
            "author_country": review.get("author_country", ""),
            "star": review.get("star", 0),
            "title": review.get("title", ""),
            "content": review.get("content", ""),
            "date": review.get("date", ""),
            "verified_purchase": review.get("verified_purchase", False),
            "item_type": review.get("item_type", ""),
            "reply_content": review.get("reply_content", ""),
            "image_urls": review.get("image_urls", ""),
            "video_urls": review.get("video_urls", ""),
            "likes_count": review.get("likes_count", 0),
            "detailed_rating_product": review.get("detailed_rating_product", 0),
            "detailed_rating_seller": review.get("detailed_rating_seller", 0),
            "detailed_rating_delivery": review.get("detailed_rating_delivery", 0),
        }

        row = []
        for header in headers:
            value = field_mapping.get(header, "")
            if value is None:
                value = ""
            row.append(value)
        return row

    def _ensure_headers(self, sheet: gspread.Worksheet) -> list[str]:
        """헤더 확인 및 생성"""
        headers = sheet.row_values(1)

        if not headers:
            # 헤더 생성
            headers = [
                "review_id", "collected_at", "product_name", "product_id",
                "author", "author_country", "star", "title", "content", "date",
                "verified_purchase", "item_type", "reply_content",
                "image_urls", "video_urls", "likes_count",
                "detailed_rating_product", "detailed_rating_seller", "detailed_rating_delivery"
            ]
            sheet.update('A1', [headers])
            logger.info(f"[{self.sheet_name}] 헤더 생성 완료: {len(headers)}개 컬럼")

        return headers

    def append_reviews(self, reviews: list[dict]) -> int:
        """신규 리뷰를 시트에 batch append"""
        if not reviews:
            logger.info(f"[{self.sheet_name}] 추가할 신규 리뷰가 없습니다")
            return 0

        sheet = self._get_or_create_sheet()
        headers = self._ensure_headers(sheet)

        rows = [self._format_review_row(r, headers) for r in reviews]

        # Batch append
        batch_size = 1000
        total_appended = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            try:
                sheet.append_rows(batch, value_input_option="USER_ENTERED")
                total_appended += len(batch)
                logger.info(
                    f"[{self.sheet_name}] 배치 추가: {i + 1}-{min(i + batch_size, len(rows))}/{len(rows)}"
                )
            except Exception as e:
                logger.error(f"[{self.sheet_name}] 배치 추가 실패: {e}")
                raise

        return total_appended

    def publish_incremental(self, result: dict) -> dict:
        """증분 업데이트 메인 메서드"""
        # 1. 기존 review_id 읽기
        existing_ids = self._read_existing_review_ids()

        # 2. 신규 리뷰 필터링
        all_reviews = result.get('reviews', [])
        new_reviews = [r for r in all_reviews if r.get('review_id') not in existing_ids]

        logger.info(
            f"[{self.sheet_name}] 총 리뷰: {len(all_reviews)}개 / "
            f"기존: {len(existing_ids)}개 / 신규: {len(new_reviews)}개"
        )

        # 3. 신규 리뷰 append
        appended_count = 0
        if new_reviews:
            appended_count = self.append_reviews(new_reviews)
            logger.info(f"[{self.sheet_name}] 업데이트 완료: {appended_count}개 신규 리뷰 추가")
        else:
            logger.info(f"[{self.sheet_name}] 신규 리뷰가 없어 업데이트를 건너뜁니다")

        return {
            "sheet_name": self.sheet_name,
            "new_reviews": len(new_reviews),
            "appended_reviews": appended_count,
            "total_reviews": len(all_reviews),
            "country": result.get('country', ''),
        }
