"""
Google Sheets API를 사용하여 Biodance 리뷰 발행

서비스 계정(Service Account) 인증 방식:
- GCP에서 서비스 계정 생성 및 JSON 키 다운로드
- 서비스 계정 이메일에 스프레드시트 편집 권한 부여
- 브라우저 로그인 없이 자동 인증
"""

import logging
import os
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)


class GoogleSheetsPublisher:
    """Google Sheets API를 사용하여 Biodance 리뷰 발행 (서비스 계정 인증)"""

    SHEET_NAME = "US자사몰"  # 기존 시트 이름
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    def __init__(
        self,
        spreadsheet_id: str,
        service_account_file: str = "config/service-account.json",
    ):
        """
        Args:
            spreadsheet_id: 스프레드시트 ID
            service_account_file: 서비스 계정 JSON 키 파일 경로
        """
        self.spreadsheet_id = spreadsheet_id
        self.service_account_file = service_account_file
        self.client = self._authenticate()
        self.spreadsheet = self.client.open_by_key(spreadsheet_id)

    def _authenticate(self) -> gspread.Client:
        """서비스 계정 인증 (브라우저 로그인 불필요)"""
        if not os.path.exists(self.service_account_file):
            raise FileNotFoundError(
                f"서비스 계정 JSON 파일이 없습니다: {self.service_account_file}\n"
                "GCP Console에서 서비스 계정을 생성하고 JSON 키를 다운로드하세요.\n"
                "그리고 서비스 계정 이메일에 스프레드시트 편집 권한을 부여하세요."
            )

        try:
            credentials = Credentials.from_service_account_file(
                self.service_account_file, scopes=self.SCOPES
            )
            logger.info("서비스 계정 인증 완료: %s", self.service_account_file)
            return gspread.authorize(credentials)
        except Exception as e:
            logger.error("서비스 계정 인증 실패: %s", e)
            raise

    def _get_us_sheet(self) -> gspread.Worksheet:
        """US자사몰 시트 가져오기"""
        try:
            return self.spreadsheet.worksheet(self.SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            logger.error("시트를 찾을 수 없습니다: %s", self.SHEET_NAME)
            raise

    def _read_existing_review_ids(self) -> set[str]:
        """Sheets에서 기존 review_id 집합 읽기 (review_id 컬럼 찾기)"""
        sheet = self._get_us_sheet()

        # 헤더 행 읽기
        headers = sheet.row_values(1)
        if not headers:
            logger.warning("헤더 행이 비어있습니다 - 전체 업로드 모드")
            return set()

        # review_id 컬럼 인덱스 찾기
        try:
            review_id_col = headers.index("review_id") + 1  # gspread는 1-based
        except ValueError:
            logger.warning("review_id 컬럼이 없습니다 - 전체 업로드 모드")
            return set()

        # review_id 컬럼만 읽기 (헤더 제외)
        try:
            review_ids_column = sheet.col_values(review_id_col)[1:]
            logger.info("기존 리뷰 ID %d개 로드 완료", len(review_ids_column))
            return set(review_ids_column)
        except Exception as e:
            logger.warning("review_id 컬럼 읽기 실패: %s", e)
            return set()

    def _format_review_row(self, review: dict, collected_at: str, headers: list[str]) -> list[Any]:
        """리뷰 dict → Sheets 행 변환 (헤더 순서에 맞게)"""
        # 헤더 순서에 맞춰 값 매핑
        field_mapping = {
            "review_id": review.get("review_id", ""),
            "collected_at": collected_at,
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
            "image_urls": ";".join(review.get("image_urls", [])) if isinstance(review.get("image_urls"), list) else review.get("image_urls", ""),
            "video_urls": ";".join(review.get("video_urls", [])) if isinstance(review.get("video_urls"), list) else review.get("video_urls", ""),
            "likes_count": review.get("likes_count", 0),
        }

        row = []
        for header in headers:
            value = field_mapping.get(header, "")
            # gspread는 None을 빈 문자열로 변환
            if value is None:
                value = ""
            row.append(value)
        return row

    def append_reviews(self, reviews: list[dict], collected_at: str) -> int:
        """신규 리뷰를 US자사몰 시트에 batch append"""
        if not reviews:
            logger.info("추가할 신규 리뷰가 없습니다")
            return 0

        sheet = self._get_us_sheet()
        headers = sheet.row_values(1)  # 기존 헤더 읽기

        if not headers:
            logger.warning("헤더 행이 비어있습니다 - 헤더 자동 생성")
            # 기본 헤더 생성
            headers = [
                "review_id", "collected_at", "product_name", "product_id",
                "author", "author_country", "star", "title", "content", "date",
                "verified_purchase", "item_type", "reply_content",
                "image_urls", "video_urls", "likes_count"
            ]
            sheet.update("A1:P1", [headers])
            logger.info("헤더 행 생성 완료: %d개 컬럼", len(headers))

        rows = [self._format_review_row(r, collected_at, headers) for r in reviews]

        # Batch append (최대 1000행씩 분할)
        batch_size = 1000
        total_appended = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            try:
                sheet.append_rows(batch, value_input_option="USER_ENTERED")
                total_appended += len(batch)
                logger.info(
                    "배치 추가 완료: %d-%d/%d",
                    i + 1,
                    min(i + batch_size, len(rows)),
                    len(rows),
                )
            except Exception as e:
                logger.error("배치 추가 실패: %s", e)
                raise

        return total_appended

    def publish_incremental(self, results: dict) -> dict:
        """증분 업데이트 메인 메서드"""
        # 1. 기존 review_id 읽기
        existing_ids = self._read_existing_review_ids()

        # 2. 모든 리뷰 추출
        all_reviews = []
        for product in results.get("products", []):
            all_reviews.extend(product.get("reviews", []))

        # 3. 신규 리뷰 필터링
        new_reviews = [r for r in all_reviews if r.get("review_id") not in existing_ids]

        logger.info(
            "총 리뷰: %d개 / 기존: %d개 / 신규: %d개",
            len(all_reviews),
            len(existing_ids),
            len(new_reviews),
        )

        # 4. 신규 리뷰 append
        appended_count = 0
        if new_reviews:
            appended_count = self.append_reviews(new_reviews, results.get("collected_at", ""))
            logger.info("Google Sheets 업데이트 완료: %d개 신규 리뷰 추가", appended_count)
        else:
            logger.info("신규 리뷰가 없어 Sheets 업데이트를 건너뜁니다")

        return {
            "new_reviews": len(new_reviews),
            "appended_reviews": appended_count,
            "total_reviews": len(all_reviews),
            "updated_products": len(results.get("products", [])),
        }
