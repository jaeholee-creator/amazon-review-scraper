"""
Google Sheets Uploader

CSV 리뷰 데이터를 Google Sheets에 업로드합니다.
인증: OAuth 토큰 방식
"""

import os
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import pytz


SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file'
]


class SheetsUploader:
    """Google Sheets에 리뷰 데이터 업로드."""

    def __init__(self, credentials_file: str = 'credentials.json'):
        """
        Args:
            credentials_file: Service Account JSON 파일 경로
        """
        self.credentials_file = credentials_file
        self.client = None
        self._initialize_client()

    def _initialize_client(self):
        """gspread 클라이언트 초기화 (Service Account 방식)."""
        if not os.path.exists(self.credentials_file):
            raise FileNotFoundError(
                f"Credentials file not found: {self.credentials_file}\n"
                f"Download Service Account JSON from:\n"
                f"https://console.cloud.google.com/iam-admin/serviceaccounts\n"
                f"Save it as '{self.credentials_file}'"
            )

        creds = Credentials.from_service_account_file(
            self.credentials_file,
            scopes=SCOPES
        )

        self.client = gspread.authorize(creds)
        print(f"✅ Google Sheets client initialized (Service Account)")

    def upload_reviews(
        self,
        spreadsheet_url: str,
        sheet_name: str,
        reviews: list[dict],
        append: bool = True
    ) -> dict:
        """
        리뷰 데이터를 Google Sheets에 업로드.

        Args:
            spreadsheet_url: 스프레드시트 URL
            sheet_name: 시트 이름 (예: "US_amazone")
            reviews: 리뷰 데이터 리스트
            append: True=기존 데이터에 추가, False=덮어쓰기

        Returns:
            {
                'success': bool,
                'rows_added': int,
                'total_rows': int,
                'sheet_url': str,
            }
        """
        if not reviews:
            return {
                'success': True,
                'rows_added': 0,
                'total_rows': 0,
                'message': 'No reviews to upload'
            }

        try:
            # 스프레드시트 열기
            spreadsheet = self.client.open_by_url(spreadsheet_url)
            print(f"   Opened: {spreadsheet.title}")

            # 시트 선택 (없으면 생성)
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                print(f"   Sheet found: {sheet_name}")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(
                    title=sheet_name,
                    rows=1000,
                    cols=20
                )
                print(f"   Sheet created: {sheet_name}")

            # 헤더 준비
            headers = [
                'ASIN',
                'Review ID',
                'Rating',
                'Title',
                'Author',
                'Date',
                'Location',
                'Verified Purchase',
                'Content',
                'Helpful Count',
                'Image Count',
                'Scraped At'
            ]

            # 기존 데이터 확인
            existing_data = worksheet.get_all_values()

            if not append or len(existing_data) == 0:
                # 덮어쓰기 또는 빈 시트
                worksheet.clear()
                worksheet.append_row(headers)
                start_row = 2
            else:
                # 추가 모드: 헤더 확인
                if existing_data[0] != headers:
                    print(f"   Warning: Headers mismatch, updating...")
                    worksheet.update('A1:L1', [headers])
                start_row = len(existing_data) + 1

            # 리뷰 데이터 변환
            rows = []
            for review in reviews:
                row = [
                    review.get('asin', ''),
                    review.get('review_id', ''),
                    review.get('rating', ''),
                    review.get('title', ''),
                    review.get('author', ''),
                    review.get('date', ''),
                    review.get('location', ''),
                    'Yes' if review.get('verified_purchase') else 'No',
                    review.get('content', ''),
                    review.get('helpful_count', 0),
                    review.get('image_count', 0),
                    review.get('scraped_at', datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d %H:%M:%S'))
                ]
                rows.append(row)

            # 배치 업데이트 (더 빠름)
            if rows:
                end_row = start_row + len(rows) - 1
                cell_range = f'A{start_row}:L{end_row}'
                worksheet.update(cell_range, rows, value_input_option='USER_ENTERED')

            total_rows = len(worksheet.get_all_values()) - 1  # 헤더 제외

            return {
                'success': True,
                'rows_added': len(rows),
                'total_rows': total_rows,
                'sheet_url': worksheet.url,
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'rows_added': 0,
            }

    def get_existing_review_ids(
        self,
        spreadsheet_url: str,
        sheet_name: str
    ) -> set[str]:
        """
        시트에 이미 존재하는 review_id 목록 조회.

        Returns:
            Set of review IDs
        """
        try:
            spreadsheet = self.client.open_by_url(spreadsheet_url)
            worksheet = spreadsheet.worksheet(sheet_name)

            # Review ID 컬럼 (B열) 가져오기
            review_ids = worksheet.col_values(2)[1:]  # 헤더 제외

            return set(review_ids)

        except Exception as e:
            print(f"   Warning: Could not fetch existing IDs: {e}")
            return set()


# 사용 예시
"""
from src.sheets_uploader import SheetsUploader

uploader = SheetsUploader(credentials_file='credentials.json')

reviews = [
    {
        'asin': 'B0B2RM68G2',
        'review_id': 'R123456',
        'rating': '5.0',
        'title': 'Great product!',
        'author': 'John Doe',
        'date': '2024-02-09',
        'location': 'United States',
        'verified_purchase': True,
        'content': 'Love it!',
        'helpful_count': 10,
        'image_count': 2,
    }
]

result = uploader.upload_reviews(
    spreadsheet_url='https://docs.google.com/spreadsheets/d/1NVUVShv5tAveINA9DdB2D21z71L3tF0In5JVK6LYX9s/edit',
    sheet_name='US_amazone',
    reviews=reviews,
    append=True
)

print(f"Success: {result['success']}")
print(f"Rows added: {result['rows_added']}")
"""
