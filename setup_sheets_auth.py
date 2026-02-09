"""
Google Sheets OAuth 인증 설정

최초 1회 실행하여 token.json 생성
"""

import os
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file'
]

CLIENT_SECRETS_FILE = 'client_secrets.json'
TOKEN_FILE = 'token.json'


def main():
    """OAuth 인증 플로우 실행."""

    if not os.path.exists(CLIENT_SECRETS_FILE):
        print(f"❌ {CLIENT_SECRETS_FILE} not found!\n")
        print("=" * 60)
        print("Google OAuth 클라이언트 ID 발급 방법:")
        print("=" * 60)
        print()
        print("1. Google Cloud Console 접속:")
        print("   https://console.cloud.google.com/")
        print()
        print("2. 프로젝트 선택 또는 생성")
        print()
        print("3. API 및 서비스 → 사용자 인증 정보")
        print("   https://console.cloud.google.com/apis/credentials")
        print()
        print("4. '사용자 인증 정보 만들기' → 'OAuth 클라이언트 ID'")
        print()
        print("5. 애플리케이션 유형: '데스크톱 앱'")
        print("   - 이름: amazon-review-scraper")
        print()
        print("6. 'JSON 다운로드' 클릭")
        print()
        print("7. 다운로드한 파일을 client_secrets.json으로 저장:")
        print(f"   mv ~/Downloads/client_secret_*.json {CLIENT_SECRETS_FILE}")
        print()
        print("8. 다시 실행: python3 setup_sheets_auth.py")
        print("=" * 60)
        return

    print("🔐 Starting OAuth authentication flow...\n")

    try:
        flow = InstalledAppFlow.from_client_secrets_file(
            CLIENT_SECRETS_FILE,
            SCOPES
        )

        # 로컬 서버로 인증 (브라우저 자동 열림)
        creds = flow.run_local_server(
            port=8080,
            prompt='consent',
            success_message='인증 성공! 이 창을 닫아도 됩니다.'
        )

        # 토큰 저장
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())

        print(f"\n✅ Authentication successful!")
        print(f"   Token saved: {TOKEN_FILE}")
        print(f"\n이제 스크래퍼를 실행할 수 있습니다:")
        print(f"   python3 api_daily_scraper.py --limit 1 --test")

    except Exception as e:
        print(f"\n❌ Authentication failed: {e}")
        print(f"\n문제 해결:")
        print(f"1. client_secrets.json 파일 확인")
        print(f"2. Google Cloud Console에서 OAuth 동의 화면 설정 확인")
        print(f"3. 승인된 리디렉션 URI에 http://localhost:8080/ 추가")


if __name__ == '__main__':
    main()
