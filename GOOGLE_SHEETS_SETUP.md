

# Google Sheets 연동 설정 가이드

CSV 대신 Google Sheets에 직접 업로드하여 실시간 데이터 확인

---

## 1단계: Google Cloud 프로젝트 생성

1. **Google Cloud Console** 접속
   - https://console.cloud.google.com/

2. **새 프로젝트 생성**
   - 프로젝트 이름: `amazon-review-scraper` (또는 원하는 이름)
   - "만들기" 클릭

3. **프로젝트 선택**
   - 상단 드롭다운에서 생성한 프로젝트 선택

---

## 2단계: Google Sheets API 활성화

1. **API 및 서비스 → 라이브러리**
   - https://console.cloud.google.com/apis/library

2. **"Google Sheets API" 검색**
   - 클릭 → "사용" 버튼

3. **"Google Drive API"도 활성화**
   - 검색 → 클릭 → "사용" 버튼

---

## 3단계: Service Account 생성

1. **API 및 서비스 → 사용자 인증 정보**
   - https://console.cloud.google.com/apis/credentials

2. **"사용자 인증 정보 만들기" → "서비스 계정"**
   - 서비스 계정 이름: `sheets-uploader`
   - 설명: `Amazon review data uploader`
   - "만들기 및 계속하기"

3. **역할 선택** (선택사항, 스킵 가능)
   - "계속" 클릭

4. **완료**

---

## 4단계: JSON 키 다운로드

1. **생성된 서비스 계정 클릭**
   - 예: `sheets-uploader@amazon-review-scraper.iam.gserviceaccount.com`

2. **"키" 탭 → "키 추가" → "새 키 만들기"**
   - 키 유형: **JSON**
   - "만들기" 클릭

3. **JSON 파일 다운로드**
   - 파일명: `amazon-review-scraper-xxxxx.json`

4. **파일을 프로젝트 루트에 복사**
   ```bash
   cp ~/Downloads/amazon-review-scraper-xxxxx.json /Users/jaeho/amazon-review-scraper/credentials.json
   ```

---

## 5단계: 스프레드시트 권한 부여

1. **Google Sheets 열기**
   - https://docs.google.com/spreadsheets/d/1NVUVShv5tAveINA9DdB2D21z71L3tF0In5JVK6LYX9s/edit

2. **"공유" 버튼 클릭**

3. **Service Account 이메일 추가**
   - `credentials.json` 파일에서 `client_email` 복사
   - 예: `sheets-uploader@amazon-review-scraper.iam.gserviceaccount.com`
   - 역할: **편집자**
   - "전송" 클릭

---

## 6단계: 테스트

```bash
python3 -c "from src.sheets_uploader import SheetsUploader; uploader = SheetsUploader(); print('✅ Credentials OK')"
```

**출력**:
```
✅ Google Sheets client initialized
✅ Credentials OK
```

---

## 7단계: 스크래퍼 실행

```bash
python3 api_daily_scraper.py --limit 1 --test
```

**출력 예시**:
```
[Step 5] Uploading to Google Sheets...
   Opened: BIODANCE Amazon Review Dashboard
   Sheet found: US_amazone
   ✅ Sheets: 3 rows added
   Total rows: 156
```

---

## 파일 구조

```
amazon-review-scraper/
├── credentials.json          # Service Account 키 (gitignore)
├── src/
│   └── sheets_uploader.py   # Sheets 업로드 모듈
└── api_daily_scraper.py      # 메인 스크립트
```

---

## credentials.json 형식

```json
{
  "type": "service_account",
  "project_id": "amazon-review-scraper",
  "private_key_id": "xxxxx",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
  "client_email": "sheets-uploader@amazon-review-scraper.iam.gserviceaccount.com",
  "client_id": "xxxxx",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  ...
}
```

**중요**: 이 파일을 GitHub에 커밋하지 마세요! (`.gitignore`에 추가됨)

---

## 스프레드시트 시트 구조

### `US_amazone` 시트

| 컬럼 | 설명 |
|------|------|
| ASIN | 제품 ID |
| Review ID | 리뷰 고유 ID |
| Rating | 평점 (1-5) |
| Title | 리뷰 제목 |
| Author | 작성자 |
| Date | 작성 날짜 |
| Location | 작성 국가 |
| Verified Purchase | 확인된 구매 (Yes/No) |
| Content | 리뷰 본문 |
| Helpful Count | 도움됨 투표 수 |
| Image Count | 첨부 이미지 수 |
| Scraped At | 수집 시간 (KST) |

---

## 작동 방식

1. **리뷰 수집** (CSV 방식과 동일)
   - Playwright → CSRF 캡처 → API/HTML 수집

2. **CSV 저장** (로컬 백업)
   - `data/daily/YYYY-MM-DD/all_reviews_api.csv`

3. **Google Sheets 업로드**
   - 기존 데이터에 추가 (append 모드)
   - 중복 검사 없음 (scraper_state.json으로 이미 처리됨)

4. **실시간 확인**
   - 스프레드시트에서 즉시 데이터 확인 가능
   - 팀원과 실시간 공유

---

## 문제 해결

### "FileNotFoundError: credentials.json"

**원인**: Service Account 키 파일이 없음

**해결**:
```bash
# 다운로드한 JSON 파일을 credentials.json으로 복사
cp ~/Downloads/your-project-xxxxx.json credentials.json
```

---

### "gspread.exceptions.APIError: PERMISSION_DENIED"

**원인**: 스프레드시트에 Service Account 권한 없음

**해결**:
1. 스프레드시트 → "공유"
2. Service Account 이메일 추가
3. 역할: 편집자

---

### "gspread.exceptions.WorksheetNotFound: US_amazone"

**원인**: 시트 이름이 정확히 일치하지 않음

**해결**:
- 스프레드시트에서 시트 이름 확인
- 대소문자, 띄어쓰기 정확히 일치 필요
- 또는 코드에서 `sheet_name='US_amazone'` 수정

---

### API 할당량 초과

**증상**: "Quota exceeded for quota metric 'Read requests'"

**원인**: Google Sheets API 무료 할당량 초과

**할당량**:
- 읽기: 300/분/프로젝트
- 쓰기: 300/분/프로젝트

**해결**:
- 배치 업데이트 사용 (이미 구현됨)
- 대량 데이터는 CSV 사용 권장

---

## 비용

**Google Sheets API**: 무료
- 읽기/쓰기: 무제한 (할당량 내)
- 스프레드시트 크기: 5백만 셀 (무료 계정)

---

## 보안

### credentials.json 보호

```bash
# .gitignore에 추가됨
echo "credentials.json" >> .gitignore
```

### Service Account 권한 최소화

- 스프레드시트 편집만 허용
- 전체 Drive 접근 불필요

---

## 다음 단계

1. **credentials.json 다운로드**
2. **스프레드시트 권한 부여**
3. **테스트 실행**

```bash
python3 api_daily_scraper.py --limit 1 --test
```

4. **스프레드시트에서 데이터 확인**
   - https://docs.google.com/spreadsheets/d/1NVUVShv5tAveINA9DdB2D21z71L3tF0In5JVK6LYX9s/edit

---

**완료!** 이제 리뷰 데이터가 자동으로 Google Sheets에 업로드됩니다. 🎉
