# GitHub Actions 자동화 설정 가이드

## 📋 개요

- **Amazon 크롤러**: 매일 오전 09:00 KST (UTC 00:00)
- **Biodance 크롤러**: 매일 오전 09:00 KST (UTC 00:00)

---

## 🔐 GitHub Secrets 설정

GitHub Repository → Settings → Secrets and variables → Actions → New repository secret

### 1. Amazon 크롤러용

#### `AMAZON_COOKIES`
```bash
# 로컬 쿠키 파일 내용 복사
cat data/cookies.json | pbcopy
```
- Name: `AMAZON_COOKIES`
- Value: 복사한 JSON 전체 내용

#### `GOOGLE_SHEETS_CREDENTIALS`
```bash
# Service Account 키 파일 내용 복사
cat credentials.json | pbcopy
```
- Name: `GOOGLE_SHEETS_CREDENTIALS`
- Value: 복사한 JSON 전체 내용

---

### 2. Biodance 크롤러용

#### `GOOGLE_SHEETS_SERVICE_ACCOUNT`
```bash
# 서비스 계정 키 파일 내용 복사
cat config/service-account.json | pbcopy
```
- Name: `GOOGLE_SHEETS_SERVICE_ACCOUNT`
- Value: 복사한 JSON 전체 내용

---

### 3. 공통

#### `GOOGLE_SHEETS_SPREADSHEET_ID`
```
1NVUVShv5tAveINA9DdB2D21z71L3tF0In5JVK6LYX9s
```
- Name: `GOOGLE_SHEETS_SPREADSHEET_ID`
- Value: `1NVUVShv5tAveINA9DdB2D21z71L3tF0In5JVK6LYX9s`

---

## 📝 설정 단계

### Step 1: GitHub Secrets 추가

1. https://github.com/jaeholee-creator/amazon-review-scraper/settings/secrets/actions
2. "New repository secret" 클릭
3. 위 4개 Secrets 추가:
   - ✅ `AMAZON_COOKIES`
   - ✅ `GOOGLE_SHEETS_CREDENTIALS`
   - ✅ `GOOGLE_SHEETS_SERVICE_ACCOUNT`
   - ✅ `GOOGLE_SHEETS_SPREADSHEET_ID`

### Step 2: 워크플로우 활성화

워크플로우 파일이 main 브랜치에 푸시되면 자동으로 활성화됩니다:
- `.github/workflows/amazon-reviews.yml`
- `.github/workflows/biodance-reviews.yml`

### Step 3: 수동 테스트 실행

1. https://github.com/jaeholee-creator/amazon-review-scraper/actions
2. "Amazon Reviews Collection" 또는 "Biodance Reviews Collection" 선택
3. "Run workflow" 버튼 클릭
4. 결과 확인

---

## ⚠️ 중요 참고사항

### Amazon 쿠키 만료 문제

**문제**: GitHub Actions에서는 쿠키가 만료되면 자동 갱신 불가

**해결 방법**:

1. **로컬에서 수동 로그인** (7-14일마다)
   ```bash
   cd /Users/jaeho/amazon-review-scraper
   python3 manual_login.py
   # 로그인 완료 후
   touch data/login_done.signal
   ```

2. **GitHub Secret 업데이트**
   ```bash
   cat data/cookies.json | pbcopy
   ```
   → GitHub Secrets의 `AMAZON_COOKIES` 값 업데이트

3. **자동화 대안** (선택사항)
   - AWS S3 / Google Cloud Storage에 쿠키 저장
   - GitHub Actions에서 동적으로 읽기/쓰기
   - 별도 구현 필요

---

## 📊 실행 모니터링

### GitHub Actions 페이지
https://github.com/jaeholee-creator/amazon-review-scraper/actions

### 실행 로그 확인
1. Actions 탭에서 워크플로우 클릭
2. 최근 실행 결과 확인
3. 로그 다운로드 가능

### 실패 시 디버깅
- 실패 시 `data/debug_*.png` 스크린샷 자동 업로드
- Artifacts에서 다운로드 가능 (7일 보관)

---

## 🔄 워크플로우 비활성화

자동 실행을 중지하려면:

1. https://github.com/jaeholee-creator/amazon-review-scraper/actions
2. 워크플로우 선택
3. "..." 메뉴 → "Disable workflow"

---

## 📅 스케줄

```yaml
schedule:
  - cron: '0 0 * * *'  # 매일 UTC 00:00 = KST 09:00
```

**테스트 스케줄 변경** (예: 매시간 실행):
```yaml
schedule:
  - cron: '0 * * * *'  # 매시간
```

---

## ✅ 설정 완료 체크리스트

- [ ] GitHub Secrets 4개 추가
- [ ] 워크플로우 파일 푸시
- [ ] 수동 실행 테스트 성공
- [ ] 쿠키 만료 대응 방법 확인
- [ ] Google Sheets에 데이터 확인

---

**완료 후**: 매일 오전 9시에 자동으로 리뷰가 수집되어 Google Sheets에 업로드됩니다! 🎉
