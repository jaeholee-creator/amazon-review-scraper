# Amazon Review Scraper

Amazon 제품 리뷰를 수집하는 Playwright + BeautifulSoup 기반 스크래퍼입니다.

## 🎯 대상 제품

**BIODANCE Bio-Collagen Real Deep Mask** (ASIN: B0B879FZBZ)
- 현재 리뷰 수: 34,513개
- 평점: 4.5/5.0

## 📋 기능

- ✅ Amazon 자동 로그인 및 세션 관리 (Firefox)
- ✅ **CAPTCHA 자동 해결** (CapSolver API 통합)
- ✅ 날짜 기반 필터링 (최근 2일 윈도우)
- ✅ API 모드 + HTML 파싱 하이브리드
- ✅ 실시간 CSV 저장 (점진적)
- ✅ 중복 제거 (review_id 트래킹)
- ✅ Rate Limiting (IP 차단 방지)
- ✅ Slack 알림 (일일 리포트)

## 📊 수집 데이터 필드

| 필드 | 설명 |
|------|------|
| `review_id` | 고유 리뷰 ID |
| `rating` | 평점 (1-5) |
| `title` | 리뷰 제목 |
| `author` | 작성자 |
| `date` | 작성일 |
| `location` | 작성 국가 |
| `verified_purchase` | 확인된 구매 여부 |
| `content` | 리뷰 본문 |
| `helpful_count` | 도움이 됨 투표 수 |
| `image_count` | 첨부 이미지 수 |
| `scraped_at` | 수집 시간 |

## 📦 설치

```bash
# 1. 레포지토리 클론
git clone https://github.com/jaeholee-creator/amazon-review-scraper.git
cd amazon-review-scraper

# 2. 가상환경 생성
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. 패키지 설치
pip install -r requirements.txt

# 4. Playwright 브라우저 설치
playwright install chromium
```

## 🚀 실행

### 일일 스크래퍼 (권장)

```bash
# 전체 제품 실행
python3 api_daily_scraper.py

# 테스트 모드 (10페이지만)
python3 api_daily_scraper.py --test

# 3개 제품만 테스트
python3 api_daily_scraper.py --limit 3 --test
```

### 첫 실행 시 (쿠키 없음)

**옵션 1: 수동 로그인 (무료)**
```bash
python3 manual_login.py
# 브라우저 창에서 로그인 → CAPTCHA 풀기 → 완료
# 쿠키 저장 후 자동 스크래퍼 실행 가능
```

**옵션 2: CAPTCHA 자동 해결 (유료) - Claude API** ⭐ 권장
```bash
# 1. Claude API 키 설정 (.env 파일)
ANTHROPIC_API_KEY=sk-ant-api03-your-key

# 2. 설정 테스트
python3 test_claude_solver.py

# 3. 자동 실행 (CAPTCHA 발생 시 Claude가 자동 해결)
python3 api_daily_scraper.py
```

**장점**:
- 🎯 모든 CAPTCHA 타입 지원 (FunCaptcha, 이미지 선택, 텍스트)
- 🧠 Claude 비전 AI의 높은 정확도 (85-95%)
- 💰 저렴한 비용 (~$0.02/월, 쿠키 재사용 시)
- ⚡ 간단한 설정 (API 키만)

자세한 설정: [CLAUDE_CAPTCHA.md](CLAUDE_CAPTCHA.md) 참고

**옵션 3: 다른 CAPTCHA 서비스**
- CapSolver (FunCaptcha 전용): [CAPTCHA_SETUP.md](CAPTCHA_SETUP.md)
- 2Captcha (이미지 CAPTCHA): [CAPTCHA_SETUP.md](CAPTCHA_SETUP.md)

### 구버전 스크래퍼

```bash
# 단일 제품 전체 수집
python main.py --test

# HTML 파싱 모드 (구버전)
python batch_daily_scraper.py --test
```

## 📁 출력 파일

```
data/
├── reviews.csv          # 수집된 리뷰 데이터
├── cookies.json         # 로그인 세션 (자동 생성)
└── checkpoint.json      # 진행 상황 (중단 시 재개용)
```

## ⚙️ 설정 변경

`config/settings.py`에서 수정 가능:

```python
# 제품 ASIN 변경
ASIN = 'B0B879FZBZ'

# 날짜 필터 변경 (기본: 30일)
DAYS_TO_SCRAPE = 30

# Rate Limiting 조정
MIN_DELAY = 4.0  # 최소 대기 시간 (초)
MAX_DELAY = 7.0  # 최대 대기 시간 (초)
```

## ⚠️ 주의사항

1. **메인 Amazon 계정 사용 주의**: 테스트 계정 사용 권장
2. **Rate Limiting 준수**: 기본 설정 변경 시 차단 위험 증가
3. **개인 사용 목적**: 상업적 사용 시 법적 검토 필요

## 🐛 문제 해결

### CAPTCHA 발생 시
- 30분 대기 후 재시도
- Rate Limiting 값 증가 고려

### IP 차단 시
- VPN 또는 Proxy 사용 고려
- 12-24시간 대기 후 재시도

### 로그인 실패 시
- `data/cookies.json` 삭제 후 재실행
- 수동 로그인 재시도

## 📝 라이선스

MIT License
