# E-Commerce Review Scraper

Amazon, Biodance 등 여러 이커머스 사이트의 제품 리뷰를 수집하고 Google Sheets로 자동 업로드하는 시스템입니다.

## 📋 지원 사이트

### ✅ Amazon
- 제품 리뷰 수집
- Google Sheets 자동 업로드
- Slack 알림

### ✅ Biodance (US 자사몰)
- 제품 리뷰 수집
- Google Sheets 자동 업로드
- 증분 업데이트 (중복 제거)

## 📁 프로젝트 구조

```
amazon-review-scraper/
├── scrapers/
│   ├── amazon/              # Amazon 크롤러
│   │   ├── amazon_scraper.py
│   │   └── run_amazon.py
│   └── biodance/            # Biodance 크롤러
│       ├── biodance_review_crawler.py
│       ├── run_biodance_reviews.py
│       └── README.md
├── publishers/              # 공통 데이터 발행 모듈
│   └── google_sheets_publisher.py
├── config/
│   ├── settings.py
│   └── service-account.json  (Git 제외)
├── data/                    # 수집 데이터
│   ├── amazon/
│   └── biodance/
└── .github/workflows/       # GitHub Actions
    ├── amazon-reviews.yml
    └── biodance-reviews.yml
```

## 🚀 시작하기

### 1. 설치

```bash
git clone https://github.com/jaeholee-creator/amazon-review-scraper.git
cd amazon-review-scraper
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. 환경변수 설정

`.env` 파일 생성:

```bash
cp .env.example .env
# .env 파일 편집
```

### 3. Google Sheets 설정

서비스 계정 생성 및 JSON 키 다운로드:
- [설정 가이드](docs/GOOGLE_SHEETS_SETUP.md)

### 4. 실행

#### Biodance 리뷰 수집
```bash
python scrapers/biodance/run_biodance_reviews.py
```

#### Amazon 리뷰 수집
```bash
python api_daily_scraper.py
```

## 📊 자동화

GitHub Actions를 통해 매일 자동 실행됩니다.

- **Biodance**: 매일 UTC 0시 (KST 오전 9시)
- **Amazon**: 매일 UTC 1시 (KST 오전 10시)

## 🔧 새로운 사이트 추가 방법

1. `scrapers/` 아래에 새 디렉토리 생성
2. 크롤러 구현 (예: `scrapers/sephora/sephora_scraper.py`)
3. `publishers/google_sheets_publisher.py` 재사용
4. GitHub Actions 워크플로우 추가

## 📝 라이선스

MIT License

## 🤝 기여

Pull Request 환영합니다!
