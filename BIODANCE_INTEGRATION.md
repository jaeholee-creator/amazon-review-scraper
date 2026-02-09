# Biodance 크롤러 통합 완료

## 🎉 완료 사항

### 1. 디렉토리 구조 재구성

```
amazon-review-scraper/
├── scrapers/
│   ├── amazon/              # Amazon 크롤러 (기존)
│   └── biodance/            # Biodance 크롤러 (신규)
│       ├── biodance_review_crawler.py
│       ├── run_biodance_reviews.py
│       └── README.md
├── publishers/              # 공통 모듈 (신규)
│   └── google_sheets_publisher.py
├── scripts/
│   └── migrate_biodance_to_sheets.py
└── .github/workflows/
    └── biodance-reviews.yml
```

### 2. 추가된 기능

- ✅ Biodance US 자사몰 리뷰 크롤링
- ✅ Google Sheets 자동 업로드 (서비스 계정 인증)
- ✅ 증분 업데이트 (중복 제거)
- ✅ GitHub Actions 자동화
- ✅ 공통 모듈 분리 (확장 가능)

### 3. 환경 설정

`.env` 파일에 추가:

```bash
# Biodance 설정
GOOGLE_SHEETS_SPREADSHEET_ID=1NVUVShv5tAveINA9DdB2D21z71L3tF0In5JVK6LYX9s
```

`config/service-account.json` 복사:
- `/Users/jaeho/Desktop/ai-trend-collector/config/service-account.json` → `config/service-account.json`

### 4. 실행 방법

```bash
# 로컬 실행
python scrapers/biodance/run_biodance_reviews.py

# GitHub Actions (자동)
매일 UTC 0시 자동 실행
```

## 📊 데이터 흐름

```
Biodance API → 크롤러 → 로컬 JSON (선택) → Google Sheets
                           ↓
                    중복 제거 (review_id 기준)
```

## 🔧 향후 확장

새로운 사이트 추가 시:

1. `scrapers/새사이트/` 디렉토리 생성
2. 크롤러 구현
3. `publishers/google_sheets_publisher.py` 재사용
4. GitHub Actions 워크플로우 추가

## 📝 참고 문서

- [Biodance README](scrapers/biodance/README.md)
- [Google Sheets 설정](GOOGLE_SHEETS_SETUP.md)
- [전체 README](README.md)

---

**구현 완료**: 2026-02-09
**통합 리포**: https://github.com/jaeholee-creator/amazon-review-scraper
