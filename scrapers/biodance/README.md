# Biodance US 자사몰 리뷰 크롤러

Biodance 미국 자사몰 (biodance.com)의 제품 리뷰를 수집합니다.

## 실행 방법

```bash
# 프로젝트 루트에서 실행
python scrapers/biodance/run_biodance_reviews.py
```

## 데이터 저장

- **로컬**: `data/biodance/*.json`, `*.csv`
- **Google Sheets**: 자동 업로드 (증분 업데이트)

## 중복 제거

- `review_id` 기준 자동 중복 제거
- 신규 리뷰만 수집 및 업로드
