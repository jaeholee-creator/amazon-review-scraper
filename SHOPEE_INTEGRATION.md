# Shopee Reviews Scraper

Shopee Singapore 및 Philippines 리뷰를 자동 수집하여 Google Sheets에 업로드하는 스크래퍼입니다.

## 📋 개요

- **플랫폼**: Shopee (Singapore, Philippines)
- **수집 방식**: Shopee 비공식 API 기반 (브라우저 불필요)
- **수집 주기**: 매일 자동 실행 (GitHub Actions)
- **날짜 범위**: 최근 3일 (today ~ today-3)
- **출력**: Google Sheets (`SG_shopee`, `PH_shopee` 시트)

---

## 🏗️ 아키텍처

```
shopee_daily_scraper.py
├── scrapers/shopee/
│   └── shopee_scraper.py       # Shopee API 크롤러
├── publishers/
│   └── shopee_sheets_publisher.py  # Google Sheets 업로드
└── config/settings.py          # 설정 (Shop IDs, 날짜 범위)
```

---

## 🚀 사용 방법

### 1. 로컬 실행

```bash
# 가상환경 활성화
source .venv/bin/activate

# 스크래퍼 실행
python shopee_daily_scraper.py
```

### 2. 자동 스케줄링 (GitHub Actions)

워크플로우가 이미 설정되어 있습니다:
- **파일**: `.github/workflows/shopee-reviews.yml`
- **스케줄**: 매일 KST 오전 10시 (UTC 1시)
- **수동 실행**: GitHub Actions 탭에서 "Run workflow" 클릭

---

## ⚙️ 설정

### Shop 정보 (`config/settings.py`)

```python
SHOPEE_SHOPS = {
    'sg': {
        'userid': '951704668',
        'shopid': '951591050',
        'sheet_name': 'SG_shopee',
    },
    'ph': {
        'userid': '952208252',
        'shopid': '952094055',
        'sheet_name': 'PH_shopee',
    }
}
```

### 날짜 범위 설정

```python
COLLECTION_WINDOW_DAYS = 3  # 최근 3일간 리뷰 수집
```

- `start_date`: `datetime.now() - timedelta(days=3)`
- `end_date`: `datetime.now()`

---

## 📊 Google Sheets 구조

### 시트 이름
- **SG_shopee**: Singapore 리뷰
- **PH_shopee**: Philippines 리뷰

### 컬럼 (19개)

| 컬럼명 | 설명 | 타입 |
|--------|------|------|
| review_id | 리뷰 고유 ID | String |
| collected_at | 수집 일시 | DateTime |
| product_name | 제품명 | String |
| product_id | 제품 ID | String |
| author | 작성자 | String |
| author_country | 국가 (SG/PH) | String |
| star | 평점 (1-5) | Integer |
| title | 제목 (Shopee는 없음) | String |
| content | 리뷰 내용 | Text |
| date | 작성일 | Date |
| verified_purchase | 구매 확정 여부 | Boolean |
| item_type | 제품 옵션/변형 | String |
| reply_content | 셀러 답변 | Text |
| image_urls | 이미지 URL (세미콜론 구분) | String |
| video_urls | 비디오 URL (세미콜론 구분) | String |
| likes_count | 좋아요 수 | Integer |
| detailed_rating_product | 제품 품질 평점 | Integer |
| detailed_rating_seller | 셀러 서비스 평점 | Integer |
| detailed_rating_delivery | 배송 서비스 평점 | Integer |

---

## 🔧 기술 스택

- **Python 3.12**
- **requests**: HTTP 요청 (API 호출)
- **gspread**: Google Sheets API
- **pytz**: 타임존 처리

---

## 🌐 API 엔드포인트

### Shopee 비공식 API

**URL Pattern**:
```
https://shopee.{country}/api/v4/seller_operation/get_shop_ratings_new
```

**Parameters**:
- `userid`: User ID
- `shopid`: Shop ID
- `limit`: 한 번에 가져올 개수 (기본 50)
- `offset`: 페이지네이션 오프셋
- `replied`: 'undefined'

**Response**:
```json
{
  "error": 0,
  "error_msg": "success",
  "data": {
    "items": [
      {
        "cmtid": 88220503186,
        "ctime": 1770690493,
        "comment": "Super fast...",
        "rating_star": 5,
        "author_username": "g*****y",
        "product_items": [{...}],
        "detailed_rating": {
          "product_quality": 5,
          "seller_service": 5,
          "delivery_service": 5
        }
      }
    ]
  }
}
```

---

## 📈 성능

### 수집 속도
- **Singapore**: ~10초 (427개 리뷰, 9 페이지)
- **Philippines**: ~1.5초 (51개 리뷰, 2 페이지)
- **총 소요 시간**: ~15-20초

### Rate Limiting
- 페이지당 1초 대기
- 안정적인 수집 속도 유지

---

## 🛠️ 트러블슈팅

### 1. API 에러 발생
```
API 에러: {error_msg}
```
**해결**: Shop ID 또는 User ID 확인

### 2. Google Sheets 인증 실패
```
서비스 계정 JSON 파일이 없습니다
```
**해결**: `credentials.json` 파일 확인 (루트 디렉토리)

### 3. 리뷰가 수집되지 않음
```
총 리뷰: 0개
```
**해결**:
- 날짜 범위 확인 (`COLLECTION_WINDOW_DAYS`)
- 해당 기간에 리뷰가 실제로 있는지 확인

---

## 📝 로그

로그 파일: `data/shopee_scraper.log`

```bash
# 로그 확인
tail -f data/shopee_scraper.log
```

---

## 🔐 보안

### 필요한 Secrets (GitHub Actions)

1. **GOOGLE_SHEETS_CREDENTIALS**
   - Service Account JSON 키 전체 내용

2. **GOOGLE_SHEETS_SPREADSHEET_ID**
   - 스프레드시트 ID: `1NVUVShv5tAveINA9DdB2D21z71L3tF0In5JVK6LYX9s`

---

## 📚 참고 자료

- [Shopee Open Platform](https://open.shopee.com/documents)
- [Google Sheets API](https://developers.google.com/sheets/api)
- [gspread Documentation](https://docs.gspread.org/)

---

## 🎯 다음 단계

### 추가 기능 제안
1. **Slack 알림**: 수집 완료 시 알림
2. **에러 알림**: 실패 시 즉시 알림
3. **통계 대시보드**: 리뷰 트렌드 분석
4. **다국가 확장**: 태국, 말레이시아 등 추가

---

## 📞 문의

문제가 발생하면 GitHub Issues에 등록해주세요.
