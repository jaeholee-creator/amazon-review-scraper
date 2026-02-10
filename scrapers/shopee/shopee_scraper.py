"""
Shopee Review Scraper - API 기반
"""
import logging
import time
from datetime import datetime
from typing import Optional
import requests

logger = logging.getLogger(__name__)


class ShopeeScraper:
    """Shopee 비공식 API를 사용한 리뷰 스크래퍼"""

    API_ENDPOINT = "/api/v4/seller_operation/get_shop_ratings_new"

    def __init__(self, country: str, userid: str, shopid: str):
        """
        Args:
            country: 국가 코드 ('sg', 'ph')
            userid: User ID
            shopid: Shop ID
        """
        self.country = country.lower()
        self.userid = userid
        self.shopid = shopid
        self.base_url = f"https://shopee.{self.country}"
        self.session = requests.Session()

        # User-Agent 설정
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Referer': f'{self.base_url}/buyer/{userid}/rating?shop_id={shopid}'
        })

    def fetch_reviews(
        self,
        start_date: datetime,
        end_date: datetime,
        limit: int = 50
    ) -> list[dict]:
        """
        날짜 범위로 리뷰를 가져오기

        Args:
            start_date: 시작 날짜 (datetime)
            end_date: 종료 날짜 (datetime)
            limit: 한 번에 가져올 리뷰 수 (기본 50)

        Returns:
            리뷰 리스트 (dict)
        """
        all_reviews = []
        offset = 0
        page_count = 0

        logger.info(
            f"[{self.country.upper()}] 리뷰 수집 시작: {start_date.date()} ~ {end_date.date()}"
        )

        while True:
            page_count += 1
            logger.info(f"[{self.country.upper()}] 페이지 {page_count} 요청 중... (offset={offset})")

            try:
                # API 호출
                reviews = self._fetch_page(offset, limit)

                if not reviews:
                    logger.info(f"[{self.country.upper()}] 더 이상 리뷰가 없습니다.")
                    break

                # 날짜 필터링
                filtered_reviews = []
                stop_pagination = False

                for review in reviews:
                    review_date = datetime.fromtimestamp(review['ctime'])

                    # 날짜 범위 체크
                    if start_date <= review_date <= end_date:
                        filtered_reviews.append(review)
                    elif review_date < start_date:
                        # 시작 날짜보다 이전이면 중단
                        stop_pagination = True
                        break

                all_reviews.extend(filtered_reviews)
                logger.info(
                    f"[{self.country.upper()}] 페이지 {page_count}: "
                    f"수집 {len(reviews)}개 / 필터링 후 {len(filtered_reviews)}개 "
                    f"(총 누적: {len(all_reviews)}개)"
                )

                if stop_pagination:
                    logger.info(
                        f"[{self.country.upper()}] 날짜 범위를 벗어났습니다. 수집 종료."
                    )
                    break

                # 다음 페이지
                offset += limit

                # Rate limiting
                time.sleep(1.0)

            except Exception as e:
                logger.error(f"[{self.country.upper()}] 페이지 {page_count} 수집 실패: {e}")
                break

        logger.info(
            f"[{self.country.upper()}] 수집 완료: 총 {len(all_reviews)}개 리뷰 "
            f"({page_count} 페이지)"
        )
        return all_reviews

    def _fetch_page(self, offset: int, limit: int) -> list[dict]:
        """
        단일 페이지 API 호출

        Args:
            offset: 오프셋
            limit: 개수

        Returns:
            리뷰 리스트
        """
        url = f"{self.base_url}{self.API_ENDPOINT}"
        params = {
            'userid': self.userid,
            'shopid': self.shopid,
            'limit': limit,
            'offset': offset,
            'replied': 'undefined'
        }

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            if data.get('error') != 0:
                logger.error(f"API 에러: {data.get('error_msg', 'Unknown error')}")
                return []

            return data.get('data', {}).get('items', [])

        except requests.exceptions.RequestException as e:
            logger.error(f"API 요청 실패: {e}")
            return []
        except ValueError as e:
            logger.error(f"JSON 파싱 실패: {e}")
            return []

    def parse_review(self, review: dict) -> dict:
        """
        Shopee API 응답을 표준 형식으로 변환

        Args:
            review: Shopee API 원본 리뷰 데이터

        Returns:
            표준화된 리뷰 dict
        """
        # 제품 정보
        product = review.get('product_items', [{}])[0]
        product_name = product.get('name', '')
        variation = product.get('model_name', '')

        # 날짜 변환
        review_date = datetime.fromtimestamp(review['ctime'])

        # 이미지/비디오 URL
        image_urls = []
        video_urls = []

        for media in review.get('medias', []):
            if media.get('image'):
                image_id = media['image'].get('image_id', '')
                if image_id:
                    image_urls.append(f"https://down-sg.img.susercontent.com/{image_id}")

            if media.get('video'):
                video_url = media['video'].get('url', '')
                if video_url:
                    video_urls.append(video_url)

        # Detailed rating 처리 (None일 수 있음)
        detailed_rating = review.get('detailed_rating') or {}

        # 표준 형식으로 변환
        return {
            'review_id': str(review['cmtid']),
            'collected_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'product_name': product_name,
            'product_id': str(product.get('itemid', '')),
            'author': review.get('author_username', ''),
            'author_country': self.country.upper(),
            'star': review.get('rating_star', 0),
            'title': '',  # Shopee에는 제목 없음
            'content': review.get('comment', ''),
            'date': review_date.strftime('%Y-%m-%d'),
            'verified_purchase': True,  # Shopee는 구매 확정 후 리뷰
            'item_type': variation,
            'reply_content': review.get('ItemRatingReply', {}).get('comment', '') if review.get('ItemRatingReply') else '',
            'image_urls': ';'.join(image_urls),
            'video_urls': ';'.join(video_urls),
            'likes_count': review.get('like_count', 0) or 0,
            'detailed_rating_product': detailed_rating.get('product_quality', 0),
            'detailed_rating_seller': detailed_rating.get('seller_service', 0),
            'detailed_rating_delivery': detailed_rating.get('delivery_service', 0),
        }

    def scrape(self, start_date: datetime, end_date: datetime) -> dict:
        """
        전체 스크래핑 프로세스 실행

        Args:
            start_date: 시작 날짜
            end_date: 종료 날짜

        Returns:
            결과 dict
        """
        logger.info(f"=" * 60)
        logger.info(f"Shopee {self.country.upper()} 스크래핑 시작")
        logger.info(f"Shop ID: {self.shopid}")
        logger.info(f"날짜 범위: {start_date.date()} ~ {end_date.date()}")
        logger.info(f"=" * 60)

        start_time = time.time()

        # 리뷰 수집
        raw_reviews = self.fetch_reviews(start_date, end_date)

        # 파싱
        parsed_reviews = [self.parse_review(r) for r in raw_reviews]

        elapsed_time = time.time() - start_time

        logger.info(f"=" * 60)
        logger.info(f"스크래핑 완료: {len(parsed_reviews)}개 리뷰")
        logger.info(f"소요 시간: {elapsed_time:.1f}초")
        logger.info(f"=" * 60)

        return {
            'country': self.country.upper(),
            'shopid': self.shopid,
            'collected_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'reviews': parsed_reviews,
            'total_reviews': len(parsed_reviews),
            'date_range': {
                'start': start_date.strftime('%Y-%m-%d'),
                'end': end_date.strftime('%Y-%m-%d')
            }
        }
