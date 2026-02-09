"""
Biodance 제품 리뷰 크롤러

biodance.com (Shopify 기반)의 모든 제품 리뷰를 Trustoo.io API를 통해 수집합니다.
브라우저 자동화 없이 HTTP 요청만으로 동작합니다.
"""

import csv
import json
import logging
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)


@dataclass
class BiodanceReview:
    review_id: str
    product_name: str
    product_id: str
    author: str
    author_country: str
    star: int
    title: str
    content: str
    date: str
    verified_purchase: bool
    item_type: str
    reply_content: str
    image_urls: list[str] = field(default_factory=list)
    video_urls: list[str] = field(default_factory=list)
    likes_count: int = 0


class BiodanceReviewCrawler:
    """Biodance 전 제품 리뷰 수집 크롤러"""

    SHOP_ID = "88710676791"
    TRUSTOO_API = "https://api.trustoo.io/api/v1/reviews/get_product_reviews"
    PRODUCTS_API = "https://biodance.com/collections/all-products/products.json"
    PRODUCT_API = "https://biodance.com/products/{handle}.json"

    MAX_RETRIES = 3
    REQUEST_DELAY = 1.0  # 초

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/131.0.0.0 Safari/537.36",
            "Accept": "application/json",
        })

    def _request_with_retry(self, url: str, params: dict | None = None) -> dict | None:
        """HTTP GET 요청 + 최대 3회 재시도 (exponential backoff)"""
        for attempt in range(self.MAX_RETRIES):
            try:
                resp = self.session.get(url, params=params, timeout=30)
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as e:
                wait = 2 ** attempt
                logger.warning(
                    "요청 실패 (시도 %d/%d): %s — %s초 후 재시도",
                    attempt + 1, self.MAX_RETRIES, e, wait,
                )
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(wait)
        logger.error("최대 재시도 초과: %s", url)
        return None

    def fetch_all_products(self) -> list[dict]:
        """Shopify JSON API로 전체 제품 목록 조회"""
        logger.info("제품 목록 조회 중...")

        data = self._request_with_retry(self.PRODUCTS_API, {"limit": 250})
        if not data or "products" not in data:
            logger.error("제품 목록 조회 실패")
            return []

        products = [
            {"id": p["id"], "title": p["title"], "handle": p["handle"]}
            for p in data["products"]
        ]
        logger.info("총 %d개 제품 확인 완료", len(products))
        return products

    def _parse_review_item(self, item: dict, product_name: str, product_id: str) -> BiodanceReview:
        """API 응답의 개별 리뷰 항목을 BiodanceReview로 변환"""
        image_urls = []
        video_urls = []
        for res in item.get("resources", []):
            src = res.get("src", "")
            if not src:
                continue
            if res.get("resource_type") == 2:
                video_urls.append(src)
            else:
                image_urls.append(src)

        commented_at = item.get("commented_at", "")
        if commented_at:
            try:
                date_str = str(commented_at).split(" ")[0]
            except (ValueError, IndexError):
                date_str = str(commented_at)
        else:
            date_str = ""

        return BiodanceReview(
            review_id=str(item.get("id", "")),
            product_name=product_name,
            product_id=str(product_id),
            author=item.get("author", ""),
            author_country=item.get("author_country", ""),
            star=item.get("star", 0),
            title=item.get("title", ""),
            content=item.get("content", ""),
            date=date_str,
            verified_purchase=bool(item.get("verified_badge", 0)),
            item_type=item.get("item_type", ""),
            reply_content=item.get("reply_content", ""),
            image_urls=image_urls,
            video_urls=video_urls,
            likes_count=item.get("likes_count", 0),
        )

    def fetch_product_reviews(
        self,
        product_id: str,
        product_name: str,
        known_review_ids: set[str] | None = None,
    ) -> tuple[list[BiodanceReview], dict]:
        """특정 제품의 리뷰를 페이지네이션으로 수집

        Args:
            known_review_ids: 이미 수집된 review_id 집합.
                제공 시, 해당 ID는 건너뛰고 한 페이지 전체가 기존 리뷰면 조기 종료.

        Returns:
            (새 리뷰 리스트, 메타 정보 dict)
        """
        new_reviews: list[BiodanceReview] = []
        meta = {
            "average_rating": "0",
            "total_reviews": 0,
            "rating_distribution": {"5": 0, "4": 0, "3": 0, "2": 0, "1": 0},
        }
        if known_review_ids is None:
            known_review_ids = set()

        page = 1
        total_pages = 1

        while page <= total_pages:
            params = {
                "shop_id": self.SHOP_ID,
                "product_id": product_id,
                "limit": 40,
                "page": page,
                "sort_by": "commented-at-descending",
            }
            data = self._request_with_retry(self.TRUSTOO_API, params)
            if not data or "data" not in data:
                logger.warning("리뷰 응답 없음: %s (page %d)", product_name, page)
                break

            payload = data["data"]

            if page == 1:
                page_info = payload.get("page") or {}
                total_pages = page_info.get("total_page", 1)
                total_rating = payload.get("total_rating") or {}
                meta["average_rating"] = total_rating.get("rating", "0") or "0"
                meta["total_reviews"] = total_rating.get("total_reviews", 0) or 0

                logger.info(
                    "  %s: 리뷰 %d개, %d페이지",
                    product_name,
                    meta["total_reviews"],
                    total_pages,
                )

            # 리뷰 파싱 + 중복 체크
            page_new_count = 0
            for item in payload.get("list", []):
                rid = str(item.get("id", ""))
                if rid in known_review_ids:
                    continue
                review = self._parse_review_item(item, product_name, product_id)
                new_reviews.append(review)
                page_new_count += 1

            # 이 페이지에서 신규 리뷰가 0개면 → 이후 페이지도 전부 기존 리뷰이므로 조기 종료
            if known_review_ids and page_new_count == 0:
                logger.info("    page %d: 신규 리뷰 없음 → 조기 종료", page)
                break

            page += 1
            if page <= total_pages:
                time.sleep(self.REQUEST_DELAY)

        return new_reviews, meta

    @staticmethod
    def _calc_rating_stats(reviews: list[dict]) -> tuple[str, dict]:
        """리뷰 리스트로부터 평균 평점과 별점 분포를 계산"""
        dist = {"5": 0, "4": 0, "3": 0, "2": 0, "1": 0}
        if not reviews:
            return "0", dist
        for r in reviews:
            key = str(r.get("star", 0))
            if key in dist:
                dist[key] += 1
        total_stars = sum(r.get("star", 0) for r in reviews)
        avg = f"{total_stars / len(reviews):.2f}"
        return avg, dist

    @staticmethod
    def load_existing_data(filepath: str) -> dict | None:
        """기존 JSON 결과 파일을 로드. 없으면 None 반환."""
        if not os.path.exists(filepath):
            return None
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("기존 데이터 로드 실패: %s — %s", filepath, e)
            return None

    def collect_incremental(self, existing_data_path: str) -> tuple[dict, int]:
        """기존 데이터를 기반으로 신규 리뷰만 증분 수집

        Args:
            existing_data_path: 기존 biodance_reviews_all.json 경로

        Returns:
            (병합된 전체 결과 dict, 이번에 추가된 신규 리뷰 수)
        """
        existing = self.load_existing_data(existing_data_path)

        # 기존 데이터에서 제품별 리뷰 맵 구축: product_id → {review_id_set, reviews_list}
        existing_map: dict[str, dict] = {}
        if existing:
            for prod in existing.get("products", []):
                pid = str(prod["product_id"])
                review_ids = {str(r["review_id"]) for r in prod.get("reviews", [])}
                existing_map[pid] = {
                    "product": prod,
                    "review_ids": review_ids,
                }
            logger.info(
                "기존 데이터 로드: %d개 제품, %d개 리뷰",
                existing.get("total_products", 0),
                existing.get("total_reviews", 0),
            )
        else:
            logger.info("기존 데이터 없음 — 전체 수집 모드")

        products = self.fetch_all_products()
        if not products:
            logger.error("제품 목록을 가져오지 못했습니다.")
            if existing:
                return existing, 0
            return {"products": [], "total_products": 0, "total_reviews": 0}, 0

        results: list[dict] = []
        grand_total = 0
        new_total = 0

        for i, prod in enumerate(products, 1):
            product_id = str(prod["id"])
            product_name = prod["title"]
            handle = prod["handle"]

            known_ids = existing_map.get(product_id, {}).get("review_ids", set())

            logger.info("[%d/%d] %s 수집 중... (기존 %d개)", i, len(products), product_name, len(known_ids))
            new_reviews, meta = self.fetch_product_reviews(product_id, product_name, known_ids)

            # 기존 리뷰 + 신규 리뷰 병합
            if product_id in existing_map:
                merged_reviews = existing_map[product_id]["product"]["reviews"] + [asdict(r) for r in new_reviews]
            else:
                merged_reviews = [asdict(r) for r in new_reviews]

            # 병합된 리뷰로 통계 재계산
            avg_rating, rating_dist = self._calc_rating_stats(merged_reviews)

            results.append({
                "product_name": product_name,
                "product_id": product_id,
                "handle": handle,
                "average_rating": avg_rating,
                "total_reviews": len(merged_reviews),
                "rating_distribution": rating_dist,
                "reviews": merged_reviews,
            })

            new_count = len(new_reviews)
            if new_count > 0:
                logger.info("    → 신규 %d개 추가 (총 %d개)", new_count, len(merged_reviews))
            new_total += new_count
            grand_total += len(merged_reviews)

            if i < len(products):
                time.sleep(self.REQUEST_DELAY)

        collected_at = datetime.now(timezone.utc).isoformat()
        merged_result = {
            "collected_at": collected_at,
            "shop_id": self.SHOP_ID,
            "total_products": len(results),
            "total_reviews": grand_total,
            "products": results,
        }
        return merged_result, new_total

    @staticmethod
    def save_to_json(data: dict, filepath: str) -> None:
        """JSON 형식으로 저장"""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info("JSON 저장 완료: %s", filepath)

    @staticmethod
    def save_to_csv(reviews: list[dict], filepath: str) -> None:
        """CSV 형식으로 저장"""
        if not reviews:
            logger.warning("저장할 리뷰가 없습니다.")
            return

        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        fieldnames = [
            "product_name", "product_id", "review_id", "author",
            "author_country", "star", "title", "content", "date",
            "verified_purchase", "item_type", "reply_content",
            "image_urls", "video_urls", "likes_count",
        ]

        with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for review in reviews:
                row = {k: review.get(k, "") for k in fieldnames}
                # list → 세미콜론 구분 문자열
                if isinstance(row["image_urls"], list):
                    row["image_urls"] = ";".join(row["image_urls"])
                if isinstance(row["video_urls"], list):
                    row["video_urls"] = ";".join(row["video_urls"])
                writer.writerow(row)

        logger.info("CSV 저장 완료: %s (%d건)", filepath, len(reviews))
