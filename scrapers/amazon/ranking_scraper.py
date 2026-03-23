"""
Amazon 상품 랭킹/순위 스크래퍼

각 상품 상세 페이지에서 Best Sellers Rank, 고객 평점, 리뷰 수를 수집합니다.
Playwright Chromium 기반, ARM64 호환.
"""

import asyncio
import json
import logging
import random
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

BASE_URLS = {
    "us": "https://www.amazon.com",
    "uk": "https://www.amazon.co.uk",
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

# 상품 상세 페이지 #detailBullets_feature_div 에서 BSR + 평점 + 리뷰 수 추출
EXTRACT_RANKING_JS = """
() => {
    const result = {
        bsr_rank: null,
        bsr_category: null,
        bsr_category_url: null,
        sub_ranks: [],
        rating: null,
        review_count: null
    };

    // Best Sellers Rank 파싱
    const detailBullets = document.querySelector('#detailBullets_feature_div');
    if (detailBullets) {
        const items = detailBullets.querySelectorAll('.a-list-item');
        for (const item of items) {
            const bold = item.querySelector('.a-text-bold');
            if (!bold || !bold.textContent.includes('Best Sellers Rank')) continue;

            // 서브 리스트 제거 후 메인 순위 텍스트만 추출
            const clone = item.cloneNode(true);
            const subList = clone.querySelector('.zg_hrsr');
            if (subList) subList.remove();
            const mainText = clone.textContent.trim().replace(/\\s+/g, ' ');

            // "#6 in Beauty & Personal Care" 패턴
            const mainMatch = mainText.match(/#([\\d,]+)\\s+in\\s+(.+?)(?:\\s*\\(See Top|$)/);
            if (mainMatch) {
                result.bsr_rank = parseInt(mainMatch[1].replace(/,/g, ''));
                result.bsr_category = mainMatch[2].trim();
            }

            // 메인 카테고리 bestsellers URL
            const bsrLinks = item.querySelectorAll('a[href*="bestsellers"]');
            if (bsrLinks.length > 0) result.bsr_category_url = bsrLinks[0].href;

            // 서브 카테고리 순위 파싱
            item.querySelectorAll('.zg_hrsr li').forEach(li => {
                const text = li.textContent.trim().replace(/\\s+/g, ' ');
                const link = li.querySelector('a');
                const rankMatch = text.match(/#([\\d,]+)\\s+in\\s+(.+)/);
                if (rankMatch) {
                    result.sub_ranks.push({
                        rank: parseInt(rankMatch[1].replace(/,/g, '')),
                        category: rankMatch[2].trim(),
                        url: link ? link.href : ''
                    });
                }
            });
            break;
        }
    }

    // 고객 평점 (acrPopover title 속성: "4.5 out of 5 stars")
    const ratingEl = document.querySelector('#acrPopover');
    if (ratingEl) {
        const title = ratingEl.getAttribute('title') || '';
        const m = title.match(/([\\d.]+)\\s+out of/);
        if (m) result.rating = parseFloat(m[1]);
    }

    // 리뷰 수 (aria-label: "38,570 Reviews")
    const reviewEl = document.querySelector('#acrCustomerReviewText');
    if (reviewEl) {
        const aria = reviewEl.getAttribute('aria-label') || reviewEl.textContent || '';
        const m = aria.match(/([\\d,]+)/);
        if (m) result.review_count = parseInt(m[1].replace(/,/g, ''));
    }

    return result;
}
"""


async def _scrape_one(page, asin: str, region: str) -> Optional[dict]:
    """단일 상품 페이지에서 랭킹 정보 수집."""
    base_url = BASE_URLS.get(region, BASE_URLS["us"])
    url = f"{base_url}/dp/{asin}"

    try:
        response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        if not response or response.status >= 400:
            status = response.status if response else "no response"
            logger.warning("[%s] %s: HTTP %s", region.upper(), asin, status)
            return None

        try:
            await page.wait_for_selector("#detailBullets_feature_div", timeout=8000)
        except Exception:
            logger.warning("[%s] %s: #detailBullets_feature_div not found", region.upper(), asin)

        data = await page.evaluate(EXTRACT_RANKING_JS)

        if data.get("bsr_rank") is None and data.get("rating") is None:
            logger.warning("[%s] %s: 랭킹 데이터 없음 (CAPTCHA 또는 구조 변경 가능성)", region.upper(), asin)
            return None

        return data

    except Exception as e:
        logger.error("[%s] %s: %s", region.upper(), asin, e)
        return None


async def scrape_all_rankings(
    asins: list[str],
    region: str,
    product_names: dict[str, str] | None = None,
    delay_min: float = 2.5,
    delay_max: float = 5.5,
) -> list[dict]:
    """
    모든 상품의 랭킹 정보 수집.

    Returns:
        [
          {
            "asin": "B0B2RM68G2",
            "region": "us",
            "product_name": "...",
            "bsr_rank": 6,
            "bsr_category": "Beauty & Personal Care",
            "bsr_category_url": "https://...",
            "sub_ranks_json": '[{"rank":1,"category":"Facial Masks","url":"..."}]',
            "rating": 4.5,
            "review_count": 38570,
            "collected_at": "2026-03-23T14:00:00+00:00",
            "collected_date": "2026-03-23",
          },
          ...
        ]
    """
    from playwright.async_api import async_playwright

    if not asins:
        return []

    collected_at = datetime.now(timezone.utc).isoformat()
    collected_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            locale="en-US" if region == "us" else "en-GB",
            viewport={"width": 1280, "height": 800},
        )
        page = await context.new_page()

        for i, asin in enumerate(asins):
            product_name = (product_names or {}).get(asin, "")
            logger.info(
                "[%s] (%d/%d) %s %s",
                region.upper(), i + 1, len(asins), asin, product_name[:40]
            )

            data = await _scrape_one(page, asin, region)
            if data:
                results.append({
                    "asin": asin,
                    "region": region,
                    "product_name": product_name,
                    "bsr_rank": data.get("bsr_rank"),
                    "bsr_category": data.get("bsr_category") or "",
                    "bsr_category_url": data.get("bsr_category_url") or "",
                    "sub_ranks_json": json.dumps(data.get("sub_ranks", []), ensure_ascii=False),
                    "rating": data.get("rating"),
                    "review_count": data.get("review_count"),
                    "collected_at": collected_at,
                    "collected_date": collected_date,
                })

            if i < len(asins) - 1:
                await asyncio.sleep(random.uniform(delay_min, delay_max))

        await browser.close()

    logger.info("[%s] 총 %d/%d 상품 수집 완료", region.upper(), len(results), len(asins))
    return results
