"""
Amazon 상품 랭킹/순위 스크래퍼

각 상품 상세 페이지에서 Best Sellers Rank, 고객 평점, 리뷰 수를 수집합니다.
Playwright Chromium 기반, ARM64 호환.

페이지 구조:
  - US: #detailBullets_feature_div (bullet list, "#6 in Beauty & Personal Care")
  - UK: table.prodDetTable × 3개 중 BSR 있는 테이블 ("3 in Beauty") — 스크롤 후 lazy-load
"""

import asyncio
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
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

EXTRACT_RANKING_JS = """
() => {
    const result = {
        bsr_rank: null,
        bsr_category: null,
        bsr_category_url: null,
        sub_rank: null,
        sub_category: null,
        rating: null,
        review_count: null
    };

    // ── US 구조: #detailBullets_feature_div ──────────────────────────────────
    const detailBullets = document.querySelector('#detailBullets_feature_div');
    if (detailBullets) {
        const items = detailBullets.querySelectorAll('.a-list-item');
        for (const item of items) {
            const bold = item.querySelector('.a-text-bold');
            if (!bold || !bold.textContent.includes('Best Sellers Rank')) continue;

            const clone = item.cloneNode(true);
            const subList = clone.querySelector('.zg_hrsr');
            if (subList) subList.remove();
            const mainText = clone.textContent.trim().replace(/\\s+/g, ' ');

            const mainMatch = mainText.match(/#?([\\d,]+)\\s+in\\s+(.+?)(?:\\s*\\(See Top|$)/);
            if (mainMatch) {
                result.bsr_rank = parseInt(mainMatch[1].replace(/,/g, ''));
                result.bsr_category = mainMatch[2].trim();
            }

            const bsrLinks = item.querySelectorAll('a[href*="bestsellers"]');
            if (bsrLinks.length > 0) result.bsr_category_url = bsrLinks[0].href;

            const firstSub = item.querySelector('.zg_hrsr li');
            if (firstSub) {
                const text = firstSub.textContent.trim().replace(/\\s+/g, ' ');
                const m = text.match(/#?([\\d,]+)\\s+in\\s+(.+)/);
                if (m) {
                    result.sub_rank = parseInt(m[1].replace(/,/g, ''));
                    result.sub_category = m[2].trim();
                }
            }
            break;
        }
    }

    // ── UK 구조: table.prodDetTable (3개 테이블 중 BSR 있는 것 찾기) ──────────
    // table[0]: 상품 사양, table[1]: Date/Reviews/BSR, table[2]: 기타
    if (result.bsr_rank === null) {
        const tables = document.querySelectorAll('table.prodDetTable');
        for (let t = 0; t < tables.length; t++) {
            const rows = tables[t].querySelectorAll('tr');
            let found = false;
            for (let r = 0; r < rows.length; r++) {
                const th = rows[r].querySelector('th');
                if (!th || !th.textContent.includes('Best Sellers Rank')) continue;

                const td = rows[r].querySelector('td');
                if (!td) break;

                // li[0]: "3 in Beauty (See Top 100 in Beauty)"
                // li[1]: "1 in Face Treatments & Masks"
                const listItems = td.querySelectorAll('li');
                for (let i = 0; i < listItems.length; i++) {
                    const text = listItems[i].textContent.trim().replace(/\\s+/g, ' ');
                    const link = listItems[i].querySelector('a[href*="bestsellers"]');
                    const m = text.match(/^([\\d,]+)\\s+in\\s+(.+?)(?:\\s*\\(See Top|$)/);
                    if (!m) continue;
                    const rank = parseInt(m[1].replace(/,/g, ''));
                    const category = m[2].trim();
                    if (i === 0) {
                        result.bsr_rank = rank;
                        result.bsr_category = category;
                        if (link) result.bsr_category_url = link.href;
                    } else if (i === 1) {
                        result.sub_rank = rank;
                        result.sub_category = category;
                    }
                }
                found = true;
                break;
            }
            if (found) break;
        }
    }

    // ── 고객 평점 (#acrPopover title: "4.5 out of 5 stars") ──────────────────
    const ratingEl = document.querySelector('#acrPopover');
    if (ratingEl) {
        const title = ratingEl.getAttribute('title') || '';
        const m = title.match(/([\\d.]+)\\s+out of/);
        if (m) result.rating = parseFloat(m[1]);
    }

    // ── 리뷰 수 (#acrCustomerReviewText aria-label: "1,789 Reviews") ─────────
    const reviewEl = document.querySelector('#acrCustomerReviewText');
    if (reviewEl) {
        const aria = reviewEl.getAttribute('aria-label') || reviewEl.textContent || '';
        const m = aria.match(/([\\d,]+)/);
        if (m) result.review_count = parseInt(m[1].replace(/,/g, ''));
    }

    return result;
}
"""


async def _scrape_one(context, asin: str, region: str) -> Optional[dict]:
    """단일 상품 페이지에서 랭킹 정보 수집. 매 호출마다 새 페이지를 생성."""
    base_url = BASE_URLS.get(region, BASE_URLS["us"])
    url = f"{base_url}/dp/{asin}"
    page = await context.new_page()

    try:
        response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        if not response or response.status >= 400:
            status = response.status if response else "no response"
            logger.warning("[%s] %s: HTTP %s", region.upper(), asin, status)
            return None

        # 초기 렌더링 대기 후 스크롤 (UK prodDetTable lazy-load 트리거)
        await page.wait_for_timeout(1000)
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.7)")
        await page.wait_for_timeout(3000)

        data = await page.evaluate(EXTRACT_RANKING_JS)

        if data.get("bsr_rank") is None and data.get("rating") is None:
            logger.warning("[%s] %s: 데이터 없음 (CAPTCHA 또는 구조 변경 가능성)", region.upper(), asin)
            return None

        return data

    except Exception as e:
        logger.error("[%s] %s: %s", region.upper(), asin, e)
        return None

    finally:
        await page.close()


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
            "product_name": "Bio-Collagen Real Deep Mask",
            "bsr_rank": 6,
            "bsr_category": "Beauty & Personal Care",
            "bsr_category_url": "https://...",
            "sub_rank": 1,
            "sub_category": "Facial Masks",
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
            locale="en-US",
            viewport={"width": 1920, "height": 1080},
        )

        for i, asin in enumerate(asins):
            product_name = (product_names or {}).get(asin, "")
            logger.info(
                "[%s] (%d/%d) %s %s",
                region.upper(), i + 1, len(asins), asin, product_name[:40]
            )

            data = await _scrape_one(context, asin, region)
            if data:
                results.append({
                    "asin": asin,
                    "region": region,
                    "product_name": product_name,
                    "bsr_rank": data.get("bsr_rank"),
                    "bsr_category": data.get("bsr_category") or "",
                    "bsr_category_url": data.get("bsr_category_url") or "",
                    "sub_rank": data.get("sub_rank"),
                    "sub_category": data.get("sub_category") or "",
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
