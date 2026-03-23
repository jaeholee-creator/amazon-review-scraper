"""
Amazon 상품 카탈로그 동기화

BIODANCE 브랜드 스토어 ALL 페이지를 Playwright로 크롤링하여
신규 상품을 감지하고 products.csv를 자동으로 업데이트합니다.

브랜드 검색 필터(rh=p_4:BIODANCE) 대신 브랜드 스토어 ALL 페이지를 사용합니다.
→ 브랜드 검색은 약 19개만 반환하지만, 스토어 페이지는 모든 상품을 포함합니다.

사용법:
    python scrapers/amazon/catalog_sync.py us
    python scrapers/amazon/catalog_sync.py uk
"""

import asyncio
import csv
import logging
import os
import sys

logger = logging.getLogger(__name__)

# BIODANCE 브랜드 스토어 ALL 페이지
# US: https://www.amazon.com/stores/BIODANCE/page/6651FCB6-7282-4AEA-8BA5-324130851A71 메인
# UK: https://www.amazon.co.uk/stores/BIODANCE/page/A4C2F38D-867D-4604-B80D-C3CD0ADB6A5F 메인
BRAND_STORE_URLS = {
    "us": "https://www.amazon.com/stores/page/C83E0E56-BFF3-4557-B3F8-7059A9C69CDF",
    "uk": "https://www.amazon.co.uk/stores/page/A914CB68-5FCF-41B9-9B6B-FA907E6FEB9D",
}

# 스토어 페이지 실패 시 폴백 (브랜드 필터 없는 일반 검색)
BRAND_SEARCH_FALLBACK = {
    "us": "https://www.amazon.com/s?k=BIODANCE&s=review-rank",
    "uk": "https://www.amazon.co.uk/s?k=BIODANCE&s=review-rank",
}

PRODUCTS_CSV = {
    "us": "config/products.csv",
    "uk": "config/products_uk.csv",
}

US_CSV_FIELDNAMES = ["asin", "name", "price", "rating", "review_count", "url"]
UK_CSV_FIELDNAMES = ["asin", "name"]

EXTRACT_ASINS_JS = """
() => {
    const seen = new Set();
    const results = [];

    // 이미지가 있는 컨테이너 안의 /dp/ASIN 링크만 추출 (실제 상품 카드)
    // Amazon Business Card 등 내비게이션 링크는 이미지가 없으므로 제외됨
    document.querySelectorAll('a[href]').forEach(function(a) {
        const href = a.getAttribute('href') || '';
        const match = href.match(/\\/dp\\/([A-Z0-9]{10})(?:[\\/\\?]|$)/);
        if (!match) return;
        const asin = match[1];
        if (seen.has(asin)) return;

        // 상위 8개 노드 안에 img[src]가 있어야 실제 상품 카드로 간주
        let node = a;
        let hasImage = false;
        for (let i = 0; i < 8; i++) {
            node = node.parentElement;
            if (!node) break;
            if (node.querySelector('img[src]')) {
                hasImage = true;
                break;
            }
        }
        if (!hasImage) return;

        seen.add(asin);
        const name = (a.title || a.textContent || '').trim().replace(/\\s+/g, ' ').slice(0, 200);
        results.push({ asin: asin, name: name });
    });

    // data-asin 속성 보완 (위에서 못 잡은 경우 — 이미지 있는 컨테이너만)
    document.querySelectorAll('[data-asin]').forEach(function(el) {
        const asin = el.getAttribute('data-asin');
        if (!asin || asin.length !== 10) return;
        if (seen.has(asin)) return;
        if (!el.querySelector('img[src]')) return;
        seen.add(asin);
        const nameEl = el.querySelector('h2, [class*="title"], [class*="name"], a[title]');
        const name = nameEl ? (nameEl.title || nameEl.textContent || '').trim().slice(0, 200) : '';
        results.push({ asin: asin, name: name });
    });

    return results;
}
"""


async def _extract_asins_from_page(page) -> list[dict]:
    """현재 페이지에서 ASIN + 상품명 추출 (이미지 있는 실제 상품만)"""
    return await page.evaluate(EXTRACT_ASINS_JS)


async def discover_asins_from_store(region: str) -> list[dict]:
    """
    브랜드 스토어 ALL 페이지에서 ASIN 수집
    """
    from playwright.async_api import async_playwright

    store_url = BRAND_STORE_URLS.get(region)
    if not store_url:
        logger.error(f"[{region.upper()}] 지원하지 않는 region: {region}")
        return []
    base_url = "https://www.amazon.com" if region == "us" else "https://www.amazon.co.uk"
    discovered: list[dict] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )
        page = await context.new_page()

        try:
            logger.info(f"[{region.upper()}] 브랜드 스토어 ALL 페이지 접속: {store_url}")
            await page.goto(store_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)

            prev_height = 0
            for scroll_attempt in range(20):
                await page.evaluate("window.scrollBy(0, window.innerHeight * 2)")
                await page.wait_for_timeout(1500)
                current_height = await page.evaluate("document.body.scrollHeight")
                if current_height == prev_height:
                    logger.info(f"[{region.upper()}] 스크롤 완료 (attempt {scroll_attempt + 1})")
                    break
                prev_height = current_height

            await page.evaluate("window.scrollTo(0, 0)")
            await page.wait_for_timeout(1000)

            items = await _extract_asins_from_page(page)
            logger.info(f"[{region.upper()}] 브랜드 스토어에서 {len(items)}개 ASIN 발견")
            discovered.extend(items)

        except Exception as e:
            logger.error(f"[{region.upper()}] 스토어 페이지 크롤링 오류: {e}")
        finally:
            await browser.close()

    seen: set[str] = set()
    unique: list[dict] = []
    skipped: list[str] = []
    for item in discovered:
        asin = item["asin"]
        if asin in seen:
            continue
        name = item.get("name", "")
        if name and "biodance" not in name.lower() and "biod" not in name.lower():
            skipped.append(f"{asin}: {name[:60]}")
            continue
        seen.add(asin)
        item["url"] = f"{base_url}/dp/{asin}"
        unique.append(item)

    if skipped:
        logger.info(f"[{region.upper()}] 비브랜드 항목 {len(skipped)}개 제외:")
        for s in skipped:
            logger.info(f"  - {s}")

    logger.info(f"[{region.upper()}] 총 {len(unique)}개 고유 상품 (스토어 페이지)")
    return unique


async def discover_asins_from_search(region: str, max_pages: int = 15) -> list[dict]:
    """
    폴백: Amazon 브랜드 검색에서 ASIN 수집 (스토어 페이지 실패 시)
    """
    from playwright.async_api import async_playwright

    search_url = BRAND_SEARCH_FALLBACK.get(region)
    if not search_url:
        logger.error(f"[{region.upper()}] 지원하지 않는 region: {region}")
        return []
    base_url = "https://www.amazon.com" if region == "us" else "https://www.amazon.co.uk"
    discovered: list[dict] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )
        page = await context.new_page()

        try:
            logger.info(f"[{region.upper()}] 폴백 브랜드 검색 접속: {search_url}")
            await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

            for page_num in range(1, max_pages + 1):
                items = await page.evaluate("""
                    () => {
                        const results = [];
                        const containers = document.querySelectorAll(
                            '[data-component-type="s-search-result"][data-asin]'
                        );
                        containers.forEach(function(container) {
                            const asin = container.getAttribute('data-asin');
                            if (!asin || asin.length < 10) return;
                            const nameEl = container.querySelector('h2 .a-text-normal, h2 span');
                            const name = nameEl ? nameEl.textContent.trim() : '';
                            const priceEl = container.querySelector('.a-price .a-offscreen');
                            const price = priceEl ? priceEl.textContent.trim() : '';
                            results.push({ asin: asin, name: name, price: price });
                        });
                        return results;
                    }
                """)

                valid = [i for i in items if i.get("asin") and len(i["asin"]) >= 10]
                discovered.extend(valid)
                logger.info(f"[{region.upper()}] Search page {page_num}: {len(valid)}개 (누적: {len(discovered)}개)")

                next_btn = await page.query_selector(
                    "a.s-pagination-next:not(.s-pagination-disabled), li.a-last a"
                )
                if not next_btn:
                    break
                await next_btn.click()
                await page.wait_for_timeout(2500)

        except Exception as e:
            logger.error(f"[{region.upper()}] 검색 페이지 크롤링 오류: {e}")
        finally:
            await browser.close()

    seen: set[str] = set()
    unique: list[dict] = []
    for item in discovered:
        asin = item["asin"]
        if asin not in seen:
            seen.add(asin)
            item["url"] = f"{base_url}/dp/{asin}"
            unique.append(item)

    logger.info(f"[{region.upper()}] 총 {len(unique)}개 고유 상품 (검색 폴백)")
    return unique


async def discover_asins(region: str = "us", max_pages: int = 15) -> list[dict]:
    """
    ASIN 수집 메인 함수.
    브랜드 스토어 ALL 페이지 시도 → 실패 시 브랜드 검색 폴백
    """
    result = await discover_asins_from_store(region)
    if len(result) < 5:
        logger.warning(f"[{region.upper()}] 스토어 페이지에서 {len(result)}개만 발견. 브랜드 검색으로 폴백...")
        result = await discover_asins_from_search(region, max_pages)
    return result


def load_csv(csv_path: str) -> tuple[list[dict], set[str]]:
    """기존 CSV 로드. 파일이 없으면 빈 결과 반환."""
    rows: list[dict] = []
    asins: set[str] = set()
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                asin = row.get("asin", "").strip()
                if asin:
                    rows.append(row)
                    asins.add(asin)
    except FileNotFoundError:
        pass
    return rows, asins


def save_csv(rows: list[dict], csv_path: str, fieldnames: list[str]) -> None:
    """CSV 저장 (디렉토리 없으면 생성)"""
    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


async def sync_products(region: str = "us") -> dict:
    """
    브랜드 스토어 ALL 페이지에서 상품 목록을 가져와 products.csv와 동기화.
    """
    csv_path = PRODUCTS_CSV.get(region, f"config/products_{region}.csv")
    fieldnames = US_CSV_FIELDNAMES if region == "us" else UK_CSV_FIELDNAMES

    logger.info(f"[{region.upper()}] ===== Amazon 상품 카탈로그 동기화 =====")

    discovered = await discover_asins(region)
    current_asins = {item["asin"] for item in discovered}

    if not current_asins:
        logger.warning(f"[{region.upper()}] Amazon에서 상품을 가져오지 못했습니다. 기존 CSV를 그대로 유지합니다.")
        return {"added": [], "removed": [], "total": 0}

    existing_rows, existing_asins = load_csv(csv_path)
    existing_map = {row["asin"]: row for row in existing_rows}

    added_asins = current_asins - existing_asins
    removed_asins = existing_asins - current_asins
    added_items = [item for item in discovered if item["asin"] in added_asins]

    if added_items:
        all_rows = list(existing_rows)
        for item in added_items:
            asin = item["asin"]
            if region == "us":
                all_rows.append({
                    "asin": asin,
                    "name": item.get("name", ""),
                    "price": item.get("price", ""),
                    "rating": "",
                    "review_count": "0",
                    "url": item.get("url", f"https://www.amazon.com/dp/{asin}"),
                })
            else:
                all_rows.append({"asin": asin, "name": item.get("name", "")})

        save_csv(all_rows, csv_path, fieldnames)
        logger.info(f"[{region.upper()}] CSV 업데이트 완료: {len(added_items)}개 신규 추가 → {csv_path}")
        for item in added_items:
            logger.info(f"  + {item['asin']}: {item.get('name', '')[:60]}")
    else:
        logger.info(f"[{region.upper()}] 신규 상품 없음 (기존 CSV 유지)")

    if removed_asins:
        logger.warning(f"[{region.upper()}] Amazon 스토어에 더 이상 노출되지 않는 상품 {len(removed_asins)}개 (CSV 유지, 수동 확인 필요):")
        for asin in removed_asins:
            name = existing_map.get(asin, {}).get("name", "")
            logger.warning(f"  ? {asin}: {name}")

    logger.info(f"[{region.upper()}] 동기화 완료 — 스토어: {len(current_asins)}개, 추가: {len(added_items)}개, 미노출: {len(removed_asins)}개")

    return {
        "added": [item["asin"] for item in added_items],
        "removed": list(removed_asins),
        "total": len(current_asins),
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    region = sys.argv[1] if len(sys.argv) > 1 else "us"
    result = asyncio.run(sync_products(region))

    print(f"\n{'='*60}")
    print(f"Amazon 상품 동기화 완료 ({region.upper()})")
    print(f"{'='*60}")
    print(f"  스토어 현재 상품 수: {result['total']}")
    print(f"  신규 추가: {len(result['added'])}개")
    if result["added"]:
        for asin in result["added"]:
            print(f"    + {asin}")
    print(f"  미노출 감지: {len(result['removed'])}개")
    if result["removed"]:
        for asin in result["removed"]:
            print(f"    ? {asin}")
    print(f"{'='*60}")
