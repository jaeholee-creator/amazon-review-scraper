import asyncio
import json
import sys
import os
import csv
import time
from datetime import datetime

from src.auth import AmazonAuth
from src.scraper import DailyReviewScraper
from src.slack_notifier import SlackNotifier
from config.settings import (
    SCRAPER_STATE_FILE,
    get_collection_date_range,
    get_collection_date_range_str,
    get_run_date_str,
)

PRODUCTS_CSV = 'config/products.csv'


def load_products_from_csv():
    products = {}
    with open(PRODUCTS_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            asin = row['asin']
            products[asin] = row['name']
    return products


def load_state() -> dict:
    if os.path.exists(SCRAPER_STATE_FILE):
        with open(SCRAPER_STATE_FILE, 'r') as f:
            return json.load(f)
    return {'collected_review_ids': [], 'last_run_date': None}


def save_state(state: dict):
    os.makedirs(os.path.dirname(SCRAPER_STATE_FILE), exist_ok=True)
    with open(SCRAPER_STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


async def scrape_single_product(asin: str, product_name: str, start_date, end_date, test_mode: bool, collected_ids: set):
    auth = None
    try:
        auth = AmazonAuth()
        context = await auth.login_and_get_context()

        scraper = DailyReviewScraper(
            browser_context=context,
            asin=asin,
            date_start=start_date,
            date_end=end_date,
            test_mode=test_mode,
            collected_ids=collected_ids
        )

        reviews, status, error_message = await scraper.scrape_reviews()

        return {
            'asin': asin,
            'product_name': product_name,
            'reviews': reviews,
            'review_count': len(reviews),
            'status': status,
            'error_message': error_message,
            'new_review_ids': scraper.new_review_ids
        }

    except Exception as e:
        return {
            'asin': asin,
            'product_name': product_name,
            'reviews': [],
            'review_count': 0,
            'status': DailyReviewScraper.STATUS_FAILED,
            'error_message': str(e),
            'new_review_ids': set()
        }

    finally:
        if auth:
            await auth.close()


def save_reviews_to_file(reviews: list, output_path: str, product_name: str, append: bool = False):
    if not reviews:
        return

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    fieldnames = [
        'asin', 'review_id', 'rating', 'title', 'author', 'date',
        'date_parsed', 'location', 'verified_purchase', 'content',
        'helpful_count', 'image_count', 'scraped_at'
    ]

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for review in reviews:
            row = {k: review.get(k, '') for k in fieldnames}
            if row.get('date_parsed'):
                row['date_parsed'] = row['date_parsed'].isoformat() if hasattr(row['date_parsed'], 'isoformat') else str(row['date_parsed'])
            writer.writerow(row)


async def main():
    test_mode = '--test' in sys.argv

    products = load_products_from_csv()
    asin_list = list(products.keys())
    
    start_date, end_date = get_collection_date_range()
    date_range_str = get_collection_date_range_str()
    run_date = get_run_date_str()

    state = load_state()
    collected_ids = set(state.get('collected_review_ids', []))

    print("\n" + "=" * 60)
    print("🚀 BIODANCE Daily Review Scraper")
    print("=" * 60)
    print(f"📅 Collection range: {date_range_str}")
    print(f"📦 Products: {len(asin_list)}")
    print(f"🔑 Known review IDs: {len(collected_ids)}")
    print(f"⚙️  Mode: {'TEST (10 pages max)' if test_mode else 'FULL'}")
    print("=" * 60 + "\n")

    start_time = time.time()
    results = []
    failed_asins = []
    all_new_ids = set()

    for i, asin in enumerate(asin_list, 1):
        product_name = products.get(asin, asin)

        print("\n" + "-" * 60)
        print(f"[{i}/{len(asin_list)}] 📦 {product_name}")
        print(f"         ASIN: {asin}")
        print("-" * 60)

        result = await scrape_single_product(asin, product_name, start_date, end_date, test_mode, collected_ids)
        results.append(result)

        new_ids = result.get('new_review_ids', set())
        all_new_ids.update(new_ids)
        collected_ids.update(new_ids)

        if result['status'] != DailyReviewScraper.STATUS_SUCCESS:
            failed_asins.append(asin)

        if result['reviews']:
            output_dir = f'data/daily/{run_date}'
            output_file = f'{output_dir}/all_reviews.csv'
            is_first_product = (i == 1)
            save_reviews_to_file(result['reviews'], output_file, product_name, append=not is_first_product)
            print(f"💾 Appended {len(result['reviews'])} reviews to: {output_file}")

        if i < len(asin_list):
            print("\n⏳ Waiting 5 seconds before next product...")
            await asyncio.sleep(5)

    if failed_asins:
        print("\n" + "=" * 60)
        print("🔄 RETRY: Retrying failed products (1 attempt)")
        print("=" * 60)

        for asin in failed_asins:
            product_name = products.get(asin, asin)

            print(f"\n🔄 Retrying: {product_name} ({asin})")

            result = await scrape_single_product(asin, product_name, start_date, end_date, test_mode, collected_ids)

            for idx, r in enumerate(results):
                if r['asin'] == asin:
                    results[idx] = result
                    break

            new_ids = result.get('new_review_ids', set())
            all_new_ids.update(new_ids)
            collected_ids.update(new_ids)

            if result['reviews']:
                output_dir = f'data/daily/{run_date}'
                output_file = f'{output_dir}/all_reviews.csv'
                save_reviews_to_file(result['reviews'], output_file, product_name, append=True)
                print(f"💾 Appended {len(result['reviews'])} reviews to: {output_file}")

            if result['status'] == DailyReviewScraper.STATUS_FAILED:
                print(f"\n❌ Retry failed for {product_name}. Stopping retries.")
                break

    elapsed = time.time() - start_time

    state['collected_review_ids'] = list(collected_ids)
    state['last_run_date'] = run_date
    save_state(state)
    print(f"\n💾 State saved: {len(all_new_ids)} new IDs added (total: {len(collected_ids)})")

    print("\n" + "=" * 60)
    print("📊 FINAL SUMMARY")
    print("=" * 60)

    total_reviews = sum(r['review_count'] for r in results)
    success_count = sum(1 for r in results if r['status'] == 'success')
    partial_count = sum(1 for r in results if r['status'] == 'partial')
    failed_count = sum(1 for r in results if r['status'] == 'failed')

    print(f"📅 Range: {date_range_str}")
    print(f"📝 Total reviews collected: {total_reviews}")
    print(f"⏱️  Total time: {elapsed:.1f}s")
    print(f"✅ Success: {success_count} | ⚠️ Partial: {partial_count} | ❌ Failed: {failed_count}")
    print()

    for r in results:
        status_icon = '✅' if r['status'] == 'success' else '⚠️' if r['status'] == 'partial' else '❌'
        error_str = f" ({r['error_message']})" if r.get('error_message') else ""
        print(f"  {status_icon} {r['product_name']}: {r['review_count']}개{error_str}")

    print("\n" + "=" * 60)

    print("\n📤 Sending Slack notification...")
    slack = SlackNotifier()
    sent = slack.send_daily_scrape_report(date_range_str, results, elapsed)

    if sent:
        print("✅ Slack notification sent!")
    else:
        print("❌ Failed to send Slack notification")

    print("\n👋 Done!")


if __name__ == '__main__':
    asyncio.run(main())
