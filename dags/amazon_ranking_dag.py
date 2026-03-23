"""
Amazon 상품 랭킹 수집 DAG

각 상품 상세 페이지에서 Best Sellers Rank, 고객 평점, 리뷰 수를 매시간 수집합니다.

플랫폼  : Amazon US, Amazon UK
스케줄  : 매시간 정각 (0 * * * *)
수집 항목: BSR 순위, 서브카테고리 순위, 평점, 리뷰 수
저장    : BigQuery member-378109.jaeho.amazon_product_rankings
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

SCRAPER_DIR = "/home/ubuntu/scraper"
VENV_ACTIVATE = "source /home/ubuntu/airflow-venv/bin/activate"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=90),
}

common_env = {
    "HEADLESS": "true",
    "PYTHONPATH": SCRAPER_DIR,
}


def _cmd(region: str) -> str:
    return (
        f"{VENV_ACTIVATE} && cd {SCRAPER_DIR} "
        f"&& python amazon_ranking_scraper.py --region {region}"
    )


with DAG(
    dag_id="amazon_ranking_collector",
    default_args=default_args,
    description="Amazon US/UK 상품 BSR 순위 + 평점 + 리뷰 수 매시간 수집 → BQ",
    schedule="0 * * * *",
    start_date=datetime(2026, 3, 23),
    catchup=False,
    max_active_runs=1,
    tags=["amazon", "ranking", "biodance"],
) as dag:

    us_rankings = BashOperator(
        task_id="amazon_us_rankings",
        bash_command=_cmd("us"),
        env={**common_env},
        append_env=True,
        execution_timeout=timedelta(minutes=45),
        retries=1,
    )

    uk_rankings = BashOperator(
        task_id="amazon_uk_rankings",
        bash_command=_cmd("uk"),
        env={**common_env},
        append_env=True,
        execution_timeout=timedelta(minutes=45),
        retries=1,
    )

    # US 완료 후 UK 수집 (동시 실행 시 IP 차단 방지)
    us_rankings >> uk_rankings
