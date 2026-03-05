"""
Review Scraper Backfill DAG - 누락된 기간 데이터 수집용
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# =============================================================================
# 공통 설정
# =============================================================================
SCRAPER_DIR = '/home/ubuntu/scraper'
VENV_ACTIVATE = 'source /home/ubuntu/airflow-venv/bin/activate'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=60),
}

common_env = {
    'HEADLESS': 'true',
    'PYTHONPATH': SCRAPER_DIR,
}


def _build_bash_command(script: str, args: str = '') -> str:
    """venv 활성화 + 작업 디렉토리 이동 + 스크립트 실행 명령 생성."""
    return f'{VENV_ACTIVATE} && cd {SCRAPER_DIR} && python {script} {args}'.strip()


# =============================================================================
# DAG 정의
# =============================================================================
with DAG(
    dag_id='review_scraper_backfill',
    default_args=default_args,
    description='누락된 기간 리뷰 데이터 백필 (수동 실행용)',
    schedule=None,  # 수동 실행만 가능
    start_date=datetime(2026, 2, 1),
    catchup=False,
    max_active_runs=1,
    tags=['scraper', 'reviews', 'backfill'],
) as dag:

    # =========================================================================
    # Task 1: Amazon US 리뷰 수집 (백필)
    # =========================================================================
    amazon_us = BashOperator(
        task_id='amazon_us_reviews_backfill',
        bash_command=_build_bash_command('daily_scraper.py', '--region us'),
        env={**common_env},
        append_env=True,
        execution_timeout=timedelta(minutes=60),
        retries=2,
    )

    # =========================================================================
    # Task 2: Amazon UK 리뷰 수집 (백필)
    # =========================================================================
    amazon_uk = BashOperator(
        task_id='amazon_uk_reviews_backfill',
        bash_command=_build_bash_command('daily_scraper.py', '--region uk'),
        env={**common_env},
        append_env=True,
        execution_timeout=timedelta(minutes=60),
        retries=2,
    )

    # =========================================================================
    # Task 3: Biodance 자사몰 리뷰 수집 (백필)
    # =========================================================================
    biodance = BashOperator(
        task_id='biodance_reviews_backfill',
        bash_command=_build_bash_command('scrapers/biodance/run_biodance_reviews.py'),
        env={
            **common_env,
            'SAVE_LOCAL_FILES': 'false',
        },
        append_env=True,
        execution_timeout=timedelta(minutes=15),
        retries=2,
    )

    # =========================================================================
    # Task 4: Shopee 리뷰 수집 (백필)
    # =========================================================================
    shopee = BashOperator(
        task_id='shopee_reviews_backfill',
        bash_command=_build_bash_command('shopee_daily_scraper.py'),
        env={**common_env},
        append_env=True,
        execution_timeout=timedelta(minutes=30),
        retries=2,
    )

    # =========================================================================
    # Task 5: TikTok Shop 리뷰 수집 (백필)
    # =========================================================================
    tiktok = BashOperator(
        task_id='tiktok_reviews_backfill',
        bash_command=_build_bash_command('tiktok_daily_scraper.py'),
        env={
            **common_env,
            'TIKTOK_HEADLESS': 'true',
        },
        append_env=True,
        execution_timeout=timedelta(minutes=30),
        retries=2,
    )

    # =========================================================================
    # Task 6: 백필 완료 알림
    # =========================================================================
    notify_completion = BashOperator(
        task_id='notify_backfill_completion',
        bash_command='echo "Backfill review scraping tasks completed at $(date)"',
        trigger_rule='all_done',
    )

    # =========================================================================
    # Task 의존성
    # =========================================================================
    [amazon_us, amazon_uk, biodance, shopee, tiktok] >> notify_completion
