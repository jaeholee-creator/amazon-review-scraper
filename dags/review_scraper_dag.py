"""
Review Scraper DAG - 5개 플랫폼 리뷰 수집 통합 DAG

플랫폼: Amazon US, Amazon UK, Biodance, Shopee (SG/PH), TikTok Shop
스케줄: 매일 KST 09:00 (UTC 00:00)
중복 체크: Google Sheets (Single Source of Truth)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# =============================================================================
# 공통 설정
# =============================================================================
SCRAPER_DIR = '/home/ubuntu/scraper'
VENV_ACTIVATE = 'source /home/ubuntu/airflow-venv/bin/activate'
CREDENTIALS_FILE = f'{SCRAPER_DIR}/credentials.json'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=60),
}

# 공통 환경변수 (Airflow Variables 또는 .env에서 로드)
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
    dag_id='review_scraper',
    default_args=default_args,
    description='5개 플랫폼 리뷰 수집 (Amazon US/UK, Biodance, Shopee, TikTok)',
    schedule='0 0 * * *',  # 매일 UTC 00:00 (KST 09:00)
    start_date=datetime(2026, 2, 11),
    catchup=False,
    max_active_runs=1,
    tags=['scraper', 'reviews', 'biodance'],
) as dag:

    # =========================================================================
    # Task 1: Amazon US 리뷰 수집
    # =========================================================================
    amazon_us = BashOperator(
        task_id='amazon_us_reviews',
        bash_command=_build_bash_command('daily_scraper.py', '--region us'),
        env={**common_env},
        append_env=True,
        execution_timeout=timedelta(minutes=60),
        retries=2,
    )

    # =========================================================================
    # Task 2: Amazon UK 리뷰 수집
    # =========================================================================
    amazon_uk = BashOperator(
        task_id='amazon_uk_reviews',
        bash_command=_build_bash_command('daily_scraper.py', '--region uk'),
        env={**common_env},
        append_env=True,
        execution_timeout=timedelta(minutes=60),
        retries=2,
    )

    # =========================================================================
    # Task 3: Biodance 자사몰 리뷰 수집
    # =========================================================================
    biodance = BashOperator(
        task_id='biodance_reviews',
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
    # Task 4: Shopee 리뷰 수집 (SG + PH)
    # =========================================================================
    shopee = BashOperator(
        task_id='shopee_reviews',
        bash_command=_build_bash_command('shopee_daily_scraper.py'),
        env={**common_env},
        append_env=True,
        execution_timeout=timedelta(minutes=30),
        retries=2,
    )

    # =========================================================================
    # Task 5: TikTok Shop 리뷰 수집
    # =========================================================================
    # TEMPORARILY DISABLED: Rate limit 차단으로 인해 일시 중지 (2026-02-12)
    # 해결 방법:
    #   1. 24-48시간 대기 후 Rate Limit 해제 확인
    #   2. 프록시 서버 사용 고려
    #   3. User-Agent 변경 및 요청 간격 증가
    # 재활성화 시: 아래 주석 해제
    # tiktok = BashOperator(
    #     task_id='tiktok_reviews',
    #     bash_command=_build_bash_command('tiktok_daily_scraper.py'),
    #     env={
    #         **common_env,
    #         'TIKTOK_HEADLESS': 'true',
    #     },
    #     append_env=True,
    #     execution_timeout=timedelta(minutes=30),
    #     retries=2,
    # )

    # =========================================================================
    # Task 6: 완료 알림
    # =========================================================================
    notify_completion = BashOperator(
        task_id='notify_completion',
        bash_command='echo "All review scraping tasks completed at $(date)"',
        trigger_rule='all_done',  # 일부 실패해도 실행
    )

    # =========================================================================
    # Task 의존성 (병렬 실행 → 완료 알림)
    # =========================================================================
    # Amazon US/UK, Biodance, Shopee → 병렬 실행
    # TikTok은 임시 비활성화 (Rate Limit)
    # 전부 끝나면 → notify_completion
    [amazon_us, amazon_uk, biodance, shopee] >> notify_completion
