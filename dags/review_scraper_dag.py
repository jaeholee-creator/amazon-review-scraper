"""
Review Scraper DAG - 5개 플랫폼 리뷰 수집 통합 DAG

플랫폼: Amazon US, Amazon UK, Biodance, Shopee (SG/PH), TikTok Shop
스케줄: 매일 KST 09:00 (UTC 00:00)
중복 체크: Google Sheets (Single Source of Truth)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

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
    description='5개 플랫폼 리뷰 수집 (Amazon US/UK, Biodance, Shopee, TikTok) + TikTok 세션 유지',
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
    # Task 4: Shopee 리뷰 수집 (SG + PH) - Python 스크래퍼
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
    # Task 5: TikTok 세션 Heartbeat (쿠키 갱신) - 스크래핑 전 세션 상태 확인
    #
    # 세션이 유효하면 쿠키를 갱신하고 성공 종료.
    # 세션이 만료됐으면 exit(1) → Airflow가 실패로 기록하지만 tiktok_reviews는
    # 내부에서 재로그인을 시도하므로 trigger_rule 기본값 유지.
    # =========================================================================
    tiktok_heartbeat = BashOperator(
        task_id='tiktok_session_heartbeat',
        bash_command=_build_bash_command('scripts/session_heartbeat.py'),
        env={
            **common_env,
            'DISPLAY': ':99',
            'TIKTOK_DATA_DIR': 'data/tiktok',
        },
        append_env=True,
        execution_timeout=timedelta(minutes=5),
        retries=1,
    )

    # =========================================================================
    # Task 6: TikTok Shop 리뷰 수집
    #
    # TIKTOK_HEADLESS=false + DISPLAY=:99: Xvfb 가상 디스플레이로 headed 모드 실행.
    # → 캡차 슬라이더를 headed 브라우저로 처리하여 자동화 감지 우회.
    # → 세션 만료 시 재로그인도 headed 모드로 시도.
    # =========================================================================
    tiktok = BashOperator(
        task_id='tiktok_reviews',
        bash_command=_build_bash_command('tiktok_daily_scraper.py'),
        env={
            **common_env,
            'DISPLAY': ':99',
            'TIKTOK_HEADLESS': 'false',
            'TIKTOK_PLATFORM_COUNTRY': 'US',
        },
        append_env=True,
        execution_timeout=timedelta(minutes=30),
        retries=2,
    )

    # =========================================================================
    # Task 7: 완료 알림
    # =========================================================================
    notify_completion = BashOperator(
        task_id='notify_completion',
        bash_command='echo "All review scraping tasks completed at $(date)"',
        trigger_rule='all_done',  # 일부 실패해도 실행
    )

    # =========================================================================
    # Task 의존성
    # =========================================================================
    # Amazon US/UK, Biodance, Shopee → 병렬 실행
    # TikTok: heartbeat(세션 갱신) → 리뷰 수집
    # 모든 플랫폼 완료 → 알림
    tiktok_heartbeat >> tiktok
    [amazon_us, amazon_uk, biodance, shopee, tiktok] >> notify_completion
