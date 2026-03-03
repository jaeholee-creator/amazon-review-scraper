"""
Review Scraper DAG - TikTok 전용 (테스트/운영)

현재 상태: TikTok만 실행. Amazon/Shopee/Biodance는 TikTok 안정화 후 재추가 예정.

TikTok 흐름:
  xvfb_ensure → tiktok_reviews

  - tiktok_heartbeat 제거: heartbeat가 rating 페이지를 방문하면 guard token
    rate-limit으로 이후 scraper가 실패함. scraper 내부 _ensure_logged_in이
    세션 확인 역할을 대신함.
  - profile_cleanup 제거: tiktok_reviews 태스크 내부에서 처리.
  - retries=0: 테스트 중 실패 원인을 즉시 확인하기 위함.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# =============================================================================
# 공통 설정
# =============================================================================
SCRAPER_DIR = '/home/ubuntu/scraper'
VENV_ACTIVATE = f'source /home/ubuntu/airflow-venv/bin/activate'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,          # 테스트 중: 실패 즉시 확인
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=60),
}

common_env = {
    'HEADLESS': 'true',
    'PYTHONPATH': SCRAPER_DIR,
}

# =============================================================================
# DAG 정의
# =============================================================================
with DAG(
    dag_id='review_scraper',
    default_args=default_args,
    description='TikTok 리뷰 수집 (테스트 중 - 다른 플랫폼 비활성화)',
    schedule='0 0 * * *',   # 매일 UTC 00:00 (KST 09:00)
    start_date=datetime(2026, 2, 11),
    catchup=False,
    max_active_runs=1,
    tags=['scraper', 'reviews', 'tiktok'],
) as dag:

    # =========================================================================
    # Task 1: Xvfb 초기화
    #
    # TikTok 스크래퍼는 headed 모드(DISPLAY=:99)로 실행.
    # 매 DAG 실행 시 Xvfb를 새로 시작해 브라우저 환경 일관성 보장.
    # =========================================================================
    xvfb_ensure = BashOperator(
        task_id='xvfb_ensure',
        bash_command=(
            'pkill -9 Xvfb || true; '
            'sleep 1; '
            'setsid Xvfb :99 -screen 0 1920x1080x24 -ac +extension GLX +render -noreset'
            ' </dev/null >/dev/null 2>&1 & '
            'sleep 2; '
            'DISPLAY=:99 xdpyinfo >/dev/null 2>&1'
            ' && echo "Xvfb ready at DISPLAY=:99"'
            ' || { echo "ERROR: Xvfb failed"; exit 1; }'
        ),
        execution_timeout=timedelta(minutes=2),
        retries=1,
    )

    # =========================================================================
    # Task 2: TikTok 리뷰 수집
    #
    # - Chrome 잔여 프로세스 및 잠금 파일 정리
    # - tiktok_daily_scraper.py 실행
    #
    # 세션 관리:
    # - browser_profile은 유지 (guard token이 IndexedDB에 저장되어 있음)
    # - 잠금 파일(Singleton*)만 제거: Chrome 강제종료 후 잔재 처리
    # - browser_profile 전체 삭제 금지: guard token이 삭제되면 rating 페이지에서
    #   ttp_session_expire=1 발생 (guard token과 session cookie가 서버에서 연결됨)
    # - _ensure_logged_in()이 내부에서 homepage warmup + rating 접근으로 세션 확인
    # - 세션 만료 시 Slack 알림 후 exit(1) → DAG 실패로 기록
    # =========================================================================
    tiktok_reviews = BashOperator(
        task_id='tiktok_reviews',
        bash_command=(
            # 1) 잔여 Chromium 프로세스 정리
            # 주의: pkill -f 패턴에 "chrome"/"chromium" 사용 시 bash 프로세스 자체가
            # cmdline에 해당 문자열을 포함하므로 자기 자신을 죽임 → 프로세스명으로만 매칭
            'pkill -9 chromium 2>/dev/null || true; '
            'sleep 2; '
            # 2) 잠금 파일만 제거 (browser_profile 자체는 유지 - guard token 보존)
            'find /home/ubuntu/scraper/data/tiktok/ -name "*.lock" -delete 2>/dev/null || true; '
            'find /home/ubuntu/scraper/data/tiktok/ -name "Singleton*" -delete 2>/dev/null || true; '
            # 3) 스크래퍼 실행
            f'{VENV_ACTIVATE} && cd {SCRAPER_DIR} && python tiktok_daily_scraper.py'
        ),
        env={
            **common_env,
            'DISPLAY': ':99',
            'TIKTOK_HEADLESS': 'false',
            'TIKTOK_PLATFORM_COUNTRY': 'US',
            'TIKTOK_DATA_DIR': 'data/tiktok',
        },
        append_env=True,
        execution_timeout=timedelta(minutes=30),
        retries=0,   # 테스트 중: 실패 즉시 확인 (운영 시 2로 변경)
    )

    # =========================================================================
    # Task 의존성
    # =========================================================================
    xvfb_ensure >> tiktok_reviews
