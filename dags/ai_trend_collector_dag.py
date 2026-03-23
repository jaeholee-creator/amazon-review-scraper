"""
AI Trend Collector DAG - AI/테크 트렌드 수집 및 Notion 발행

소스 프로젝트: https://github.com/jaeholee-creator/ai-trend-collector
소스: Twitter/X, Reddit, GitHub, StackOverflow, HackerNews,
      한국 커뮤니티, News RSS, HuggingFace, MCP.so
스케줄: 매일 KST 09:00 (UTC 00:00)
발행: Notion "📊 데일리 리포트" + "신규 배포" 페이지

[통합 변경 이력]
- GitHub Actions 스케줄 제거 → Airflow 단일 스케줄러로 통합
- 로그 파일 저장 추가 (GitHub Actions artifact 기능 대체)
- 실패 시 Slack 알림 추가 (SLACK_WEBHOOK_URL 환경변수 필요)
- collect 타임아웃 30분 → 60분 확장 (GitHub Actions timeout 기준)
- publish 타임아웃 10분 → 20분 확장
"""

import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

KST = ZoneInfo("Asia/Seoul")

# =============================================================================
# 공통 설정
# =============================================================================
TREND_COLLECTOR_DIR = '/home/ubuntu/ai-trend-collector'
VENV_ACTIVATE = 'source /home/ubuntu/trend-collector-venv/bin/activate'
LOG_DIR = f'{TREND_COLLECTOR_DIR}/output'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

common_env = {
    'PYTHONPATH': TREND_COLLECTOR_DIR,
}


def _build_bash_command(script: str, args: str = '', log_tag: str = '') -> str:
    base = f'{VENV_ACTIVATE} && cd {TREND_COLLECTOR_DIR} && python -u {script} {args}'.strip()
    if log_tag:
        ts = '$(date +%Y%m%d_%H%M%S)'
        log_file = f'{LOG_DIR}/{log_tag}_{ts}.log'
        return f'set -o pipefail && mkdir -p {LOG_DIR} && {{ {base} 2>&1 | tee {log_file}; }}'
    return base


def _send_slack(message: str) -> None:
    webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    if not webhook_url:
        return
    try:
        import requests
        requests.post(webhook_url, json={'text': message}, timeout=10)
    except Exception as e:
        print(f'[Slack] 알림 전송 실패: {e}')


def _notify_success(**context):
    exec_date = context.get('execution_date', datetime.now(KST))
    date_str = exec_date.strftime('%Y-%m-%d') if hasattr(exec_date, 'strftime') else str(exec_date)[:10]
    now_kst = datetime.now(KST)
    print(f'✅ AI Trend Collector 완료 — {date_str} (완료 시각: {now_kst.strftime("%H:%M:%S")} KST)')
    _send_slack(f'✅ *AI Trend Collector* 완료\n• 날짜: {date_str}\n• 완료: {now_kst.strftime("%H:%M")} KST')


def _notify_failure(**context):
    exec_date = context.get('execution_date', datetime.now(KST))
    date_str = exec_date.strftime('%Y-%m-%d') if hasattr(exec_date, 'strftime') else str(exec_date)[:10]

    failed_tasks = []
    try:
        for ti in context['dag_run'].get_task_instances():
            if ti.state == 'failed':
                failed_tasks.append(ti.task_id)
    except Exception:
        pass

    failed_str = ', '.join(failed_tasks) if failed_tasks else '알 수 없음'
    collection_summary = ""
    try:
        import json
        import glob
        meta_files = sorted(glob.glob(f'{TREND_COLLECTOR_DIR}/data/all_trends_*.json'), reverse=True)
        if meta_files:
            with open(meta_files[0], 'r') as f:
                data = json.load(f)
            meta = data.get('_collection_meta', {})
            summary = meta.get('_summary', {})
            if summary:
                collection_summary = (
                    f"\n• 수집 현황: {summary.get('success_count', '?')}/{summary.get('enabled_sources', '?')} 소스 성공"
                )
                failed_sources = [k for k, v in meta.items() if k != '_summary' and isinstance(v, dict) and v.get('status') in ('failed', 'timeout')]
                if failed_sources:
                    collection_summary += f"\n• 실패 소스: {', '.join(failed_sources)}"
    except Exception:
        pass

    now_kst = datetime.now(KST)
    _send_slack(
        f'🚨 *AI Trend Collector 실패*\n'
        f'• 날짜: {date_str}\n'
        f'• 실패 태스크: `{failed_str}`'
        f'{collection_summary}\n'
        f'• Airflow 대시보드에서 로그를 확인하세요'
    )


# =============================================================================
# DAG 정의
# =============================================================================
with DAG(
    dag_id='ai_trend_collector',
    default_args=default_args,
    description='AI/테크 트렌드 수집 (9개 소스) → 스코어링 → Claude 분석 → Notion 발행',
    schedule='0 0 * * *',
    start_date=datetime(2026, 2, 12),
    catchup=False,
    max_active_runs=1,
    tags=['ai-trends', 'notion', 'crawler'],
) as dag:

    collect_trends = BashOperator(
        task_id='collect_trends',
        bash_command=_build_bash_command('main.py', '--stage collect', 'collect'),
        env={**common_env},
        append_env=True,
        execution_timeout=timedelta(minutes=90),
        retries=2,
    )

    publish_to_notion = BashOperator(
        task_id='publish_to_notion',
        bash_command=_build_bash_command('main.py', '--stage publish', 'publish'),
        env={**common_env},
        append_env=True,
        execution_timeout=timedelta(minutes=20),
        retries=2,
    )

    analyze_keywords = BashOperator(
        task_id='analyze_keywords',
        bash_command=_build_bash_command('main.py', '--stage keywords', 'keywords'),
        env={**common_env},
        append_env=True,
        execution_timeout=timedelta(minutes=5),
        retries=1,
    )

    notify_success = PythonOperator(
        task_id='notify_success',
        python_callable=_notify_success,
        trigger_rule='all_success',
    )

    notify_failure = PythonOperator(
        task_id='notify_failure',
        python_callable=_notify_failure,
        trigger_rule='one_failed',
    )

    collect_trends >> publish_to_notion >> analyze_keywords
    analyze_keywords >> [notify_success, notify_failure]
