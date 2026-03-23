"""
CS 리뷰 리포트 DAG - 주간/월간 CS 리뷰 리포트 자동 발행

플로우:
  extract_data → analyze_with_claude → [publish_to_notion || notify_slack]

스케줄:
  주간: 매주 월요일 09:00 KST (UTC 00:00)
  월간: 매월 1일 09:00 KST (UTC 00:00)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# =============================================================================
# 공통 설정
# =============================================================================
CS_REPORTER_DIR = "/home/ubuntu/cs-review-reporter"
VENV_ACTIVATE = "source /home/ubuntu/cs-review-reporter-venv/bin/activate"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _cmd(period: str, stage: str) -> str:
    """venv 활성화 → 디렉토리 이동 → main.py 실행"""
    return (
        f"{VENV_ACTIVATE} && "
        f"cd {CS_REPORTER_DIR} && "
        f"set -a && source .env && set +a && "
        f"python main.py --period {period} --stage {stage}"
    )


# =============================================================================
# 주간 리포트 DAG
# =============================================================================
with DAG(
    dag_id="cs_review_reporter_weekly",
    default_args=default_args,
    description="CS 리뷰 주간 리포트 (BigQuery → Claude 분석 → Notion/Slack 병렬 발행)",
    schedule="0 0 * * 1",  # 매주 월요일 UTC 00:00 = KST 09:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["cs-review", "weekly", "notion", "slack", "bigquery"],
) as weekly_dag:

    weekly_extract = BashOperator(
        task_id="extract_data",
        bash_command=_cmd("weekly", "extract"),
        execution_timeout=timedelta(minutes=10),
        retries=2,
    )

    weekly_analyze = BashOperator(
        task_id="analyze_with_claude",
        bash_command=_cmd("weekly", "analyze"),
        execution_timeout=timedelta(minutes=45),
        retries=1,
    )

    weekly_notion = BashOperator(
        task_id="publish_to_notion",
        bash_command=_cmd("weekly", "notion"),
        execution_timeout=timedelta(minutes=10),
        retries=2,
    )

    weekly_slack = BashOperator(
        task_id="notify_slack",
        bash_command=_cmd("weekly", "slack"),
        execution_timeout=timedelta(minutes=5),
        retries=2,
    )

    weekly_extract >> weekly_analyze >> [weekly_notion, weekly_slack]


# =============================================================================
# 월간 리포트 DAG
# =============================================================================
with DAG(
    dag_id="cs_review_reporter_monthly",
    default_args=default_args,
    description="CS 리뷰 월간 리포트 (BigQuery → Claude 분석 → Notion/Slack 병렬 발행)",
    schedule="0 0 1 * *",  # 매월 1일 UTC 00:00 = KST 09:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["cs-review", "monthly", "notion", "slack", "bigquery"],
) as monthly_dag:

    monthly_extract = BashOperator(
        task_id="extract_data",
        bash_command=_cmd("monthly", "extract"),
        execution_timeout=timedelta(minutes=15),
        retries=2,
    )

    monthly_analyze = BashOperator(
        task_id="analyze_with_claude",
        bash_command=_cmd("monthly", "analyze"),
        execution_timeout=timedelta(minutes=60),
        retries=1,
    )

    monthly_notion = BashOperator(
        task_id="publish_to_notion",
        bash_command=_cmd("monthly", "notion"),
        execution_timeout=timedelta(minutes=10),
        retries=2,
    )

    monthly_slack = BashOperator(
        task_id="notify_slack",
        bash_command=_cmd("monthly", "slack"),
        execution_timeout=timedelta(minutes=5),
        retries=2,
    )

    monthly_extract >> monthly_analyze >> [monthly_notion, monthly_slack]
