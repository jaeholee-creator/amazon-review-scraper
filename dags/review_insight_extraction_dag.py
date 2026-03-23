"""
review_insight_extraction_dag.py
- 매일 10:00 KST (01:00 UTC) US 플랫폼 리뷰에서
  미처리 건을 Gemini로 분석 후 BQ review_insights 적재
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.models import Variable

    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

logger = logging.getLogger(__name__)

_default_project_dir = str(Path(__file__).parent.parent)

try:
    PROJECT_DIR = Variable.get(
        "AMZ_PROJECT_DIR",
        default_var=os.environ.get("AMZ_PROJECT_DIR", _default_project_dir),
    )
    BQ_CREDENTIALS_FILE = Variable.get(
        "AMZ_BQ_CREDENTIALS_FILE",
        default_var=os.environ.get(
            "AMZ_BQ_CREDENTIALS_FILE",
            str(Path(_default_project_dir) / "credentials.json"),
        ),
    )
    GEMINI_API_KEY = Variable.get(
        "GEMINI_API_KEY",
        default_var=os.environ.get("GEMINI_API_KEY", ""),
    )
except Exception:
    PROJECT_DIR = os.environ.get("AMZ_PROJECT_DIR", _default_project_dir)
    BQ_CREDENTIALS_FILE = os.environ.get(
        "AMZ_BQ_CREDENTIALS_FILE",
        str(Path(_default_project_dir) / "credentials.json"),
    )
    GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

DEFAULT_ARGS = {
    "owner": "jaeho",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def run_extraction(**context):
    import sys
    import os

    sys.path.insert(0, PROJECT_DIR)

    if not GEMINI_API_KEY:
        raise RuntimeError("Airflow Variable 'GEMINI_API_KEY' 가 설정되지 않았습니다.")

    os.environ["GEMINI_API_KEY"] = GEMINI_API_KEY
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BQ_CREDENTIALS_FILE

    from review_insight_extractor import run
    run()


if AIRFLOW_AVAILABLE:
    dag = DAG(
        dag_id="review_insight_extraction",
        default_args=DEFAULT_ARGS,
        description="Gemini로 US 플랫폼 리뷰 인사이트 추출 → BQ review_insights 적재",
        schedule_interval="0 1 * * *",   # 매일 01:00 UTC = 매일 10:00 KST
        start_date=datetime(2026, 3, 23),
        catchup=False,
        tags=["review", "gemini", "bigquery", "us-marketing"],
    )

    extract_task = PythonOperator(
        task_id="extract_review_insights",
        python_callable=run_extraction,
        dag=dag,
    )
