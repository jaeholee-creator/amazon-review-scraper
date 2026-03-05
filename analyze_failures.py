#!/usr/bin/env python3
"""
Airflow DAG 실패 기록 분석 스크립트
2026-02-15 이후 review_scraper DAG의 실패 기록을 조사합니다.
"""

import requests
import json
from datetime import datetime

# Airflow REST API 엔드포인트
AIRFLOW_API = "http://129.146.108.143:8080/api/v1"

def get_failed_dag_runs(dag_id="review_scraper", start_date="2026-02-15"):
    """실패한 DAG 실행 조회"""

    url = f"{AIRFLOW_API}/dags/{dag_id}/dagRuns"
    params = {
        "state": ["failed", "upstream_failed"],
        "limit": 100,
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ API 호출 실패: {e}")
        return None


def analyze_task_failures(dag_id, dag_run_id):
    """DAG 실행의 실패한 태스크 분석"""

    url = f"{AIRFLOW_API}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        failed_tasks = [
            task for task in data.get("task_instances", [])
            if task.get("state") in ["failed", "upstream_failed"]
        ]

        return failed_tasks
    except Exception as e:
        print(f"❌ 태스크 조회 실패: {e}")
        return []


def main():
    print("\n📊 Review Scraper DAG 실패 기록 분석")
    print("=" * 60)

    # 실패한 DAG 실행 조회
    print("\n🔍 실패한 DAG 실행 조회 중...")
    failed_runs = get_failed_dag_runs()

    if not failed_runs or "dag_runs" not in failed_runs:
        print("❌ 실패 기록을 가져올 수 없습니다.")
        return

    dag_runs = failed_runs.get("dag_runs", [])

    if not dag_runs:
        print("✅ 2026-02-15 이후 실패 기록이 없습니다!")
        return

    print(f"\n📋 발견된 실패: {len(dag_runs)}개\n")

    # 실패 기록 분석
    failure_summary = {}

    for run in dag_runs:
        dag_run_id = run.get("dag_run_id")
        state = run.get("state")
        execution_date = run.get("execution_date")
        start_date = run.get("start_date")
        end_date = run.get("end_date")

        print(f"📌 DAG Run ID: {dag_run_id}")
        print(f"   상태: {state}")
        print(f"   실행 날짜: {execution_date}")
        print(f"   시작: {start_date}")
        print(f"   종료: {end_date}")

        # 실패한 태스크 분석
        failed_tasks = analyze_task_failures("review_scraper", dag_run_id)

        if failed_tasks:
            print(f"   ❌ 실패한 태스크:")
            for task in failed_tasks:
                task_id = task.get("task_id")
                task_state = task.get("state")
                print(f"      - {task_id}: {task_state}")

                # 요약에 추가
                date_key = execution_date.split("T")[0] if execution_date else "unknown"
                if date_key not in failure_summary:
                    failure_summary[date_key] = {}

                if task_id not in failure_summary[date_key]:
                    failure_summary[date_key][task_id] = []
                failure_summary[date_key][task_id].append(task_state)

        print()

    # 요약 출력
    print("\n" + "=" * 60)
    print("📈 실패 기록 요약")
    print("=" * 60)

    for date in sorted(failure_summary.keys()):
        print(f"\n📅 {date}")
        for task_id, states in failure_summary[date].items():
            print(f"   - {task_id}: {', '.join(set(states))}")


if __name__ == "__main__":
    main()
