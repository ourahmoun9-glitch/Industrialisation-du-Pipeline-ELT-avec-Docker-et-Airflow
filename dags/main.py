from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import pendulum

from datetime import datetime, timedelta

from videos_status import (
    get_playlist_id,
    get_video_ids,
    extract_video_details,
    save_to_json,
)

from datawarehouse.dwh import staging_table, core_table


# Morocco timezone
local_tz = pendulum.timezone("Africa/Casablanca")


# Default arguments
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email": "data@engineers.com",
    "email_on_failure": False,
    "email_on_retry": False,
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    # "end_date": datetime(2030, 12, 31, tzinfo=local_tz),
}


# ─────────────────────────────────────────────
# DAG 1 : Produce JSON
# Extracts raw YouTube data and saves it as JSON
# ─────────────────────────────────────────────
with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw YouTube data",
    schedule="0 14 * * *",  # Every day at 14:00 Morocco time
    catchup=False,
    tags=["youtube", "etl", "json"],
) as dag_produce:

    # Tasks
    playlist_id = get_playlist_id()

    video_ids = get_video_ids(playlist_id)

    extract_data = extract_video_details(video_ids)

    save_to_json_task = save_to_json(extract_data)

    # Dependencies
    playlist_id >> video_ids >> extract_data >> save_to_json_task


# ─────────────────────────────────────────────
# DAG 2 : Load to Data Warehouse
# Loads JSON → staging (raw) → core (transformed)
# Triggered automatically after produce_json finishes
# ─────────────────────────────────────────────
with DAG(
    dag_id="load_to_dwh",
    default_args=default_args,
    description="DAG to load YouTube data into the PostgreSQL Data Warehouse (staging → core)",
    schedule=None,  # Triggered by produce_json, not on a schedule
    catchup=False,
    tags=["youtube", "etl", "dwh", "postgres"],
) as dag_dwh:

    # Task 1 : Load raw data into staging layer
    staging_task = staging_table()

    # Task 2 : Transform and load into core layer
    core_task = core_table()

    # Dependencies : staging must finish before core
    staging_task >> core_task


# ─────────────────────────────────────────────
# Chain DAG 1 → DAG 2 via TriggerDagRunOperator
# Placed inside dag_produce so it runs as its last step
# ─────────────────────────────────────────────
with dag_produce:

    trigger_dwh = TriggerDagRunOperator(
        task_id="trigger_load_to_dwh",
        trigger_dag_id="load_to_dwh",
        wait_for_completion=False,  # Fire-and-forget; set True to wait & propagate failures
    )

    # save_to_json finishes → trigger the DWH DAG
    save_to_json_task >> trigger_dwh