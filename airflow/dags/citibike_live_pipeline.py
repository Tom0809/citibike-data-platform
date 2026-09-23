from datetime import datetime, timezone
import json

import boto3
import requests

from airflow.sdk import DAG, task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


# ============================================================
# Configuration
# ============================================================

S3_BUCKET = "tombucket2026"

STATION_STATUS_URL = (
    "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
)

STATION_INFORMATION_URL = (
    "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
)


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="citibike_live_pipeline",
    start_date=datetime(2026, 9, 22),
    schedule="*/15 * * * *",   # every 15 minutes
    catchup=False,
    max_active_runs=1,
    tags=["citibike", "live", "aws", "databricks"],
) as dag:

    # --------------------------------------------------------
    # Task 1: Fetch station status -> S3
    # --------------------------------------------------------

    @task
    def ingest_station_status():

        response = requests.get(
            STATION_STATUS_URL,
            timeout=30,
        )
        response.raise_for_status()

        payload = response.json()

        timestamp = datetime.now(timezone.utc)

        year = timestamp.strftime("%Y")
        month = timestamp.strftime("%m")
        day = timestamp.strftime("%d")
        ts = timestamp.strftime("%Y%m%dT%H%M%SZ")

        s3_key = (
            f"raw/station_status/"
            f"year={year}/"
            f"month={month}/"
            f"day={day}/"
            f"station_status_{ts}.json"
        )

        s3 = boto3.client("s3")

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(payload),
            ContentType="application/json",
        )

        print(f"Uploaded to s3://{S3_BUCKET}/{s3_key}")

        return s3_key


    # --------------------------------------------------------
    # Task 2: Fetch station information -> S3
    # --------------------------------------------------------

    @task
    def ingest_station_information():

        response = requests.get(
            STATION_INFORMATION_URL,
            timeout=30,
        )
        response.raise_for_status()

        payload = response.json()

        timestamp = datetime.now(timezone.utc)

        year = timestamp.strftime("%Y")
        month = timestamp.strftime("%m")
        day = timestamp.strftime("%d")
        ts = timestamp.strftime("%Y%m%dT%H%M%SZ")

        s3_key = (
            f"raw/station_information/"
            f"year={year}/"
            f"month={month}/"
            f"day={day}/"
            f"station_information_{ts}.json"
        )

        s3 = boto3.client("s3")

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(payload),
            ContentType="application/json",
        )

        print(f"Uploaded to s3://{S3_BUCKET}/{s3_key}")

        return s3_key


    # --------------------------------------------------------
    # Task 3: Trigger existing Databricks Job
    # --------------------------------------------------------

    trigger_databricks = DatabricksRunNowOperator(
        task_id="trigger_databricks_live_pipeline",
        databricks_conn_id="databricks_default",

        # Replace with your real Databricks Job ID
        job_id=693748717852868,
    )


    # --------------------------------------------------------
    # Dependencies
    # --------------------------------------------------------

    status_task = ingest_station_status()
    information_task = ingest_station_information()

    [status_task, information_task] >> trigger_databricks