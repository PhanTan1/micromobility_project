from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# --- 1. Path Fix ---
# This ensures Python looks in the current folder for your 'scripts'
dag_dir = os.path.dirname(os.path.abspath(__file__))
if dag_dir not in sys.path:
    sys.path.insert(0, dag_dir)

# --- 2. Direct Imports ---
# If these fail, Airflow will now show you EXACTLY why (e.g., file not found)
from scripts.ingest_villo_station_status import run_ingestion as ingest_status
from scripts.ingest_villo_station_information import run_ingestion as ingest_info
from scripts.transform_villo_station_status import run_villo_status_transformation
from scripts.transform_villo_station_information import run_station_info_transformation
from scripts.analytics_villo_refresh import run_analytics_refresh

default_args = {
    'owner': 'tan',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'villo_medallion_pipeline',
    default_args=default_args,
    description='Full Medallion Pipeline: Raw -> Staging -> Analytics',
    schedule=timedelta(minutes=10), # Fixed keyword for Airflow 2.4+
    catchup=False,
    tags=['villo', 'micromobility'],
) as dag:

    t1_ingest_status = PythonOperator(
        task_id='ingest_status_raw',
        python_callable=ingest_status,
    )

    t1_ingest_info = PythonOperator(
        task_id='ingest_info_raw',
        python_callable=ingest_info,
    )

    t2_transform_status = PythonOperator(
        task_id='transform_status_to_staging',
        python_callable=run_villo_status_transformation,
    )

    t2_transform_info = PythonOperator(
        task_id='transform_info_to_staging',
        python_callable=run_station_info_transformation,
    )

    t3_analytics_refresh = PythonOperator(
        task_id='refresh_analytics_gold',
        python_callable=run_analytics_refresh,
    )

    # Dependencies
    [t1_ingest_status, t1_ingest_info] >> t2_transform_status
    t2_transform_status >> t2_transform_info >> t3_analytics_refresh
    # Note: I adjusted the chain slightly to ensure status moves before info refresh