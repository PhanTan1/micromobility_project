from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from scripts.analytics_villo_refresh import refresh_dimensions
from common.utils import get_pg_conn

with DAG(
    dag_id='villo_dimension_daily',
    start_date=datetime(2026, 3, 18),
    schedule='5 0 * * *', 
    catchup=False,
    tags=['villo', 'dimensions']
) as dag:

    # 1. Ingest Task
    ingest_info = BashOperator(
        task_id='ingest_station_info',
        bash_command='export PYTHONPATH=/opt/airflow/dags && python /opt/airflow/dags/scripts/ingest_villo_station_information.py'
    )

    # 2. Transform Task (CHECK: Name must be 'transform_info')
    transform_info = BashOperator(
        task_id='transform_info_to_staging',
        bash_command='export PYTHONPATH=/opt/airflow/dags && python /opt/airflow/dags/scripts/transform_villo_station_information.py'
    )

    # 3. Analytics Load Task
    load_gold_dimensions = PythonOperator(
        task_id='refresh_analytics_dimensions',
        python_callable=refresh_dimensions
    )

    # 4. Define Dependencies (Variables here must match definitions above)
    ingest_info >> transform_info >> load_gold_dimensions