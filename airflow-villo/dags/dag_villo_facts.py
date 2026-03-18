from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'scripts')))
from analytics_villo_refresh import refresh_facts

with DAG(
    dag_id='villo_facts_10min',
    start_date=datetime(2026, 3, 18),
    schedule='*/10 * * * *', # Exécution toutes les 10 minutes
    catchup=False,
    tags=['villo', 'facts']
) as dag:

    ingest_status = BashOperator(
        task_id='ingest_station_status',
        # Ajout de l'export PYTHONPATH pour trouver utils.py
        bash_command='export PYTHONPATH=/opt/airflow/dags && python /opt/airflow/dags/scripts/ingest_villo_station_status.py'
    )

    transform_status = BashOperator(
        task_id='transform_status_to_staging',
        # Corrected filename: transform_villo_station_status.py
        bash_command='export PYTHONPATH=/opt/airflow/dags && python /opt/airflow/dags/scripts/transform_villo_station_status.py'
    )

    load_gold_facts = PythonOperator(
        task_id='refresh_analytics_facts',
        python_callable=refresh_facts
    )

    # Ordre d'exécution des tâches
    ingest_status >> transform_status >> load_gold_facts