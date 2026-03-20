from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from common.utils import get_pg_conn

def check_orphans_live():
    """Vérifie s'il y a des nouvelles stations orphelines sur les dernières 24h."""
    sql = """
    SELECT COUNT(*) 
    FROM "VILLO_ANALYTICS"."F_STATION_STATUS" f 
    LEFT JOIN "VILLO_ANALYTICS"."D_STATION" d ON f.station_fk = d.station_pk 
    WHERE f.load_ts > NOW() - INTERVAL '24 hours'
    AND d.station_pk IS NULL;
    """
    
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            count = cur.fetchone()[0]
            
            if count > 0:
                # On soulève une erreur pour faire échouer la tâche et déclencher l'alerte
                raise ValueError(f"DATA QUALITY ALERT: {count} nouvelles stations orphelines détectées !")
            
            logging.info("Data Quality Check: 0 orphelins détectés. Tout est nominal.")

def check_freshness():
    """Vérifie si on a bien reçu des données dans la dernière heure."""
    sql = """
    SELECT COUNT(*) 
    FROM "VILLO_ANALYTICS"."F_STATION_STATUS" 
    WHERE load_ts > NOW() - INTERVAL '1 hour';
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            count = cur.fetchone()[0]
            
            if count == 0:
                raise ValueError("DATA QUALITY ALERT [FRAÎCHEUR] : 0 ligne insérée depuis 1 heure. Le pipeline est bloqué ou l'API est tombée !")
            
            logging.info(f"Test Fraîcheur OK : {count} lignes reçues dans la dernière heure.")


def check_volume():
    """Vérifie si le dernier batch contient un nombre normal de stations."""
    # On regarde les 15 dernières minutes (pour couvrir le run de 10 min)
    sql = """
    SELECT COUNT(*) 
    FROM "VILLO_ANALYTICS"."F_STATION_STATUS" 
    WHERE load_ts > NOW() - INTERVAL '15 minutes';
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            count = cur.fetchone()[0]
            
            if count > 0 and count < 340: 
                raise ValueError(f"DATA QUALITY ALERT [VOLUME] : Seulement {count} lignes reçues au dernier batch. Il manque des stations !")
            
            logging.info(f"Test Volume OK : {count} lignes reçues au dernier batch.")
default_args = {
    'owner': 'villo_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'email': ['testdata109@gmail.com'], 
    'email_on_failure': True,           
    'retries': 0,
}

with DAG(
    'villo_data_quality_audit',
    default_args=default_args,
    description='Audit quotidien de la qualité des données Villo',
    schedule='@daily', 
    catchup=False
) as dag:

    task_check_orphans = PythonOperator(
        task_id='check_orphans_live',
        python_callable=check_orphans_live,
    )

    task_check_freshness = PythonOperator(
        task_id='check_freshness',
        python_callable=check_freshness,
    )

    task_check_volume = PythonOperator(
        task_id='check_volume',
        python_callable=check_volume,
    )

    # Ordre d'exécution : on lance les 3 tests en parallèle
    [task_check_orphans, task_check_freshness, task_check_volume]