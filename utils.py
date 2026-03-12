import os
import json
import logging
import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DATABASE", "stage_micromobility"),
        user=os.getenv("PG_USER", "tan"),
        password=os.getenv("PG_PASS", "")
    )

def ingest_raw_data(target_table, url):
    try:
        logging.info(f"Fetching data for {target_table}...")
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        payload = r.json()

        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                sql = f'INSERT INTO "VILLO_RAW"."{target_table}" (RAW, SOURCE_URL) VALUES (%s, %s)'
                cur.execute(sql, (json.dumps(payload), url))
            conn.commit()
        logging.info(f"Success: {target_table} updated.")
    except Exception as e:
        logging.error(f"Error ingesting {target_table}: {e}")