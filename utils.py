import os
import json
import logging
import psycopg2
import psycopg2.extras  # Required for execute_values
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_pg_conn():
    """Returns a connection to the PostgreSQL database."""
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DATABASE", "stage_micromobility"),
        user=os.getenv("PG_USER", "tan"),
        password=os.getenv("PG_PASS", "")
    )

def ingest_raw_villo_data(target_table, url):
    """Fetches JSON from Villo API and inserts it into the RAW schema."""
    try:
        logging.info(f"Requesting data from: {url}")
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        payload = r.json()

        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                sql = f'INSERT INTO "VILLO_RAW"."{target_table}" (RAW, SOURCE_URL) VALUES (%s, %s)'
                cur.execute(sql, (json.dumps(payload), url))
            conn.commit()
        logging.info(f"Successfully updated VILLO_RAW.{target_table}")
    except Exception as e:
        logging.error(f"Failed to ingest {target_table}: {e}")

def batch_insert_staging(target_table, columns, rows):
    """
    Handles high-performance batch inserts into the staging layer.
    Uses psycopg2.extras.execute_values for efficiency.
    """
    if not rows:
        logging.warning(f"No rows provided for {target_table}. Skipping.")
        return

    col_str = ", ".join(columns)
    # Note: We use %s as a placeholder for the entire list of tuples
    query = f'INSERT INTO "VILLO_STAGING"."{target_table}" ({col_str}) VALUES %s'

    try:
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, query, rows)
            conn.commit()
        logging.info(f"Successfully batch inserted {len(rows)} rows into VILLO_STAGING.{target_table}")
    except Exception as e:
        logging.error(f"Batch insert failed for {target_table}: {e}")