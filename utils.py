import os
import json
import logging
import psycopg2
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging for the entire project
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

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
                # Target table is controlled internally, safe for f-string
                sql = f'INSERT INTO "VILLO_RAW"."{target_table}" (RAW, SOURCE_URL) VALUES (%s, %s)'
                cur.execute(sql, (json.dumps(payload), url))
            conn.commit()
        logging.info(f"Successfully updated VILLO_RAW.{target_table}")
    except Exception as e:
        logging.error(f"Failed to ingest {target_table}: {e}")