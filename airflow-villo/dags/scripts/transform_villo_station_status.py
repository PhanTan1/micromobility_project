import json
import logging
from datetime import datetime, timezone
from dateutil import parser
from utils import get_pg_conn, batch_insert_staging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_iso8601(ts_str):
    if not ts_str: return None
    try:
        return parser.isoparse(ts_str).astimezone(timezone.utc).replace(tzinfo=None)
    except:
        return None

def run_villo_status_transformation():
    """
    Incremental transformation: Finds all RAW snapshots not yet in STAGING 
    and processes them in order.
    """
    logging.info("Starting Incremental Transformation: RAW to STAGING")

    try:
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                # 1. Get the 'Watermark' (The latest record we already have in STAGING)
                cur.execute('SELECT MAX(load_ts) FROM "VILLO_STAGING"."F_STATION_STATUS"')
                watermark = cur.fetchone()[0]
                
                if watermark is None:
                    logging.info("Staging is empty. Fetching all available history from RAW.")
                    query_raw = 'SELECT raw, load_ts FROM "VILLO_RAW"."F_STATION_STATUS" ORDER BY load_ts ASC'
                    cur.execute(query_raw)
                else:
                    logging.info(f"Last processed record in STAGING: {watermark}. Fetching new data...")
                    query_raw = 'SELECT raw, load_ts FROM "VILLO_RAW"."F_STATION_STATUS" WHERE load_ts > %s ORDER BY load_ts ASC'
                    cur.execute(query_raw, (watermark,))
                
                raw_snapshots = cur.fetchall()

        if not raw_snapshots:
            logging.info("No new records found in RAW. Staging is already up to date.")
            return

        logging.info(f"Found {len(raw_snapshots)} new raw snapshots to process.")

        # 2. Process each snapshot
        total_rows_processed = 0
        for payload, raw_load_ts in raw_snapshots:
            
            # Extract header timestamp
            header_raw = payload.get("last_updated")
            last_updated_ts = datetime.fromtimestamp(header_raw, tz=timezone.utc).replace(tzinfo=None) \
                if isinstance(header_raw, (int, float)) else parse_iso8601(header_raw)

            stations = payload.get("data", {}).get("stations", [])
            rows_to_insert = []

            for s in stations:
                v_types = s.get("vehicle_types_available", [])
                mech = sum(v.get("count", 0) for v in v_types if v.get("vehicle_type_id") == "mechanical")
                elec = sum(v.get("count", 0) for v in v_types if v.get("vehicle_type_id") == "electrical")

                rows_to_insert.append((
                    s.get("station_id"),
                    last_updated_ts,
                    parse_iso8601(s.get("last_reported")),
                    s.get("num_vehicles_available"),
                    s.get("num_docks_available"),
                    s.get("num_vehicles_disabled"),
                    s.get("num_docks_disabled"),
                    json.dumps(v_types),
                    mech,
                    elec,
                    s.get("is_installed"),
                    s.get("is_renting"),
                    s.get("is_returning"),
                    raw_load_ts
                ))

            # 3. Batch insert this snapshot
            cols = [
                "station_id", "last_updated", "last_reported", "num_vehicles_available",
                "num_docks_available", "num_vehicles_disabled", "num_docks_disabled",
                "vehicle_types_available", "mechanical_count", "electrical_count",
                "is_installed", "is_renting", "is_returning", "load_ts"
            ]
            batch_insert_staging("F_STATION_STATUS", cols, rows_to_insert)
            total_rows_processed += len(rows_to_insert)

        logging.info(f"Transformation Complete. Total stations staged: {total_rows_processed}")

    except Exception as e:
        logging.error(f"Incremental transformation failed: {e}")

if __name__ == "__main__":
    run_villo_status_transformation()