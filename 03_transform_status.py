import os
import json
import logging
from datetime import datetime, timezone
import psycopg2
import psycopg2.extras
from dateutil import parser
from utils import get_pg_conn  # Reusing your centralized connection logic

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_iso8601(ts_str):
    """Reliably parse ISO8601 timestamps from GBFS payload."""
    if not ts_str:
        return None
    try:
        # Returns naive datetime for Postgres 'timestamp without time zone'
        return parser.isoparse(ts_str).astimezone(timezone.utc).replace(tzinfo=None)
    except Exception as e:
        logging.error(f"Timestamp parsing error: {ts_str} - {e}")
        return None

def get_latest_raw_payload(cur):
    """Fetch the most recent record from the RAW table."""
    query = 'SELECT raw, load_ts FROM "VILLO_RAW"."F_STATION_STATUS" ORDER BY load_ts DESC LIMIT 1'
    cur.execute(query)
    return cur.fetchone()

def transform_status():
    logging.info("Starting transformation: RAW to STAGING (Station Status)")
    
    try:
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                # 1. Get the latest snapshot from RAW
                result = get_latest_raw_payload(cur)
                if not result:
                    logging.warning("No data found in VILLO_RAW.F_STATION_STATUS.")
                    return
                
                payload, raw_load_ts = result
                
                # 2. Extract global timestamp (last_updated)
                header_last_updated = payload.get("last_updated")
                if isinstance(header_last_updated, (int, float)):
                    last_updated_ts = datetime.fromtimestamp(header_last_updated, tz=timezone.utc).replace(tzinfo=None)
                else:
                    last_updated_ts = parse_iso8601(header_last_updated)

                if not last_updated_ts:
                    logging.error("Missing valid last_updated timestamp in payload. Skipping.")
                    return

                # 3. Process station list
                stations = payload.get("data", {}).get("stations", [])
                rows = []

                for s in stations:
                    station_id = s.get("station_id")
                    if not station_id:
                        continue

                    # Count vehicle types
                    vehicle_types = s.get("vehicle_types_available", [])
                    mech_count = sum(v.get("count", 0) for v in vehicle_types if v.get("vehicle_type_id") == "mechanical")
                    elec_count = sum(v.get("count", 0) for v in vehicle_types if v.get("vehicle_type_id") == "electrical")

                    rows.append((
                        station_id,
                        last_updated_ts,
                        parse_iso8601(s.get("last_reported")),
                        s.get("num_vehicles_available"),
                        s.get("num_docks_available"),
                        s.get("num_vehicles_disabled"),
                        s.get("num_docks_disabled"),
                        json.dumps(vehicle_types),
                        mech_count,
                        elec_count,
                        s.get("is_installed"),
                        s.get("is_renting"),
                        s.get("is_returning"),
                        raw_load_ts # Traceability: when it was originally loaded
                    ))

                # 4. Insert into STAGING
                if rows:
                    insert_query = """
                    INSERT INTO "VILLO_STAGING"."F_STATION_STATUS" (
                        station_id, last_updated, last_reported, num_vehicles_available,
                        num_docks_available, num_vehicles_disabled, num_docks_disabled,
                        vehicle_types_available, mechanical_count, electrical_count,
                        is_installed, is_renting, is_returning, load_ts
                    ) VALUES %s
                    """
                    psycopg2.extras.execute_values(cur, insert_query, rows)
                    conn.commit()
                    logging.info(f"Successfully staged {len(rows)} rows from RAW snapshot {raw_load_ts}")

    except Exception as e:
        logging.error(f"Transformation failed: {e}")

if __name__ == "__main__":
    transform_status()