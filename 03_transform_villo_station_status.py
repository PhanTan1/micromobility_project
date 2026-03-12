import json
import logging
from datetime import datetime, timezone
from dateutil import parser
from utils import get_pg_conn, batch_insert_staging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_iso8601(ts_str):
    """Reliably parse ISO8601 timestamps from GBFS payload into naive UTC datetime."""
    if not ts_str:
        return None
    try:
        return parser.isoparse(ts_str).astimezone(timezone.utc).replace(tzinfo=None)
    except Exception as e:
        logging.error(f"Timestamp parsing error for '{ts_str}': {e}")
        return None

def transform_villo_status():
    """
    Reads the latest RAW Villo status snapshot and transforms it into the STAGING layer.
    Utilizes batch insertion for performance.
    """
    logging.info("Starting transformation: VILLO_RAW to VILLO_STAGING (Station Status)")

    # Query to fetch only the most recent raw record
    query_raw = 'SELECT raw, load_ts FROM "VILLO_RAW"."F_STATION_STATUS" ORDER BY load_ts DESC LIMIT 1'
    
    try:
        # 1. Fetch latest raw payload
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query_raw)
                result = cur.fetchone()
        
        if not result:
            logging.warning("No raw status data found to transform.")
            return

        payload, raw_load_ts = result
        
        # 2. Extract global snapshot timestamp (last_updated)
        header_last_updated = payload.get("last_updated")
        if isinstance(header_last_updated, (int, float)):
            last_updated_ts = datetime.fromtimestamp(header_last_updated, tz=timezone.utc).replace(tzinfo=None)
        else:
            last_updated_ts = parse_iso8601(header_last_updated)

        if not last_updated_ts:
            logging.error("Missing valid 'last_updated' timestamp. Transformation aborted.")
            return

        # 3. Process the station list and prepare tuples for batch insert
        stations = payload.get("data", {}).get("stations", [])
        rows_to_insert = []

        for s in stations:
            station_id = s.get("station_id")
            if not station_id:
                continue

            # Calculate vehicle metrics from the nested types array
            v_types = s.get("vehicle_types_available", [])
            mech_count = sum(v.get("count", 0) for v in v_types if v.get("vehicle_type_id") == "mechanical")
            elec_count = sum(v.get("count", 0) for v in v_types if v.get("vehicle_type_id") == "electrical")

            rows_to_insert.append((
                station_id,
                last_updated_ts,
                parse_iso8601(s.get("last_reported")),
                s.get("num_vehicles_available"),
                s.get("num_docks_available"),
                s.get("num_vehicles_disabled"),
                s.get("num_docks_disabled"),
                json.dumps(v_types),
                mech_count,
                elec_count,
                s.get("is_installed"),
                s.get("is_renting"),
                s.get("is_returning"),
                raw_load_ts  # Carry over the raw load timestamp for data lineage
            ))

        # 4. Batch Insert into Staging
        if rows_to_insert:
            columns = [
                "station_id", "last_updated", "last_reported", "num_vehicles_available",
                "num_docks_available", "num_vehicles_disabled", "num_docks_disabled",
                "vehicle_types_available", "mechanical_count", "electrical_count",
                "is_installed", "is_renting", "is_returning", "load_ts"
            ]
            
            # Use utility function for execute_values performance
            batch_insert_staging("F_STATION_STATUS", columns, rows_to_insert)
            logging.info(f"Successfully processed {len(rows_to_insert)} stations from snapshot {raw_load_ts}")

    except Exception as e:
        logging.error(f"Transformation failed: {e}")

if __name__ == "__main__":
    transform_villo_status()