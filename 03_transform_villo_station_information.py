import logging
from utils import get_pg_conn, batch_insert_staging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_station_info():
    """
    Refreshes the STAGING station information by parsing the latest RAW snapshot.
    Uses a 'Truncate and Load' strategy.
    """
    logging.info("Starting transformation: VILLO_RAW to VILLO_STAGING (Station Info)")

    # 1. Fetch the absolute latest raw record
    query_raw = 'SELECT raw, load_ts FROM "VILLO_RAW"."D_STATION" ORDER BY load_ts DESC LIMIT 1'
    
    try:
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                # TRUNCATE first to ensure we only keep the latest reference data
                cur.execute('TRUNCATE TABLE "VILLO_STAGING"."D_STATION"')
                
                cur.execute(query_raw)
                result = cur.fetchone()
        
        if not result:
            logging.warning("No raw information data found.")
            return

        payload, raw_load_ts = result
        stations = payload.get("data", {}).get("stations", [])
        rows_to_insert = []

        for s in stations:
            # Logic for localized names
            name_data = s.get("name", [])
            
            # Default to None and fill if found in the JSON array
            n_en = n_nl = n_fr = None
            
            if isinstance(name_data, list):
                for lang_entry in name_data:
                    lang = lang_entry.get("language")
                    text = lang_entry.get("text")
                    if lang == 'en': n_en = text
                    elif lang == 'nl': n_nl = text
                    elif lang == 'fr': n_fr = text
            else:
                # Fallback if name is just a string (depending on API version)
                n_en = n_nl = n_fr = name_data

            rows_to_insert.append((
                s.get("station_id"),
                n_en, n_nl, n_fr,
                s.get("lat"),
                s.get("lon"),
                s.get("address"),
                s.get("capacity"),
                raw_load_ts
            ))

        # 2. Batch Insert
        cols = ["station_id", "name_en", "name_nl", "name_fr", "lat", "lon", "address", "capacity", "load_ts"]
        batch_insert_staging("D_STATION", cols, rows_to_insert)
        logging.info(f"Successfully refreshed {len(rows_to_insert)} stations in STAGING.")

    except Exception as e:
        logging.error(f"Transformation of station information failed: {e}")

if __name__ == "__main__":
    transform_station_info()