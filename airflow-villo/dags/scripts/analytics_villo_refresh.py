import logging
from utils import get_pg_conn

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- SQL COMMANDS ---

SQL_UPSERT_D_STATION = """
-- 1. Update existing stations (coordinates and timestamp)
UPDATE "VILLO_ANALYTICS"."D_STATION" AS tgt
SET 
    lat = src.lat, 
    lon = src.lon, 
    load_ts = src.load_ts
FROM "VILLO_STAGING"."D_STATION" AS src
WHERE tgt.station_pk = src.station_id::bigint;

-- 2. Insert new stations that don't exist yet
INSERT INTO "VILLO_ANALYTICS"."D_STATION" (
    station_id, 
    station_pk, 
    station_name, 
    name_fr, 
    name_nl, 
    address_fr, 
    address_nl, 
    lat, 
    lon, 
    load_ts
)
SELECT 
    src.station_id || ' - ' || src.name_en, 
    src.station_id::bigint, 
    src.name_en, 
    src.name_fr, 
    src.name_nl, 
    src.address, 
    src.address, 
    src.lat, 
    src.lon, 
    src.load_ts
FROM "VILLO_STAGING"."D_STATION" AS src
WHERE NOT EXISTS (
    SELECT 1 
    FROM "VILLO_ANALYTICS"."D_STATION" AS tgt 
    WHERE tgt.station_pk = src.station_id::bigint
);
"""

SQL_MOVE_TO_FACTS = """
INSERT INTO "VILLO_ANALYTICS"."F_STATION_STATUS" ( 
    STATION_FK, 
    LAST_UPDATE_TS, 
    STANDS_NB, 
    AVAILABLE_STANDS_NB, 
    AVAILABLE_VEHICLES_NB, 
    STATUS, 
    BONUS_FLAG, 
    BANKING_FLAG, 
    LOAD_TS
)
SELECT
    d_ana.station_pk                AS station_fk,
    s.last_updated                  AS last_update_ts,
    d_sta.capacity                  AS stands_nb,
    s.num_docks_available           AS available_stands_nb,
    s.num_vehicles_available        AS available_vehicles_nb,
    CASE
        WHEN s.is_renting = FALSE AND s.is_returning = FALSE THEN 'CLOSED'
        ELSE 'OPEN'
    END                             AS status,
    COALESCE(ref.bonus_flag, FALSE)   AS bonus_flag,
    COALESCE(ref.banking_flag, FALSE) AS banking_flag,
    CURRENT_TIMESTAMP               AS load_ts
FROM "VILLO_STAGING"."F_STATION_STATUS" s
JOIN "VILLO_STAGING"."D_STATION" d_sta
    ON d_sta.station_id = s.station_id
JOIN "VILLO_ANALYTICS"."D_STATION" d_ana
    ON d_ana.station_pk = d_sta.station_id::bigint
LEFT JOIN "VILLO_ANALYTICS"."REF_STATION" ref
    ON ref.station_pk = d_ana.station_pk;
"""

def run_analytics_refresh():
    """Main orchestrator for Gold Layer refresh."""
    logging.info("Starting Analytics Refresh (Gold Layer)...")
    
    try:
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                # Step 1: Update D_STATION (Dimensions)
                logging.info("Step 1: Upserting D_STATION (Analytics Dimensions)...")
                cur.execute(SQL_UPSERT_D_STATION)
                
                # Step 2: Update F_STATION_STATUS (Facts)
                logging.info("Step 2: Inserting F_STATION_STATUS (Analytics Facts)...")
                cur.execute(SQL_MOVE_TO_FACTS)
                fact_count = cur.rowcount
                
                # Step 3: Clean up Staging Facts
                logging.info(f"Step 3: Truncating Staging Facts ({fact_count} rows processed)...")
                cur.execute('TRUNCATE TABLE "VILLO_STAGING"."F_STATION_STATUS"')
                
            conn.commit()
            logging.info("Analytics layer successfully refreshed.")
            
    except Exception as e:
        logging.error(f"Critical error during Analytics refresh: {e}")

if __name__ == "__main__":
    run_analytics_refresh()