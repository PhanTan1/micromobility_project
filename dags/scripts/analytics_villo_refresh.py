import logging
from common.utils import get_pg_conn

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

-- 2. Insert new stations with deduplication and quality filter
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
SELECT DISTINCT ON (src.station_id::bigint) -- Empêche les doublons dans le même batch
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
WHERE 
    -- FILTRE DE QUALITÉ : On ignore les stations sans nom ou sans adresse
    src.name_en != '' 
    AND src.address != ''
    -- FILTRE ANTI-DOUBLONS Gold
    AND NOT EXISTS (
        SELECT 1 
        FROM "VILLO_ANALYTICS"."D_STATION" AS tgt 
        WHERE tgt.station_pk = src.station_id::bigint
    )
ORDER BY src.station_id::bigint, src.load_ts DESC; -- On prend la version la plus récente si doublon

-- 3. Insert new stations into REF_STATION
INSERT INTO "VILLO_ANALYTICS"."REF_STATION" (station_pk, bonus_flag, banking_flag)
SELECT DISTINCT src.station_id::bigint, FALSE, FALSE
FROM "VILLO_STAGING"."D_STATION" AS src
WHERE 
    src.name_en != '' -- Même filtre de qualité ici
    AND NOT EXISTS (
        SELECT 1 
        FROM "VILLO_ANALYTICS"."REF_STATION" AS tgt 
        WHERE tgt.station_pk = src.station_id::bigint
    );
"""

SQL_MOVE_TO_FACTS = """
INSERT INTO "VILLO_ANALYTICS"."F_STATION_STATUS" ( 
    STATION_FK, LAST_UPDATE_TS, STANDS_NB, AVAILABLE_STANDS_NB, AVAILABLE_VEHICLES_NB, STATUS, BONUS_FLAG, BANKING_FLAG, LOAD_TS
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
    s.load_ts                        AS load_ts
FROM "VILLO_STAGING"."F_STATION_STATUS" s
JOIN "VILLO_STAGING"."D_STATION" d_sta
    ON d_sta.station_id = s.station_id
JOIN "VILLO_ANALYTICS"."D_STATION" d_ana
    ON d_ana.station_pk = d_sta.station_id::bigint
LEFT JOIN "VILLO_ANALYTICS"."REF_STATION" ref
    ON ref.station_pk = d_ana.station_pk
WHERE NOT EXISTS (
    SELECT 1 FROM "VILLO_ANALYTICS"."F_STATION_STATUS" f_existing
    WHERE f_existing.station_fk = d_ana.station_pk AND f_existing.last_update_ts = s.last_updated
);
"""

def refresh_dimensions():
    """Runs ONCE A DAY to update the gold dimension tables."""
    logging.info("Starting Daily Analytics Refresh (Dimensions)...")
    try:
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(SQL_UPSERT_D_STATION)
            conn.commit()
            logging.info("Dimensions successfully updated.")
    except Exception as e:
        logging.error(f"Dimension refresh failed: {e}")

def refresh_facts():
    """Runs EVERY 10 MINUTES to insert new status facts."""
    logging.info("Starting 10-Min Analytics Refresh (Facts)...")
    try:
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(SQL_MOVE_TO_FACTS)
                fact_count = cur.rowcount
                logging.info(f"Inserted {fact_count} new facts.")
                
                # ONLY truncate the F_STATION_STATUS table. 
                # Keep D_STATION so the next 10-min run can still join with it!
                cur.execute('TRUNCATE TABLE "VILLO_STAGING"."F_STATION_STATUS"')
            conn.commit()
            logging.info("Facts successfully updated and staging cleared.")
    except Exception as e:
        logging.error(f"Fact refresh failed: {e}")