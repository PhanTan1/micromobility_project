import sys
import logging
from dotenv import load_dotenv
from pathlib import Path

racine_projet = Path(__file__).parent.parent
sys.path.append(str(racine_projet))

from dags.common.utils import get_pg_conn
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_history_migration():
    logging.info("Starting historical data migration with UTC conversion...")
    
    conn = get_pg_conn()
    conn.autocommit = True 
    cur = conn.cursor()

    try:
        # 1. Performance Tuning
        logging.info("Tuning database session for heavy load...")
        cur.execute("SET maintenance_work_mem = '4GB';")
        cur.execute("SET synchronous_commit = off;")

        # 2. Quarantine: Move rows with empty timestamps
        logging.info("Step 1: Quarantining rows with empty timestamps...")
        quarantine_sql = """
            INSERT INTO "VILLO_RAW"."BACKUP_ERRORS"
            SELECT * FROM "VILLO_RAW"."SNOWFLAKE_BACKUP"
            WHERE "LAST_UPDATE_TS" = '' OR "LAST_UPDATE_TS" IS NULL;
            
            DELETE FROM "VILLO_RAW"."SNOWFLAKE_BACKUP"
            WHERE "LAST_UPDATE_TS" = '' OR "LAST_UPDATE_TS" IS NULL;
        """
        cur.execute(quarantine_sql)
        logging.info("Quarantine and cleanup complete.")

        # 3. La "Coupe Franche" : Supprimer le chevauchement avec Airflow
        logging.info("Step 2: Cleaning overlap to prevent duplicates with Live Airflow data...")
        clean_overlap_sql = """
            DELETE FROM "VILLO_ANALYTICS"."F_STATION_STATUS"
            WHERE last_update_ts <= (
                SELECT MAX("LAST_UPDATE_TS"::timestamp AT TIME ZONE 'Europe/Brussels' AT TIME ZONE 'UTC')
                FROM "VILLO_RAW"."SNOWFLAKE_BACKUP"
            );
        """
        cur.execute(clean_overlap_sql)
        logging.info("Overlap cleaned successfully.")

        # 4. Bulk Move avec CONVERSION UTC
        logging.info("Step 3: Performing bulk move and UTC conversion to VILLO_ANALYTICS...")
        move_sql = """
            INSERT INTO "VILLO_ANALYTICS"."F_STATION_STATUS" (
                station_fk, 
                last_update_ts,
                stands_nb, 
                available_stands_nb, 
                available_vehicles_nb,
                status, 
                bonus_flag, 
                banking_flag, 
                load_ts
            )
            SELECT 
                "STATION_FK",
                -- CONVERSION MAGIQUE : Heure Locale (Bruxelles) -> UTC (GMT 0)
                ("LAST_UPDATE_TS"::timestamp AT TIME ZONE 'Europe/Brussels') AT TIME ZONE 'UTC',
                "STANDS_NB",
                "AVAILABLE_STANDS_NB",
                "AVAILABLE_VEHICLES_NB",
                "STATUS",
                "BONUS_FLAG",
                "BANKING_FLAG",
                -- On convertit aussi le Load_ts pour rester propre
                ("LOAD_TS"::timestamp AT TIME ZONE 'Europe/Brussels') AT TIME ZONE 'UTC'
            FROM "VILLO_RAW"."SNOWFLAKE_BACKUP";
        """
        cur.execute(move_sql)
        logging.info(f"Successfully migrated and converted {cur.rowcount} rows to Analytics.")

        # 5. Indexing
        logging.info("Step 4: Creating indexes (this will take a few minutes)...")
        cur.execute('CREATE INDEX IF NOT EXISTS idx_f_status_station_fk ON "VILLO_ANALYTICS"."F_STATION_STATUS" (station_fk);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_f_status_date ON "VILLO_ANALYTICS"."F_STATION_STATUS" (last_update_ts);')
        
        logging.info("Step 5: Updating statistics (ANALYZE)...")
        cur.execute('ANALYZE "VILLO_ANALYTICS"."F_STATION_STATUS";')

        logging.info("Migration successfully completed! Data is now in pure UTC.")

    except Exception as e:
        logging.error(f"Migration failed: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    run_history_migration()