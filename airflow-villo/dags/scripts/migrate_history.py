import os
import sys
import logging
from dotenv import load_dotenv

# --- ADD THIS BLOCK BEFORE FROM UTILS IMPORT ---
# This adds the parent directory (dags/) to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils import get_pg_conn
# -----------------------------------------------
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_history_migration():
    logging.info("Starting historical data migration (61M rows)...")
    
    conn = get_pg_conn()
    conn.autocommit = True 
    cur = conn.cursor()

    try:
        # 1. Performance Tuning (Allowed at session level)
        logging.info("Tuning database session for heavy load...")
        cur.execute("SET maintenance_work_mem = '4GB';")
        cur.execute("SET synchronous_commit = off;")
        # checkpoint_timeout removed because it requires superuser/config file access

        # 2. Quarantine: Move rows with empty timestamps to BACKUP_ERRORS
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

        # 3. Bulk Move: Transfer clean data to Analytics
        logging.info("Step 2: Performing bulk move to VILLO_ANALYTICS (61M rows)...")
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
                "LAST_UPDATE_TS"::timestamp,
                "STANDS_NB",
                "AVAILABLE_STANDS_NB",
                "AVAILABLE_VEHICLES_NB",
                "STATUS",
                "BONUS_FLAG",
                "BANKING_FLAG",
                "LOAD_TS"::timestamp
FROM "VILLO_RAW"."SNOWFLAKE_BACKUP";
        """
        cur.execute(move_sql)
        logging.info(f"Successfully migrated {cur.rowcount} rows to Analytics.")

        # 4. Indexing: Create indexes AFTER data load
        logging.info("Step 3: Creating indexes (this will take a few minutes)...")
        cur.execute('CREATE INDEX IF NOT EXISTS idx_f_status_station_fk ON "VILLO_ANALYTICS"."F_STATION_STATUS" (station_fk);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_f_status_date ON "VILLO_ANALYTICS"."F_STATION_STATUS" (last_update_ts);')
        
        logging.info("Step 4: Updating statistics (ANALYZE)...")
        cur.execute('ANALYZE "VILLO_ANALYTICS"."F_STATION_STATUS";')

        logging.info("Migration successfully completed!")

    except Exception as e:
        logging.error(f"Migration failed: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    run_history_migration()