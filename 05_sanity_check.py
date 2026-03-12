import logging
from utils import get_pg_conn

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(INFO)s - %(message)s')

def run_sanity_checks():
    logging.info("--- STARTING DATA PIPELINE AUDIT ---")
    
    queries = {
        "Total Raw Snapshots": 'SELECT COUNT(*) FROM "VILLO_RAW"."F_STATION_STATUS"',
        "Total Analytics Records": 'SELECT COUNT(*) FROM "VILLO_ANALYTICS"."F_STATION_STATUS"',
        "Total Stations (Gold)": 'SELECT COUNT(*) FROM "VILLO_ANALYTICS"."D_STATION"',
        "Last Update Timestamp": 'SELECT MAX(last_update_ts) FROM "VILLO_ANALYTICS"."F_STATION_STATUS"',
    }

    try:
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                # 1. Basic Counts
                for description, query in queries.items():
                    cur.execute(query)
                    result = cur.fetchone()[0]
                    print(f"[CHECK] {description}: {result}")

                # 2. Orphan Check (Crucial for Data Integrity)
                # This finds status records that don't match any station in your dimension table
                orphan_query = """
                SELECT COUNT(*) 
                FROM "VILLO_ANALYTICS"."F_STATION_STATUS" f
                LEFT JOIN "VILLO_ANALYTICS"."D_STATION" d ON f.station_fk = d.station_pk
                WHERE d.station_pk IS NULL
                """
                cur.execute(orphan_query)
                orphans = cur.fetchone()[0]
                
                if orphans == 0:
                    print("[PASS] Integrity: No orphan status records found.")
                else:
                    print(f"[FAIL] Integrity: Found {orphans} status records without a matching station!")

                # 3. Snapshot Consistency
                # Check if a single raw snapshot (average ~200 stations) matches the expected volume
                consistency_query = """
                SELECT load_ts, COUNT(*) 
                FROM "VILLO_ANALYTICS"."F_STATION_STATUS" 
                GROUP BY load_ts 
                ORDER BY load_ts DESC LIMIT 5
                """
                cur.execute(consistency_query)
                print("\n[INFO] Recent Load Consistency (Expect ~200-215 per snapshot):")
                for row in cur.fetchall():
                    print(f"       {row[0]} -> {row[1]} rows")

    except Exception as e:
        logging.error(f"Audit failed: {e}")

if __name__ == "__main__":
    run_sanity_checks()