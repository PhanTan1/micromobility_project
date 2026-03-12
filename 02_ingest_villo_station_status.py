import time
from utils import ingest_raw_villo_data, logging

# Villo GBFS v3 Status URL
URL_STATUS = "https://api.cyclocity.fr/contracts/bruxelles/gbfs/v3/station_status.json"
INTERVAL_SECONDS = 600  # 10 minutes

def main():
    logging.info("Starting Villo Station Status Collector (Interval: 10m)")
    while True:
        start_time = time.time()
        
        ingest_raw_villo_data("F_STATION_STATUS", URL_STATUS)
        
        # Calculate precise sleep time
        elapsed = time.time() - start_time
        sleep_duration = max(0, INTERVAL_SECONDS - elapsed)
        logging.info(f"Waiting {int(sleep_duration)}s for next cycle...")
        time.sleep(sleep_duration)

if __name__ == "__main__":
    main()