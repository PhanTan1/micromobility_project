import time
from utils import ingest_raw_data, logging

URL_STATUS = "https://api.cyclocity.fr/contracts/bruxelles/gbfs/v3/station_status.json"
INTERVAL = 600 

def main():
    logging.info("Starting station status collector (Interval: 10m)")
    while True:
        start_time = time.time()
        ingest_raw_data("F_STATION_STATUS", URL_STATUS)
        elapsed = time.time() - start_time
        time.sleep(max(0, INTERVAL - elapsed))

if __name__ == "__main__":
    main()