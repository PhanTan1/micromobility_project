from utils import ingest_raw_villo_data, logging

# Villo GBFS v3 Information URL
URL_INFO = "https://api.cyclocity.fr/contracts/bruxelles/gbfs/v3/station_information.json"

def main():
    logging.info("Starting Villo Station Information Ingestion (Daily Job)")
    # This script is intended to be run once per day (e.g., via Cron)
    ingest_raw_villo_data("D_STATION", URL_INFO)

if __name__ == "__main__":
    main()