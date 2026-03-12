from utils import ingest_raw_data, logging

URL_INFO = "https://api.cyclocity.fr/contracts/bruxelles/gbfs/v3/station_information.json"

def main():
    logging.info("Starting station information ingestion (Frequency: 24h)")
    ingest_raw_data("D_STATION", URL_INFO)

if __name__ == "__main__":
    main()