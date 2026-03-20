from common.utils import ingest_raw_villo_data, logging

# Villo GBFS v3 Status URL
URL_STATUS = "https://api.cyclocity.fr/contracts/bruxelles/gbfs/v3/station_status.json"

def run_ingestion():
    """
    Logic called by Airflow. 
    Downloads one snapshot of station status and saves to RAW.
    """
    logging.info("Executing Villo Station Status Ingestion...")
    try:
        ingest_raw_villo_data("F_STATION_STATUS", URL_STATUS)
    except Exception as e:
        logging.error(f"Ingestion failed: {e}")
        raise e

if __name__ == "__main__":
    run_ingestion()