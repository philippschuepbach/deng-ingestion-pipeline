from pathlib import Path
from loguru import logger

# Import your newly refactored modules
from gdelt.lastupdate import get_latest_gdelt_url
from gdelt.download import download_file
from gdelt.extract import extract_single_file

# Constants
GDELT_LAST_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
RAW_DATA_DIR = Path("data/raw")


def main():
    logger.info("Starting GDELT batch ingestion pipeline...")

    try:
        # Step 1: Find the latest URL
        url = get_latest_gdelt_url(last_update_url=GDELT_LAST_UPDATE_URL)

        # Step 2: Download the file atomically
        zip_path = download_file(url, dest_dir=RAW_DATA_DIR)

        # Step 3: Extract the file atomically
        csv_path = extract_single_file(zip_path, dest_dir=RAW_DATA_DIR)

        logger.success(
            "Pipeline execution complete! Data is ready for Jupyter at: {}", csv_path
        )

    except Exception as e:
        logger.exception("Pipeline failed:")


if __name__ == "__main__":
    main()
