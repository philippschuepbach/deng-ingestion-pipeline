from pathlib import Path

from .gdelt.lookup import get_latest_gdelt_url
from .gdelt.download import download_file
from .gdelt.extract import extract_single_file


def fetch_latest_gdelt(
    *,
    last_update_url: str,
    raw_data_dir: Path,
) -> Path:
    """Run full GDELT ingestion pipeline and return extracted CSV path."""
    url = get_latest_gdelt_url(last_update_url=last_update_url)

    zip_path = download_file(url, dest_dir=raw_data_dir)

    csv_path = extract_single_file(zip_path, dest_dir=raw_data_dir)

    return csv_path