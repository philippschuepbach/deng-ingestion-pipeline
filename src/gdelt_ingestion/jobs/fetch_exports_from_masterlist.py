from __future__ import annotations

from pathlib import Path

from loguru import logger

from gdelt_ingestion.gdelt.fetch_files import download_and_extract_files
from gdelt_ingestion.gdelt.masterlist import (
    iter_urls_in_range_from_file,
    iter_urls_in_range_from_url,
)


def _validate_master_source(
    *,
    master_url: str | None,
    master_file: Path | None,
) -> None:
    """Ensure exactly one master list source is provided."""
    if master_url and master_file:
        raise ValueError("Provide either master_url or master_file, not both")
    if not master_url and not master_file:
        raise ValueError("Either master_url or master_file must be provided")


def run(
    *,
    start_ts: str,
    end_ts: str,
    raw_data_dir: Path,
    master_url: str | None = None,
    master_file: Path | None = None,
    timeout_sec: int = 60,
    overwrite: bool = False,
    delete_zip: bool = False,
) -> list[Path]:
    """
    Fetch GDELT export files from a master list for the given time range.

    The master list can be provided either as a remote URL or as a local file.
    Matching export ZIPs are downloaded and extracted into raw_data_dir.

    Returns a list of extracted CSV file paths.
    """
    _validate_master_source(master_url=master_url, master_file=master_file)

    logger.info(
        "Starting export fetch from master list for range {} .. {}",
        start_ts,
        end_ts,
    )

    if master_url is not None:
        urls = list(
            iter_urls_in_range_from_url(
                master_url=master_url,
                start_ts=start_ts,
                end_ts=end_ts,
                file_types=["export"],
                timeout_sec=timeout_sec,
            )
        )
    else:
        urls = list(
            iter_urls_in_range_from_file(
                master_file=master_file,
                start_ts=start_ts,
                end_ts=end_ts,
                file_types=["export"],
            )
        )

    if not urls:
        logger.warning(
            "No export files found in master list for range {} .. {}",
            start_ts,
            end_ts,
        )
        return []

    logger.info("Found {} export file(s) to download and extract", len(urls))

    extracted_paths = download_and_extract_files(
        urls,
        dest_dir=raw_data_dir,
        timeout_sec=timeout_sec,
        overwrite=overwrite,
        delete_zip=delete_zip,
    )

    logger.success(
        "Fetched and extracted {} export file(s) into {}",
        len(extracted_paths),
        raw_data_dir,
    )
    return extracted_paths
