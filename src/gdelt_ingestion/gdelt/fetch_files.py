from __future__ import annotations

from pathlib import Path

import requests
from loguru import logger

from gdelt_ingestion.gdelt.download import download_file
from gdelt_ingestion.gdelt.extract import extract_single_file


def download_and_extract_file(
    url: str,
    *,
    dest_dir: Path,
    timeout_sec: int = 60,
    overwrite: bool = False,
    delete_zip: bool = False,
    session: requests.Session | None = None,
) -> Path:
    """
    Download a single GDELT ZIP file and extract it.

    Returns the extracted file path.
    """
    zip_path = download_file(
        url,
        dest_dir=dest_dir,
        timeout_sec=timeout_sec,
        overwrite=overwrite,
        session=session,
    )

    extracted_path = extract_single_file(
        zip_path,
        dest_dir=dest_dir,
        overwrite=overwrite,
    )

    if delete_zip:
        try:
            zip_path.unlink(missing_ok=True)
            logger.info("Deleted ZIP after extraction: {}", zip_path)
        except OSError as e:
            raise RuntimeError(
                f"Failed to delete ZIP after extraction: {zip_path}"
            ) from e

    return extracted_path


def download_and_extract_files(
    urls: list[str],
    *,
    dest_dir: Path,
    timeout_sec: int = 60,
    overwrite: bool = False,
    delete_zip: bool = False,
    session: requests.Session | None = None,
) -> list[Path]:
    """
    Download and extract multiple GDELT ZIP files.

    Returns a list of extracted file paths.
    """
    extracted_paths: list[Path] = []

    for index, url in enumerate(urls, start=1):
        logger.info("Processing file {}/{}: {}", index, len(urls), url)

        extracted_path = download_and_extract_file(
            url,
            dest_dir=dest_dir,
            timeout_sec=timeout_sec,
            overwrite=overwrite,
            delete_zip=delete_zip,
            session=session,
        )
        extracted_paths.append(extracted_path)

    logger.success("Downloaded and extracted {} file(s)", len(extracted_paths))
    return extracted_paths
