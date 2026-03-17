from __future__ import annotations

from pathlib import Path

from loguru import logger

from gdelt_ingestion.constants import build_lookup_urls
from ..gdelt.download import download_file


def run(*, dest_dir: Path) -> list[Path]:
    """Download all GDELT lookup tables."""
    logger.info("Downloading GDELT lookup tables...")

    paths: list[Path] = []

    for url in build_lookup_urls():
        path = download_file(url, dest_dir=dest_dir)
        paths.append(path)

    logger.success("Downloaded {} lookup files", len(paths))
    return paths