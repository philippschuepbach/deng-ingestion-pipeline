from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import requests
from loguru import logger

from ..common.files import build_part_path, cleanup_temp_file


def _filename_from_url(url: str) -> str:
    """Extract filename from URL path."""
    name = Path(urlparse(url).path).name
    if not name:
        raise ValueError(f"URL does not contain a filename: {url}")
    return name


def download_file(
    url: str,
    *,
    dest_dir: Path,
    timeout_sec: int = 60,
    chunk_size: int = 8192,
    overwrite: bool = False,
    session: requests.Session | None = None,
) -> Path:
    """Download a file via HTTP with atomic write."""
    dest_dir.mkdir(parents=True, exist_ok=True)

    filename = _filename_from_url(url)
    dest_path = dest_dir / filename
    tmp_path = build_part_path(dest_path)

    if dest_path.exists() and not overwrite:
        logger.info("File exists, skipping: {}", dest_path)
        return dest_path

    client = session or requests

    logger.info("Downloading {} -> {}", url, dest_path)

    try:
        with client.get(url, stream=True, timeout=timeout_sec) as response:
            response.raise_for_status()

            with tmp_path.open("wb") as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)

        tmp_path.replace(dest_path)
        logger.success("Downloaded to {}", dest_path)
        return dest_path

    except requests.RequestException as e:
        raise RuntimeError(f"Download failed for {url}") from e

    finally:
        cleanup_temp_file(tmp_path)
