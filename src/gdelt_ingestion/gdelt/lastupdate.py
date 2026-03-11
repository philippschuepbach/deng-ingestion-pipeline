from __future__ import annotations

import requests
from loguru import logger


def fetch_text(url: str, *, timeout_sec: int = 10) -> str:
    """Fetch a URL and return the response body as text."""
    logger.info("Fetching lastupdate from {}", url)
    try:
        response = requests.get(url, timeout=timeout_sec)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        raise RuntimeError(
            f"HTTP request failed for {url} (timeout={timeout_sec}s)"
        ) from e


def extract_url_ending_with(lastupdate_text: str, target_file: str) -> str:
    """Return the URL in lastupdate.txt whose filename ends with target_file."""
    for line in lastupdate_text.splitlines():
        parts = line.split(maxsplit=2)
        if len(parts) != 3:
            continue

        url = parts[2].strip()
        if url.endswith(target_file):
            return url

    raise ValueError(f"No URL ending with {target_file!r} found in lastupdate response")


def get_latest_gdelt_url(
    *, last_update_url: str, target_file: str = "export.CSV.zip", timeout_sec: int = 10
) -> str:
    """Return the latest GDELT file URL for the given target_file based on lastupdate.txt."""
    text = fetch_text(last_update_url, timeout_sec=timeout_sec)
    url = extract_url_ending_with(text, target_file)
    logger.info("Latest file URL: {}", url)
    return url
