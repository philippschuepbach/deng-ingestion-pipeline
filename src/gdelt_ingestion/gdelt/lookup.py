from __future__ import annotations

from loguru import logger

from ..common.http import fetch_text


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
    *,
    last_update_url: str,
    target_file: str = "export.CSV.zip",
    timeout_sec: int = 10,
) -> str:
    """Return the latest GDELT file URL."""
    text = fetch_text(last_update_url, timeout_sec=timeout_sec)
    url = extract_url_ending_with(text, target_file)

    logger.info("Latest GDELT URL: {}", url)
    return url