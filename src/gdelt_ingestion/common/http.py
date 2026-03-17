from __future__ import annotations
from pathlib import Path

from loguru import logger
import requests

def fetch_text(url: str, *, timeout_sec: int = 10, session: requests.Session | None = None) -> str:
    """Fetch a URL and return the response body as text."""
    client = session or requests
    logger.info("Fetching URL-Content from {}", url)
    try:
        response = client.get(url, timeout=timeout_sec)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        raise RuntimeError(
            f"HTTP request failed for {url} (timeout={timeout_sec}s)"
        ) from e
