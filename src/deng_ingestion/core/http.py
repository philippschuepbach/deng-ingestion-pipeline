from __future__ import annotations

import socket
import time
from pathlib import Path
from shutil import copyfileobj
from typing import cast
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from loguru import logger

RETRYABLE_HTTP_STATUS_CODES = {408, 429, 500, 502, 503, 504}


def _is_retryable_exception(exc: Exception) -> bool:
    if isinstance(exc, HTTPError):
        return exc.code in RETRYABLE_HTTP_STATUS_CODES

    if isinstance(exc, URLError):
        reason = exc.reason
        return isinstance(reason, TimeoutError | socket.timeout | OSError)

    return isinstance(exc, TimeoutError | socket.timeout | OSError)


def fetch_text(
    url: str,
    *,
    timeout_seconds: float = 15.0,
    retries: int = 3,
    retry_delay_seconds: float = 1.0,
    encoding: str = "utf-8",
) -> str:
    last_exception: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            with urlopen(url, timeout=timeout_seconds) as response:
                data = cast(bytes, response.read())
                return data.decode(encoding)
        except Exception as exc:
            last_exception = exc

            if attempt == retries or not _is_retryable_exception(exc):
                raise

            time.sleep(retry_delay_seconds * attempt)

    assert last_exception is not None
    raise last_exception


def download_binary_to_file(
    url: str,
    destination: Path,
    *,
    timeout_seconds: float = 30.0,
    retries: int = 3,
    retry_delay_seconds: float = 1.0,
    chunk_size: int = 1024 * 1024,
) -> None:
    last_exception: Exception | None = None
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_suffix(destination.suffix + ".part")

    for attempt in range(1, retries + 1):
        try:
            if temp_path.exists():
                temp_path.unlink()

            with (
                urlopen(url, timeout=timeout_seconds) as response,
                temp_path.open("wb") as target,
            ):
                copyfileobj(response, target, length=chunk_size)

            temp_path.replace(destination)
            return

        except Exception as exc:
            last_exception = exc

            if isinstance(exc, HTTPError):
                logger.warning(
                    "HTTP retryable error while downloading: "
                    "url={}, status_code={}, attempt={}/{}",
                    url,
                    exc.code,
                    attempt,
                    retries,
                )

            if temp_path.exists():
                temp_path.unlink()

            if attempt == retries or not _is_retryable_exception(exc):
                raise

            time.sleep(retry_delay_seconds * attempt)

    assert last_exception is not None
    raise last_exception
