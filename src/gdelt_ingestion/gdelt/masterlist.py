from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator
import re

import requests
from loguru import logger

GDELT_FILE_URL_PATTERN = re.compile(
    r"^https?://data\.gdeltproject\.org/gdeltv2/"
    r"(?P<timestamp>\d{14})\.(?P<file_type>export|mentions|gkg)\.[^/\s]+$",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class GdeltMasterEntry:
    """Represents one parsed line from a GDELT master list."""

    size_bytes: int
    md5: str
    url: str
    timestamp: str
    file_type: str


def parse_masterlist_line(line: str) -> GdeltMasterEntry | None:
    """
    Parse one line from a GDELT master list.

    Expected format:
    <size> <md5> <url>

    Example:
    270542 6369360500439b1ebf2f4563c2eabcfc
    http://data.gdeltproject.org/gdeltv2/20200505201500.mentions.CSV.zip
    """
    stripped = line.strip()
    if not stripped:
        return None

    parts = stripped.split(maxsplit=2)
    if len(parts) != 3:
        return None

    raw_size, md5, url = parts

    try:
        size_bytes = int(raw_size)
    except ValueError:
        return None

    match = GDELT_FILE_URL_PATTERN.match(url)
    if not match:
        return None

    timestamp = match.group("timestamp")
    file_type = match.group("file_type").lower()

    return GdeltMasterEntry(
        size_bytes=size_bytes,
        md5=md5,
        url=url,
        timestamp=timestamp,
        file_type=file_type,
    )


def iter_master_entries_from_lines(lines: Iterable[str]) -> Iterator[GdeltMasterEntry]:
    """Yield parsed GDELT master entries from an iterable of text lines."""
    for line in lines:
        entry = parse_masterlist_line(line)
        if entry is not None:
            yield entry


def iter_master_entries_from_url(
    *,
    master_url: str,
    timeout_sec: int = 60,
    session: requests.Session | None = None,
) -> Iterator[GdeltMasterEntry]:
    """
    Stream a remote GDELT master list and yield parsed entries.

    This avoids loading the full master list into memory.
    """
    client = session or requests

    logger.info("Streaming GDELT master list from {}", master_url)

    try:
        with client.get(master_url, stream=True, timeout=timeout_sec) as response:
            response.raise_for_status()

            for raw_line in response.iter_lines(decode_unicode=True):
                if raw_line is None:
                    continue

                entry = parse_masterlist_line(raw_line)
                if entry is not None:
                    yield entry

    except requests.RequestException as e:
        raise RuntimeError(f"Failed to stream master list from {master_url}") from e


# Check if needed
def iter_master_entries_from_file(
    master_file: str | Path,
) -> Iterator[GdeltMasterEntry]:
    """Read a local GDELT master list file line by line and yield parsed entries."""
    file_path = Path(master_file)

    logger.info("Reading GDELT master list from local file {}", file_path)

    with file_path.open("r", encoding="utf-8") as f:
        yield from iter_master_entries_from_lines(f)


def filter_master_entries(
    entries: Iterable[GdeltMasterEntry],
    *,
    start_ts: str,
    end_ts: str,
    file_types: Iterable[str] | None = None,
) -> Iterator[GdeltMasterEntry]:
    """
    Filter GDELT master entries by inclusive timestamp range and optional file types.

    Timestamps must use GDELT format YYYYMMDDHHMMSS.
    Since that format is lexicographically sortable, string comparison works.
    """
    allowed_file_types = None
    if file_types is not None:
        allowed_file_types = {file_type.lower() for file_type in file_types}

    for entry in entries:
        if allowed_file_types is not None and entry.file_type not in allowed_file_types:
            continue

        if start_ts <= entry.timestamp <= end_ts:
            yield entry


def iter_urls_in_range_from_url(
    *,
    master_url: str,
    start_ts: str,
    end_ts: str,
    file_types: Iterable[str] | None = None,
    timeout_sec: int = 60,
    session: requests.Session | None = None,
) -> Iterator[str]:
    """Stream a remote master list and yield matching URLs only."""
    entries = iter_master_entries_from_url(
        master_url=master_url,
        timeout_sec=timeout_sec,
        session=session,
    )
    for entry in filter_master_entries(
        entries,
        start_ts=start_ts,
        end_ts=end_ts,
        file_types=file_types,
    ):
        yield entry.url


# Check if needed
def iter_urls_in_range_from_file(
    *,
    master_file: str | Path,
    start_ts: str,
    end_ts: str,
    file_types: Iterable[str] | None = None,
) -> Iterator[str]:
    """Read a local master list file and yield matching URLs only."""
    entries = iter_master_entries_from_file(master_file)
    for entry in filter_master_entries(
        entries,
        start_ts=start_ts,
        end_ts=end_ts,
        file_types=file_types,
    ):
        yield entry.url
