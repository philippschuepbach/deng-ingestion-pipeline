from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path

from deng_ingestion.manifest.entry import ManifestEntry

_MANIFEST_LINE_RE = re.compile(r"^(\d+)\s+([a-fA-F0-9]+)\s+(https?://\S+)$")


def parse_manifest_line(line: str) -> ManifestEntry | None:
    line = line.strip()
    if not line:
        return None

    match = _MANIFEST_LINE_RE.match(line)
    if not match:
        return None

    file_size_bytes_str, md5_hash, source_url = match.groups()
    file_name = Path(source_url).name

    parts = file_name.split(".")
    if len(parts) < 4:
        raise ValueError(f"Unexpected GDELT file name format: {file_name}")

    timestamp_str = parts[0]
    file_type = parts[1].lower()

    gdelt_timestamp = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S").replace(
        tzinfo=UTC
    )

    return ManifestEntry(
        file_size_bytes=int(file_size_bytes_str),
        md5_hash=md5_hash,
        source_url=source_url,
        file_name=file_name,
        gdelt_timestamp=gdelt_timestamp,
        file_type=file_type,
    )
