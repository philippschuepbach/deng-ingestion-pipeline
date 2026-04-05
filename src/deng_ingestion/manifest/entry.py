from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ManifestEntry:
    file_size_bytes: int
    md5_hash: str
    source_url: str
    file_name: str
    gdelt_timestamp: datetime
    file_type: str
