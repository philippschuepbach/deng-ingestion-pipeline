from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TypedDict


class BatchInfo(TypedDict):
    batch_id: int
    source_type: str
    file_type: str
    source_url: str
    file_name: str
    gdelt_timestamp: datetime
    status: str
    claimed_at: datetime | None
    claimed_by: str | None


class SilverBatchInfo(TypedDict):
    batch_id: int
    source_type: str
    file_type: str
    source_url: str
    file_name: str
    gdelt_timestamp: datetime
    status: str
    claimed_at: datetime | None
    claimed_by: str | None


class LookupLoadCounts(TypedDict, total=False):
    dim_fips_country_codes: int
    dim_cameo_country_codes: int
    dim_cameo_known_groups: int
    dim_cameo_event_roots: int
    dim_cameo_event_codes: int


BatchIdList = list[int]
PathValue = Path
