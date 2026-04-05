from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime
from typing import Any


@dataclass
class PipelineContext:
    run_id: str
    execution_ts: datetime
    working_dir: Path
    data: dict[str, Any] = field(default_factory=dict)
