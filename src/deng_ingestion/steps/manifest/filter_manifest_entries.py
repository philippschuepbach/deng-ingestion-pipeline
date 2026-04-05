from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from deng_ingestion.pipeline.context import PipelineContext


@dataclass(frozen=True)
class FilterManifestEntriesStep:
    name: str = "filter_manifest_entries"
    allowed_file_types: tuple[str, ...] = ("export",)
    date_from: datetime | None = None
    date_to: datetime | None = None

    def run(self, context: PipelineContext) -> None:
        entries = context.data["manifest_entries"]

        filtered = []
        for entry in entries:
            if entry.file_type not in self.allowed_file_types:
                continue

            if self.date_from is not None and entry.gdelt_timestamp < self.date_from:
                continue

            if self.date_to is not None and entry.gdelt_timestamp > self.date_to:
                continue

            filtered.append(entry)

        context.data["filtered_manifest_entries"] = filtered
