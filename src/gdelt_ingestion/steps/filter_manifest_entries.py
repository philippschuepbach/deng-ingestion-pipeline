# Copyright (C) 2026 Philipp Schüpbach
# This file is part of deng-ingestion-pipeline.
#
# deng-ingestion-pipeline is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# deng-ingestion-pipeline is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with deng-ingestion-pipeline. If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from gdelt_ingestion.pipeline.context import PipelineContext


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
