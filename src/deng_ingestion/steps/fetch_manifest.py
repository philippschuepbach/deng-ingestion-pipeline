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
from urllib.request import urlopen

from deng_ingestion.pipeline.context import PipelineContext


@dataclass(frozen=True)
class FetchManifestStep:
    name: str
    manifest_url: str
    source_type: str

    def run(self, context: PipelineContext) -> None:
        with urlopen(self.manifest_url) as response:
            manifest_text = response.read().decode("utf-8")

        context.data["manifest_text"] = manifest_text
        context.data["manifest_source_type"] = self.source_type
