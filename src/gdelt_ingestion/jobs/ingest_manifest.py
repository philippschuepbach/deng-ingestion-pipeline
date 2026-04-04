# Copyright (C) 2026 Philipp Schüpbach
# This file is part of gdelt-ingestion-pipeline.
#
# gdelt-ingestion-pipeline is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# gdelt-ingestion-pipeline is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with gdelt-ingestion-pipeline. If not, see <https://www.gnu.org/licenses/>.

# src/gdelt_ingestion/jobs/ingest_manifest.py

from __future__ import annotations

from datetime import datetime

from gdelt_ingestion.pipeline.job import PipelineJob
from gdelt_ingestion.steps.fetch_manifest import FetchManifestStep
from gdelt_ingestion.steps.parse_manifest_entries import ParseManifestEntriesStep
from gdelt_ingestion.steps.filter_manifest_entries import FilterManifestEntriesStep
from gdelt_ingestion.steps.register_manifest_batches import RegisterManifestBatchesStep

LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
MASTERFILELIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"


def build_ingest_manifest_job(
    manifest_url: str,
    source_type: str,
    allowed_file_types: tuple[str, ...] = ("export",),
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> PipelineJob:
    return PipelineJob(
        name="ingest_manifest",
        steps=[
            FetchManifestStep(
                name=f"fetch_{source_type}_manifest",
                manifest_url=manifest_url,
                source_type=source_type,
            ),
            ParseManifestEntriesStep(),
            FilterManifestEntriesStep(
                allowed_file_types=allowed_file_types,
                date_from=date_from,
                date_to=date_to,
            ),
            RegisterManifestBatchesStep(),
        ],
    )


def build_incremental_manifest_job() -> PipelineJob:
    return build_ingest_manifest_job(
        manifest_url=LASTUPDATE_URL,
        source_type="lastupdate",
        allowed_file_types=("export",),
    )


def build_backfill_manifest_job(
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> PipelineJob:
    return build_ingest_manifest_job(
        manifest_url=MASTERFILELIST_URL,
        source_type="masterfilelist",
        allowed_file_types=("export",),
        date_from=date_from,
        date_to=date_to,
    )
