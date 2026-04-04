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

from __future__ import annotations

from gdelt_ingestion.pipeline.job import PipelineJob
from gdelt_ingestion.steps.download_export_archive import DownloadExportArchiveStep
from gdelt_ingestion.steps.extract_export_csv import ExtractExportCsvStep
from gdelt_ingestion.steps.ingest_all_pending_export_batches import (
    IngestAllPendingExportBatchesStep,
)
from gdelt_ingestion.steps.load_export_events_to_bronze import (
    LoadExportEventsToBronzeStep,
)
from gdelt_ingestion.steps.select_pending_export_batch import (
    SelectPendingExportBatchStep,
)


def build_ingest_export_events_job() -> PipelineJob:
    return PipelineJob(
        name="ingest_export_events",
        steps=[
            SelectPendingExportBatchStep(),
            DownloadExportArchiveStep(),
            ExtractExportCsvStep(),
            LoadExportEventsToBronzeStep(),
        ],
    )


def build_ingest_all_export_events_job() -> PipelineJob:
    return PipelineJob(
        name="ingest_all_export_events",
        steps=[
            IngestAllPendingExportBatchesStep(),
        ],
    )
