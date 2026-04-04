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

from deng_ingestion.pipeline.job import PipelineJob
from deng_ingestion.steps.download_export_archive import DownloadExportArchiveStep
from deng_ingestion.steps.extract_export_csv import ExtractExportCsvStep
from deng_ingestion.steps.ingest_all_pending_export_batches import (
    IngestAllPendingExportBatchesStep,
)
from deng_ingestion.steps.load_export_events_to_bronze import (
    LoadExportEventsToBronzeStep,
)
from deng_ingestion.steps.select_pending_export_batch import (
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
