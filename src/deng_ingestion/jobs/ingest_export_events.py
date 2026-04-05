from __future__ import annotations

from deng_ingestion.pipeline.job import PipelineJob
from deng_ingestion.steps.download_export_archive import DownloadExportArchiveStep
from deng_ingestion.steps.extract_export_csv import ExtractExportCsvStep
from deng_ingestion.steps.ingest_all_pending_export_batches import (
    IngestAllPendingExportBatchesStep,
)
from deng_ingestion.steps.ingest_registered_export_batches import (
    IngestRegisteredExportBatchesStep,
)
from deng_ingestion.steps.load_export_events_to_bronze import (
    LoadExportEventsToBronzeStep,
)
from deng_ingestion.steps.select_pending_export_batch import (
    SelectPendingExportBatchStep,
)
from deng_ingestion.steps.select_registered_export_batch import (
    SelectRegisteredExportBatchStep,
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


def build_ingest_registered_export_events_job() -> PipelineJob:
    return PipelineJob(
        name="ingest_registered_export_events",
        steps=[
            IngestRegisteredExportBatchesStep(),
        ],
    )


def build_ingest_registered_export_event_job() -> PipelineJob:
    return PipelineJob(
        name="ingest_registered_export_event",
        steps=[
            SelectRegisteredExportBatchStep(),
            DownloadExportArchiveStep(),
            ExtractExportCsvStep(),
            LoadExportEventsToBronzeStep(),
        ],
    )
