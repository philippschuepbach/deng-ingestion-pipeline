from __future__ import annotations

from deng_ingestion.pipeline.job import PipelineJob
from deng_ingestion.steps.export import (
    DownloadExportArchiveStep,
    ExtractExportCsvStep,
    IngestAllPendingExportBatchesStep,
    IngestRegisteredExportBatchesStep,
    LoadExportEventsToBronzeStep,
    SelectPendingExportBatchStep,
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
