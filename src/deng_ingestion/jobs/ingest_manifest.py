from __future__ import annotations

from datetime import datetime

from deng_ingestion.pipeline.job import PipelineJob
from deng_ingestion.steps.fetch_manifest import FetchManifestStep
from deng_ingestion.steps.parse_manifest_entries import ParseManifestEntriesStep
from deng_ingestion.steps.filter_manifest_entries import FilterManifestEntriesStep
from deng_ingestion.steps.register_manifest_batches import RegisterManifestBatchesStep
from deng_ingestion.steps.register_manifest_batches_for_current_run import (
    RegisterManifestBatchesForCurrentRunStep,
)

LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
MASTERFILELIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"


def build_ingest_manifest_job(
    manifest_url: str,
    source_type: str,
    allowed_file_types: tuple[str, ...] = ("export",),
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    register_step: (
        RegisterManifestBatchesStep | RegisterManifestBatchesForCurrentRunStep | None
    ) = None,
) -> PipelineJob:
    if register_step is None:
        register_step = RegisterManifestBatchesStep()

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
            register_step,
        ],
    )


def build_incremental_manifest_job() -> PipelineJob:
    return build_ingest_manifest_job(
        manifest_url=LASTUPDATE_URL,
        source_type="lastupdate",
        allowed_file_types=("export",),
        register_step=RegisterManifestBatchesForCurrentRunStep(),
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
        register_step=RegisterManifestBatchesStep(),
    )
