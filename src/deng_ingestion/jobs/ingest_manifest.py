from __future__ import annotations

from datetime import datetime

from deng_ingestion.pipeline.job import PipelineJob
from deng_ingestion.steps.manifest import (
    FetchManifestStep,
    FilterManifestEntriesStep,
    ParseManifestEntriesStep,
    RegisterManifestBatchesForCurrentRunStep,
    RegisterManifestBatchesStep,
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
