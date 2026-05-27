from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.steps.datalake.ingest_manifest_entries_to_datalake import (
    IngestManifestEntriesToDatalakeStep,
)
from deng_ingestion.steps.manifest.fetch_manifest import FetchManifestStep
from deng_ingestion.steps.manifest.filter_manifest_entries import (
    FilterManifestEntriesStep,
)
from deng_ingestion.steps.manifest.parse_manifest_entries import (
    ParseManifestEntriesStep,
)

GDELT_LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
GDELT_MASTERFILELIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"


@dataclass(frozen=True)
class IngestDatalakeDirectJob:
    fetch_manifest_step: FetchManifestStep

    name: str = "ingest_datalake_direct"
    parse_manifest_entries_step: ParseManifestEntriesStep = field(
        default_factory=ParseManifestEntriesStep
    )
    filter_manifest_entries_step: FilterManifestEntriesStep = field(
        default_factory=FilterManifestEntriesStep
    )
    ingest_manifest_entries_step: IngestManifestEntriesToDatalakeStep = field(
        default_factory=IngestManifestEntriesToDatalakeStep
    )

    def run(self, context: PipelineContext) -> None:
        self.fetch_manifest_step.run(context)
        self.parse_manifest_entries_step.run(context)
        self.filter_manifest_entries_step.run(context)
        self.ingest_manifest_entries_step.run(context)


def build_incremental_datalake_direct_job() -> IngestDatalakeDirectJob:
    return IngestDatalakeDirectJob(
        fetch_manifest_step=FetchManifestStep(
            name="fetch_lastupdate_manifest",
            manifest_url=GDELT_LASTUPDATE_URL,
            source_type="lastupdate",
        ),
        filter_manifest_entries_step=FilterManifestEntriesStep(
            allowed_file_types=("export",),
        ),
    )


def build_backfill_datalake_direct_job(
    date_from: datetime,
    date_to: datetime,
) -> IngestDatalakeDirectJob:
    return IngestDatalakeDirectJob(
        fetch_manifest_step=FetchManifestStep(
            name="fetch_masterfilelist_manifest",
            manifest_url=GDELT_MASTERFILELIST_URL,
            source_type="masterfilelist",
        ),
        filter_manifest_entries_step=FilterManifestEntriesStep(
            allowed_file_types=("export",),
            date_from=date_from,
            date_to=date_to,
        ),
    )
