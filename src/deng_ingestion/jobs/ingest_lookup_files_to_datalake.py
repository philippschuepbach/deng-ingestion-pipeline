from __future__ import annotations

from dataclasses import dataclass, field

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.steps.datalake.build_object_paths import BuildLookupObjectPathsStep
from deng_ingestion.steps.datalake.upload_lookup_files_to_datalake import (
    UploadLookupFilesToDatalakeStep,
)
from deng_ingestion.steps.lookup.download_lookup_files import DownloadLookupFilesStep


@dataclass(frozen=True)
class IngestLookupFilesToDatalakeJob:
    name: str = "ingest_lookup_files_to_datalake"

    download_lookup_files_step: DownloadLookupFilesStep = field(
        default_factory=DownloadLookupFilesStep
    )
    build_lookup_object_paths_step: BuildLookupObjectPathsStep = field(
        default_factory=BuildLookupObjectPathsStep
    )
    upload_lookup_files_step: UploadLookupFilesToDatalakeStep = field(
        default_factory=UploadLookupFilesToDatalakeStep
    )

    def run(self, context: PipelineContext) -> None:
        self.download_lookup_files_step.run(context)
        self.build_lookup_object_paths_step.run(context)
        self.upload_lookup_files_step.run(context)


def build_ingest_lookup_files_to_datalake_job() -> IngestLookupFilesToDatalakeJob:
    return IngestLookupFilesToDatalakeJob()
