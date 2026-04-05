from __future__ import annotations

from deng_ingestion.pipeline.job import PipelineJob
from deng_ingestion.steps.lookup import (
    DownloadLookupFilesStep,
    LoadLookupDimensionsStep,
    SeedRiskCategoryMappingStep,
)


def build_load_lookups_job() -> PipelineJob:
    return PipelineJob(
        name="load_lookups",
        steps=[
            DownloadLookupFilesStep(),
            LoadLookupDimensionsStep(),
            SeedRiskCategoryMappingStep(),
        ],
    )
