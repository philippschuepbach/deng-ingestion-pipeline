from __future__ import annotations

from deng_ingestion.pipeline.job import PipelineJob
from deng_ingestion.steps.lookup.download_lookup_files import DownloadLookupFilesStep
from deng_ingestion.steps.lookup.load_lookup_dimensions import LoadLookupDimensionsStep
from deng_ingestion.steps.lookup.seed_risk_category_mapping import (
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
