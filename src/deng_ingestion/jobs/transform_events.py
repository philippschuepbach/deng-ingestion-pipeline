from __future__ import annotations

from deng_ingestion.pipeline.job import PipelineJob
from deng_ingestion.steps.silver import (
    SelectPendingSilverBatchStep,
    TransformAllPendingBatchesToSilverStep,
    TransformBatchToSilverStep,
    TransformRegisteredBatchesToSilverStep,
)


def build_transform_events_job() -> PipelineJob:
    return PipelineJob(
        name="transform_events",
        steps=[
            SelectPendingSilverBatchStep(),
            TransformBatchToSilverStep(),
        ],
    )


def build_transform_all_events_job() -> PipelineJob:
    return PipelineJob(
        name="transform_all_events",
        steps=[
            TransformAllPendingBatchesToSilverStep(),
        ],
    )


def build_transform_registered_events_job() -> PipelineJob:
    return PipelineJob(
        name="transform_registered_events",
        steps=[
            TransformRegisteredBatchesToSilverStep(),
        ],
    )
