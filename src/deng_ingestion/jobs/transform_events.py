from __future__ import annotations

from deng_ingestion.pipeline.job import PipelineJob
from deng_ingestion.steps.select_pending_silver_batch import (
    SelectPendingSilverBatchStep,
)
from deng_ingestion.steps.transform_all_pending_batches_to_silver import (
    TransformAllPendingBatchesToSilverStep,
)
from deng_ingestion.steps.transform_batch_to_silver import TransformBatchToSilverStep


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
