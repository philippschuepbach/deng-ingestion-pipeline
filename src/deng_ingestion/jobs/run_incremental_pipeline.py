from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.jobs.build_risk_alerts_gold import build_risk_alerts_gold_job
from deng_ingestion.jobs.ingest_export_events import (
    build_ingest_registered_export_events_job,
)
from deng_ingestion.jobs.ingest_manifest import build_incremental_manifest_job
from deng_ingestion.jobs.transform_events import build_transform_registered_events_job
from deng_ingestion.pipeline.context import PipelineContext


@dataclass(frozen=True)
class IncrementalPipelineJob:
    name: str = "incremental_pipeline"

    def run(self, context: PipelineContext) -> None:
        logger.info("Starting incremental pipeline job")

        build_incremental_manifest_job().run(context)
        build_ingest_registered_export_events_job().run(context)
        build_transform_registered_events_job().run(context)
        build_risk_alerts_gold_job().run(context)

        logger.info("Finished incremental pipeline job")


def build_incremental_pipeline_job() -> IncrementalPipelineJob:
    return IncrementalPipelineJob()
