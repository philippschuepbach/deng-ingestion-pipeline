from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Sequence
from loguru import logger

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.types import PipelineStep


@dataclass(frozen=True)
class PipelineJob:
    name: str
    steps: Sequence[PipelineStep]

    def run(self, context: PipelineContext) -> None:
        logger.info("Starting pipeline job: {}", self.name)

        for step in self.steps:
            logger.info("Running pipeline step: {}", step.name)
            try:
                step.run(context)
            except Exception:
                logger.exception("Pipeline step failed: {}", step.name)
                raise

        logger.info("Finished pipeline job: {}", self.name)
