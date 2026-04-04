# Copyright (C) 2026 Philipp Schüpbach
# This file is part of gdelt-ingestion-pipeline.
#
# gdelt-ingestion-pipeline is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# gdelt-ingestion-pipeline is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with gdelt-ingestion-pipeline. If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Sequence
from loguru import logger

from gdelt_ingestion.pipeline.context import PipelineContext
from gdelt_ingestion.pipeline.types import PipelineStep


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
