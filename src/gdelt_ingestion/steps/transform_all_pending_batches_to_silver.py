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

from loguru import logger

from gdelt_ingestion.db.connection import get_connection
from gdelt_ingestion.pipeline.context import PipelineContext
from gdelt_ingestion.steps.select_pending_silver_batch import (
    SelectPendingSilverBatchStep,
)
from gdelt_ingestion.steps.transform_batch_to_silver import TransformBatchToSilverStep


@dataclass(frozen=True)
class TransformAllPendingBatchesToSilverStep:
    name: str = "transform_all_pending_batches_to_silver"

    def run(self, context: PipelineContext) -> None:
        select_step = SelectPendingSilverBatchStep()
        transform_step = TransformBatchToSilverStep()

        processed_batches = 0
        conn = get_connection()
        context.data["db_connection"] = conn

        try:
            while True:
                context.data.pop("current_silver_batch", None)
                context.data.pop("last_silver_inserted_rows", None)

                select_step.run(context)

                current_batch = context.data.get("current_silver_batch")
                if current_batch is None:
                    break

                transform_step.run(context)

                processed_batches += 1
                logger.info(
                    "Processed silver batch {}: batch_id={}, file_name={}, inserted_rows={}",
                    processed_batches,
                    current_batch["batch_id"],
                    current_batch["file_name"],
                    context.data.get("last_silver_inserted_rows", 0),
                )
        finally:
            context.data.pop("db_connection", None)
            conn.close()

        context.data["processed_silver_batches"] = processed_batches

        logger.info(
            "Finished transform-all silver step: processed_batches={}",
            processed_batches,
        )
