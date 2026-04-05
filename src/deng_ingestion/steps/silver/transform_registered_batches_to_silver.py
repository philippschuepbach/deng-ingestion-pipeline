from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.db.connection import get_connection
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.steps.silver import (
    SelectRegisteredSilverBatchStep,
    TransformBatchToSilverStep,
)


@dataclass(frozen=True)
class TransformRegisteredBatchesToSilverStep:
    name: str = "transform_registered_batches_to_silver"

    def run(self, context: PipelineContext) -> None:
        select_step = SelectRegisteredSilverBatchStep()
        transform_step = TransformBatchToSilverStep()

        processed_batches = 0
        transformed_export_batch_ids: list[int] = []

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
                transformed_export_batch_ids.append(current_batch["batch_id"])

                logger.info(
                    "Processed registered silver batch {}: batch_id={}, file_name={}, inserted_rows={}",
                    processed_batches,
                    current_batch["batch_id"],
                    current_batch["file_name"],
                    context.data.get("last_silver_inserted_rows", 0),
                )
        finally:
            context.data.pop("db_connection", None)
            context.data.pop("remaining_ingested_export_batch_ids", None)
            conn.close()

        context.data["processed_silver_batches"] = processed_batches
        context.data["transformed_export_batch_ids"] = transformed_export_batch_ids

        logger.info(
            "Finished transform registered silver step: requested_batches={}, processed_batches={}",
            len(context.data.get("ingested_export_batch_ids", [])),
            processed_batches,
        )
