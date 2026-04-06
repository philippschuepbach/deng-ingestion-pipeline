from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.db.connection import get_connection
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    clear_current_silver_batch,
    clear_db_connection,
    clear_last_silver_inserted_rows,
    get_current_silver_batch,
    get_last_silver_inserted_rows,
    set_db_connection,
    set_processed_silver_batches,
)

from .select_pending_silver_batch import SelectPendingSilverBatchStep
from .transform_batch_to_silver import TransformBatchToSilverStep


@dataclass(frozen=True)
class TransformAllPendingBatchesToSilverStep:
    name: str = "transform_all_pending_batches_to_silver"

    def run(self, context: PipelineContext) -> None:
        select_step = SelectPendingSilverBatchStep()
        transform_step = TransformBatchToSilverStep()

        processed_batches = 0
        conn = get_connection()
        set_db_connection(context, conn)

        try:
            while True:
                clear_current_silver_batch(context)
                clear_last_silver_inserted_rows(context)

                select_step.run(context)

                current_batch = get_current_silver_batch(context)
                if current_batch is None:
                    break

                transform_step.run(context)

                processed_batches += 1
                logger.info(
                    (
                        "Processed pending silver batch:"
                        " batch_id={}, file_name={}, inserted_rows={}"
                    ),
                    current_batch["batch_id"],
                    current_batch["file_name"],
                    get_last_silver_inserted_rows(context) or 0,
                )
        finally:
            clear_db_connection(context)
            conn.close()

        set_processed_silver_batches(context, processed_batches)

        logger.info(
            "Finished transform-all silver step: processed_batches={}",
            processed_batches,
        )
