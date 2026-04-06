from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.db.connection import get_connection
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    clear_current_silver_batch,
    clear_db_connection,
    clear_last_silver_inserted_rows,
    clear_remaining_ingested_export_batch_ids,
    get_current_silver_batch,
    get_ingested_export_batch_ids,
    get_last_silver_inserted_rows,
    set_db_connection,
    set_processed_silver_batches,
    set_transformed_export_batch_ids,
)

from .select_registered_silver_batch import SelectRegisteredSilverBatchStep
from .transform_batch_to_silver import TransformBatchToSilverStep


@dataclass(frozen=True)
class TransformRegisteredBatchesToSilverStep:
    name: str = "transform_registered_batches_to_silver"

    def run(self, context: PipelineContext) -> None:
        select_step = SelectRegisteredSilverBatchStep()
        transform_step = TransformBatchToSilverStep()

        processed_batches = 0
        transformed_export_batch_ids: list[int] = []

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
                transformed_export_batch_ids.append(current_batch["batch_id"])

                logger.debug(
                    (
                        "Processed registered silver batch {}:"
                        " batch_id={}, file_name={}, inserted_rows={}"
                    ),
                    processed_batches,
                    current_batch["batch_id"],
                    current_batch["file_name"],
                    get_last_silver_inserted_rows(context) or 0,
                )
        finally:
            clear_db_connection(context)
            clear_remaining_ingested_export_batch_ids(context)
            conn.close()

        set_processed_silver_batches(context, processed_batches)
        set_transformed_export_batch_ids(context, transformed_export_batch_ids)

        logger.debug(
            (
                "Finished transform registered silver step:"
                " requested_batches={}, processed_batches={}"
            ),
            len(get_ingested_export_batch_ids(context)),
            processed_batches,
        )
