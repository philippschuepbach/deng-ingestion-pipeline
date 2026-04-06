from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.db.connection import get_context_connection
from deng_ingestion.db.pipeline_batches import (
    clear_batch_claim,
    mark_batch_failed,
)
from deng_ingestion.db.sql_loader import load_sql
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    get_current_silver_batch,
    set_last_silver_inserted_rows,
)


@dataclass(frozen=True)
class TransformBatchToSilverStep:
    name: str = "transform_batch_to_silver"

    def run(self, context: PipelineContext) -> None:
        batch = get_current_silver_batch(context)
        if batch is None:
            logger.debug("Skipping silver transformation because no batch is selected")
            return

        logger.info(
            "Transforming bronze batch to silver: batch_id={}, file_name={}",
            batch["batch_id"],
            batch["file_name"],
        )
        insert_sql = load_sql("silver", "transform_batch_to_silver.sql")

        conn, owns_connection = get_context_connection(context)

        try:
            with conn.cursor() as cursor:
                cursor.execute(insert_sql, {"batch_id": batch["batch_id"]})
                inserted_rows = cursor.rowcount

            clear_batch_claim(conn, batch["batch_id"])
            conn.commit()

        except Exception as exc:
            conn.rollback()

            try:
                mark_batch_failed(conn, batch["batch_id"], exc)
                conn.commit()
            except Exception:
                conn.rollback()
                logger.exception(
                    "Failed to persist failed batch state: batch_id={}",
                    batch["batch_id"],
                )

            raise

        finally:
            if owns_connection:
                conn.close()

        set_last_silver_inserted_rows(context, inserted_rows)

        logger.info(
            "Finished silver transformation for batch_id={}, inserted_rows={}",
            batch["batch_id"],
            inserted_rows,
        )
