from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.db.connection import get_context_connection
from deng_ingestion.pipeline.context import PipelineContext


@dataclass(frozen=True)
class SelectRegisteredSilverBatchStep:
    name: str = "select_registered_silver_batch"

    def run(self, context: PipelineContext) -> None:
        remaining_batch_ids = context.data.get("remaining_ingested_export_batch_ids")
        if remaining_batch_ids is None:
            remaining_batch_ids = list(
                context.data.get("ingested_export_batch_ids", [])
            )
            context.data["remaining_ingested_export_batch_ids"] = remaining_batch_ids

        if not remaining_batch_ids:
            logger.info("No registered silver batch found")
            context.data["current_silver_batch"] = None
            return

        sql = """
        SELECT
            pb.batch_id,
            pb.source_type,
            pb.file_type,
            pb.source_url,
            pb.file_name,
            pb.gdelt_timestamp,
            pb.status
        FROM pipeline_batches pb
        WHERE pb.batch_id = %(batch_id)s
          AND pb.file_type = 'export'
          AND pb.status = 'loaded'
          AND EXISTS (
              SELECT 1
              FROM events_bronze eb
              WHERE eb.batch_id = pb.batch_id
          )
          AND NOT EXISTS (
              SELECT 1
              FROM events_silver es
              WHERE es.batch_id = pb.batch_id
          )
        """

        conn, owns_connection = get_context_connection(context)

        try:
            while remaining_batch_ids:
                batch_id = remaining_batch_ids.pop(0)

                with conn.cursor() as cursor:
                    cursor.execute(sql, {"batch_id": batch_id})
                    row = cursor.fetchone()

                if row is None:
                    logger.info(
                        "Skipping registered silver batch because it is not transformable: batch_id={}",
                        batch_id,
                    )
                    continue

                batch = {
                    "batch_id": row[0],
                    "source_type": row[1],
                    "file_type": row[2],
                    "source_url": row[3],
                    "file_name": row[4],
                    "gdelt_timestamp": row[5],
                    "status": row[6],
                }

                logger.info(
                    "Selected registered silver batch: batch_id={}, file_name={}, status={}",
                    batch["batch_id"],
                    batch["file_name"],
                    batch["status"],
                )

                context.data["current_silver_batch"] = batch
                return
        finally:
            if owns_connection:
                conn.close()

        logger.info("No registered silver batch found")
        context.data["current_silver_batch"] = None
