from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.db.connection import get_context_connection
from deng_ingestion.pipeline.context import PipelineContext


@dataclass(frozen=True)
class SelectPendingSilverBatchStep:
    name: str = "select_pending_silver_batch"

    def run(self, context: PipelineContext) -> None:
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
        WHERE pb.file_type = 'export'
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
        ORDER BY pb.gdelt_timestamp ASC
        LIMIT 1
        """

        conn, owns_connection = get_context_connection(context)

        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                row = cursor.fetchone()
        finally:
            if owns_connection:
                conn.close()

        if row is None:
            logger.info("No pending silver batch found")
            context.data["current_silver_batch"] = None
            return

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
            "Selected pending silver batch: batch_id={}, file_name={}, status={}",
            batch["batch_id"],
            batch["file_name"],
            batch["status"],
        )

        context.data["current_silver_batch"] = batch
