from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.db.connection import get_context_connection
from deng_ingestion.pipeline.context import PipelineContext


@dataclass(frozen=True)
class SelectPendingExportBatchStep:
    name: str = "select_pending_export_batch"

    def run(self, context: PipelineContext) -> None:
        sql = """
        SELECT
            batch_id,
            source_type,
            file_type,
            source_url,
            file_name,
            gdelt_timestamp,
            status
        FROM pipeline_batches
        WHERE file_type = 'export'
          AND status IN ('discovered', 'downloaded')
        ORDER BY gdelt_timestamp ASC
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
            logger.info("No pending export batch found")
            context.data["current_batch"] = None
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
            "Selected pending export batch: batch_id={}, file_name={}, status={}",
            batch["batch_id"],
            batch["file_name"],
            batch["status"],
        )

        context.data["current_batch"] = batch
