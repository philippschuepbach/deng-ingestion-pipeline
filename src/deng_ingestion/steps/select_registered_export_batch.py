from __future__ import annotations

from dataclasses import dataclass

from deng_ingestion.pipeline.context import PipelineContext


@dataclass(frozen=True)
class SelectRegisteredExportBatchStep:
    name: str = "select_registered_export_batch"

    def run(self, context: PipelineContext) -> None:
        conn = context.data["db_connection"]

        remaining_batch_ids = context.data.get("remaining_registered_export_batch_ids")
        if remaining_batch_ids is None:
            remaining_batch_ids = list(
                context.data.get("registered_export_batch_ids", [])
            )
            context.data["remaining_registered_export_batch_ids"] = remaining_batch_ids

        if not remaining_batch_ids:
            context.data["current_batch"] = None
            return

        batch_id = remaining_batch_ids.pop(0)

        sql = """
        SELECT
            batch_id,
            file_name,
            source_url,
            status,
            gdelt_timestamp,
            downloaded_at,
            loaded_at
        FROM pipeline_batches
        WHERE batch_id = %(batch_id)s
          AND file_type = 'export'
        """

        with conn.cursor() as cursor:
            cursor.execute(sql, {"batch_id": batch_id})
            row = cursor.fetchone()

        if row is None:
            context.data["current_batch"] = None
            return

        context.data["current_batch"] = {
            "batch_id": row[0],
            "file_name": row[1],
            "source_url": row[2],
            "status": row[3],
            "gdelt_timestamp": row[4],
            "downloaded_at": row[5],
            "loaded_at": row[6],
        }
