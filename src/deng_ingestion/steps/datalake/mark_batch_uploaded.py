from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.db.connection import get_connection
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    get_archive_object_path,
    get_csv_object_path,
    get_current_batch,
)


@dataclass(frozen=True)
class MarkBatchUploadedStep:
    name: str = "mark_batch_uploaded"

    def run(self, context: PipelineContext) -> None:
        current_batch = get_current_batch(context)
        if current_batch is None:
            raise ValueError("Expected current batch in pipeline context")

        batch_id = current_batch.get("batch_id")
        if batch_id is None:
            raise ValueError("Expected batch_id in current batch")

        archive_object_path = get_archive_object_path(context)
        if archive_object_path is None:
            raise ValueError("Expected archive_object_path in pipeline context")

        csv_object_path = get_csv_object_path(context)
        if csv_object_path is None:
            raise ValueError("Expected csv_object_path in pipeline context")

        logger.debug(
            "Marking batch as uploaded: batch_id={},"
            " archive_object_path={}, csv_object_path={}",
            batch_id,
            archive_object_path,
            csv_object_path,
        )

        sql = """
        UPDATE pipeline_batches
        SET
            archive_object_path = %(archive_object_path)s,
            csv_object_path = %(csv_object_path)s,
            uploaded_at = NOW(),
            status = 'uploaded',
            error_message = NULL
        WHERE batch_id = %(batch_id)s
        """

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    {
                        "batch_id": batch_id,
                        "archive_object_path": archive_object_path,
                        "csv_object_path": csv_object_path,
                    },
                )
            conn.commit()
