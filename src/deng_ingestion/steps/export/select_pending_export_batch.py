from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.db.connection import get_context_connection
from deng_ingestion.db.pipeline_batches import CLAIM_TTL_MINUTES
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import set_current_batch
from deng_ingestion.pipeline.context_types import BatchInfo


@dataclass(frozen=True)
class SelectPendingExportBatchStep:
    name: str = "select_pending_export_batch"

    def run(self, context: PipelineContext) -> None:
        sql = """
        WITH candidate AS (
            SELECT
                batch_id
            FROM pipeline_batches
            WHERE file_type = 'export'
              AND status IN ('discovered', 'downloaded')
              AND (
                  claimed_at IS NULL
                  OR claimed_at < NOW() - (%(claim_ttl_minutes)s * INTERVAL '1 minute')
              )
            ORDER BY gdelt_timestamp ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        UPDATE pipeline_batches pb
        SET
            claimed_at = NOW(),
            claimed_by = %(claimed_by)s,
            error_message = NULL
        FROM candidate
        WHERE pb.batch_id = candidate.batch_id
        RETURNING
            pb.batch_id,
            pb.source_type,
            pb.file_type,
            pb.source_url,
            pb.file_name,
            pb.gdelt_timestamp,
            pb.status,
            pb.claimed_at,
            pb.claimed_by
        """

        conn, owns_connection = get_context_connection(context)

        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    {
                        "claimed_by": context.run_id,
                        "claim_ttl_minutes": CLAIM_TTL_MINUTES,
                    },
                )
                row = cursor.fetchone()
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            if owns_connection:
                conn.close()

        if row is None:
            logger.debug("No pending export batch found")
            set_current_batch(context, None)
            return

        batch: BatchInfo = {
            "batch_id": row[0],
            "source_type": row[1],
            "file_type": row[2],
            "source_url": row[3],
            "file_name": row[4],
            "gdelt_timestamp": row[5],
            "status": row[6],
            "claimed_at": row[7],
            "claimed_by": row[8],
        }

        logger.debug(
            (
                "Claimed pending export batch:"
                " batch_id={}, file_name={}, status={}, claimed_by={}"
            ),
            batch["batch_id"],
            batch["file_name"],
            batch["status"],
            batch["claimed_by"],
        )

        set_current_batch(context, batch)
