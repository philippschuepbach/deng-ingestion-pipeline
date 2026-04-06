from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.db.connection import get_context_connection
from deng_ingestion.db.pipeline_batches import CLAIM_TTL_MINUTES
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import set_current_silver_batch
from deng_ingestion.pipeline.context_types import SilverBatchInfo


@dataclass(frozen=True)
class SelectPendingSilverBatchStep:
    name: str = "select_pending_silver_batch"

    def run(self, context: PipelineContext) -> None:
        sql = """
        WITH candidate AS (
            SELECT pb.batch_id
            FROM pipeline_batches pb
            WHERE pb.file_type = 'export'
              AND pb.status = 'loaded'
              AND (
                  pb.claimed_at IS NULL
                  OR pb.claimed_at < NOW() - (%(claim_ttl_minutes)s * INTERVAL '1 minute')
              )
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
            logger.info("No pending silver batch found")
            set_current_silver_batch(context, None)
            return

        batch: SilverBatchInfo = {
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

        logger.info(
            "Claimed pending silver batch: batch_id={}, file_name={}, status={}, claimed_by={}",
            batch["batch_id"],
            batch["file_name"],
            batch["status"],
            batch["claimed_by"],
        )

        set_current_silver_batch(context, batch)
