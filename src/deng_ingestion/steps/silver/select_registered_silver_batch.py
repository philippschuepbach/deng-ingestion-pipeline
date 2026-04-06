from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.db.connection import get_context_connection
from deng_ingestion.db.pipeline_batches import CLAIM_TTL_MINUTES
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    get_ingested_export_batch_ids,
    get_remaining_ingested_export_batch_ids,
    set_current_silver_batch,
    set_remaining_ingested_export_batch_ids,
)
from deng_ingestion.pipeline.context_types import SilverBatchInfo


@dataclass(frozen=True)
class SelectRegisteredSilverBatchStep:
    name: str = "select_registered_silver_batch"

    def run(self, context: PipelineContext) -> None:
        remaining_batch_ids = get_remaining_ingested_export_batch_ids(context)
        if remaining_batch_ids is None:
            remaining_batch_ids = list(get_ingested_export_batch_ids(context))
            set_remaining_ingested_export_batch_ids(context, remaining_batch_ids)

        if not remaining_batch_ids:
            logger.debug("No registered silver batch found")
            set_current_silver_batch(context, None)
            return

        sql = """
        UPDATE pipeline_batches pb
        SET
            claimed_at = NOW(),
            claimed_by = %(claimed_by)s,
            error_message = NULL
        WHERE pb.batch_id = %(batch_id)s
          AND pb.file_type = 'export'
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
            while remaining_batch_ids:
                batch_id = remaining_batch_ids.pop(0)

                with conn.cursor() as cursor:
                    cursor.execute(
                        sql,
                        {
                            "batch_id": batch_id,
                            "claimed_by": context.run_id,
                            "claim_ttl_minutes": CLAIM_TTL_MINUTES,
                        },
                    )
                    row = cursor.fetchone()

                conn.commit()

                if row is None:
                    logger.debug(
                        (
                            "Skipping registered silver batch "
                            "because it is not claimable:"
                            " batch_id={}"
                        ),
                        batch_id,
                    )
                    continue

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

                logger.debug(
                    (
                        "Claimed registered silver batch:"
                        " batch_id={}, file_name={}, status={}, claimed_by={}"
                    ),
                    batch["batch_id"],
                    batch["file_name"],
                    batch["status"],
                    batch["claimed_by"],
                )

                set_current_silver_batch(context, batch)
                return

        except Exception:
            conn.rollback()
            raise

        finally:
            if owns_connection:
                conn.close()

        logger.debug("No registered silver batch found")
        set_current_silver_batch(context, None)
