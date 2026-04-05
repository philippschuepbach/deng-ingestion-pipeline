from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.db.connection import get_context_connection
from deng_ingestion.pipeline.context import PipelineContext

CLAIM_TTL_MINUTES = 30


@dataclass(frozen=True)
class SelectRegisteredExportBatchStep:
    name: str = "select_registered_export_batch"

    def run(self, context: PipelineContext) -> None:
        remaining_batch_ids = context.data.get("remaining_registered_export_batch_ids")
        if remaining_batch_ids is None:
            remaining_batch_ids = list(
                context.data.get("registered_export_batch_ids", [])
            )
            context.data["remaining_registered_export_batch_ids"] = remaining_batch_ids

        if not remaining_batch_ids:
            logger.info("No registered export batch found")
            context.data["current_batch"] = None
            return

        sql = """
        UPDATE pipeline_batches
        SET
            claimed_at = NOW(),
            claimed_by = %(claimed_by)s,
            error_message = NULL
        WHERE batch_id = %(batch_id)s
          AND file_type = 'export'
          AND status IN ('discovered', 'downloaded')
          AND (
              claimed_at IS NULL
              OR claimed_at < NOW() - (%(claim_ttl_minutes)s * INTERVAL '1 minute')
          )
        RETURNING
            batch_id,
            source_type,
            file_type,
            source_url,
            file_name,
            gdelt_timestamp,
            status,
            claimed_at,
            claimed_by
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
                    logger.info(
                        "Skipping registered export batch because it is not claimable: batch_id={}",
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
                    "claimed_at": row[7],
                    "claimed_by": row[8],
                }

                logger.info(
                    "Claimed registered export batch: batch_id={}, file_name={}, status={}, claimed_by={}",
                    batch["batch_id"],
                    batch["file_name"],
                    batch["status"],
                    batch["claimed_by"],
                )

                context.data["current_batch"] = batch
                return

        except Exception:
            conn.rollback()
            raise

        finally:
            if owns_connection:
                conn.close()

        logger.info("No registered export batch found")
        context.data["current_batch"] = None
