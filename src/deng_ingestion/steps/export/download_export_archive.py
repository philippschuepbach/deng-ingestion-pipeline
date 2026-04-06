from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.core.http import download_binary_to_file
from deng_ingestion.db.connection import get_context_connection
from deng_ingestion.db.pipeline_batches import (
    mark_batch_downloaded,
    mark_batch_failed,
)
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    get_current_batch,
    set_archive_path,
)


@dataclass(frozen=True)
class DownloadExportArchiveStep:
    name: str = "download_export_archive"

    def run(self, context: PipelineContext) -> None:
        batch = get_current_batch(context)
        if batch is None:
            logger.debug("Skipping archive download because no batch is selected")
            return

        archives_dir = context.working_dir / "data" / "raw" / "archives"
        archives_dir.mkdir(parents=True, exist_ok=True)

        archive_path = archives_dir / batch["file_name"]

        conn, owns_connection = get_context_connection(context)

        try:
            if not archive_path.exists():
                logger.info(
                    "Downloading export archive: batch_id={}, url={}",
                    batch["batch_id"],
                    batch["source_url"],
                )

                download_binary_to_file(
                    batch["source_url"],
                    archive_path,
                    timeout_seconds=30.0,
                    retries=3,
                )
            else:
                logger.debug(
                    "Archive already exists locally, reusing file:"
                    " batch_id={}, path={}",
                    batch["batch_id"],
                    archive_path,
                )

            mark_batch_downloaded(conn, batch["batch_id"])
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

        set_archive_path(context, archive_path)
