from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.request import urlopen

from loguru import logger

from deng_ingestion.db.connection import get_context_connection
from deng_ingestion.pipeline.context import PipelineContext


@dataclass(frozen=True)
class DownloadExportArchiveStep:
    name: str = "download_export_archive"

    def run(self, context: PipelineContext) -> None:
        batch = context.data.get("current_batch")
        if batch is None:
            logger.debug("Skipping archive download because no batch is selected")
            return

        archives_dir = context.working_dir / "data" / "raw" / "archives"
        archives_dir.mkdir(parents=True, exist_ok=True)

        archive_path = archives_dir / batch["file_name"]

        if not archive_path.exists():
            logger.info(
                "Downloading export archive: batch_id={}, url={}",
                batch["batch_id"],
                batch["source_url"],
            )

            with (
                urlopen(batch["source_url"]) as response,
                archive_path.open("wb") as target,
            ):
                target.write(response.read())
        else:
            logger.debug(
                "Archive already exists locally, reusing file: batch_id={}, path={}",
                batch["batch_id"],
                archive_path,
            )

        update_sql = """
        UPDATE pipeline_batches
        SET
            status = 'downloaded',
            downloaded_at = COALESCE(downloaded_at, NOW())
        WHERE batch_id = %(batch_id)s
        """

        conn, owns_connection = get_context_connection(context)

        try:
            with conn.cursor() as cursor:
                cursor.execute(update_sql, {"batch_id": batch["batch_id"]})
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            if owns_connection:
                conn.close()

        context.data["archive_path"] = archive_path
