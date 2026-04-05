from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.db.connection import get_connection
from deng_ingestion.pipeline.context import PipelineContext
from .download_export_archive import DownloadExportArchiveStep
from .extract_export_csv import ExtractExportCsvStep
from .load_export_events_to_bronze import LoadExportEventsToBronzeStep
from .select_pending_export_batch import SelectPendingExportBatchStep


@dataclass(frozen=True)
class IngestAllPendingExportBatchesStep:
    name: str = "ingest_all_pending_export_batches"

    def run(self, context: PipelineContext) -> None:
        select_step = SelectPendingExportBatchStep()
        download_step = DownloadExportArchiveStep()
        extract_step = ExtractExportCsvStep()
        load_step = LoadExportEventsToBronzeStep()

        processed_batches = 0
        conn = get_connection()
        context.data["db_connection"] = conn

        try:
            while True:
                context.data.pop("current_batch", None)
                context.data.pop("archive_path", None)
                context.data.pop("extracted_csv_path", None)

                select_step.run(context)

                current_batch = context.data.get("current_batch")
                if current_batch is None:
                    break

                download_step.run(context)
                extract_step.run(context)
                load_step.run(context)

                processed_batches += 1
                logger.info(
                    "Processed export batch {}: batch_id={}, file_name={}",
                    processed_batches,
                    current_batch["batch_id"],
                    current_batch["file_name"],
                )

        finally:
            context.data.pop("db_connection", None)
            conn.close()

        context.data["processed_batches"] = processed_batches

        logger.info(
            "Finished ingest-all export step: processed_batches={}",
            processed_batches,
        )
