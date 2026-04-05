from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.db.connection import get_connection
from deng_ingestion.pipeline.context import PipelineContext
from .download_export_archive import DownloadExportArchiveStep
from .extract_export_csv import ExtractExportCsvStep
from .load_export_events_to_bronze import LoadExportEventsToBronzeStep
from .select_registered_export_batch import SelectRegisteredExportBatchStep


@dataclass(frozen=True)
class IngestRegisteredExportBatchesStep:
    name: str = "ingest_registered_export_batches"

    def run(self, context: PipelineContext) -> None:
        select_step = SelectRegisteredExportBatchStep()
        download_step = DownloadExportArchiveStep()
        extract_step = ExtractExportCsvStep()
        load_step = LoadExportEventsToBronzeStep()

        processed_batches = 0
        ingested_export_batch_ids: list[int] = []

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

                if current_batch["loaded_at"] is not None:
                    logger.info(
                        "Skipping already loaded registered export batch: batch_id={}, file_name={}",
                        current_batch["batch_id"],
                        current_batch["file_name"],
                    )
                    continue

                download_step.run(context)
                extract_step.run(context)
                load_step.run(context)

                processed_batches += 1
                ingested_export_batch_ids.append(current_batch["batch_id"])

                logger.info(
                    "Processed registered export batch {}: batch_id={}, file_name={}",
                    processed_batches,
                    current_batch["batch_id"],
                    current_batch["file_name"],
                )

        finally:
            context.data.pop("db_connection", None)
            context.data.pop("remaining_registered_export_batch_ids", None)
            conn.close()

        context.data["processed_batches"] = processed_batches
        context.data["ingested_export_batch_ids"] = ingested_export_batch_ids

        logger.info(
            "Finished ingest registered export batches step: requested_batches={}, processed_batches={}",
            len(context.data.get("registered_export_batch_ids", [])),
            processed_batches,
        )
