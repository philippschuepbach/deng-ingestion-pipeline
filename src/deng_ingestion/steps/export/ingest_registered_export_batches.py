from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.db.connection import get_connection
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    clear_archive_path,
    clear_current_batch,
    clear_db_connection,
    clear_extracted_csv_path,
    clear_remaining_registered_export_batch_ids,
    get_current_batch,
    get_registered_export_batch_ids,
    set_db_connection,
    set_ingested_export_batch_ids,
    set_processed_batches,
)
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
        set_db_connection(context, conn)

        try:
            while True:
                clear_current_batch(context)
                clear_archive_path(context)
                clear_extracted_csv_path(context)

                select_step.run(context)

                current_batch = get_current_batch(context)
                if current_batch is None:
                    break

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
            clear_db_connection(context)
            clear_remaining_registered_export_batch_ids(context)
            conn.close()

        set_processed_batches(context, processed_batches)
        set_ingested_export_batch_ids(context, ingested_export_batch_ids)

        logger.info(
            "Finished ingest registered export batches step: requested_batches={}, processed_batches={}",
            len(get_registered_export_batch_ids(context)),
            processed_batches,
        )
