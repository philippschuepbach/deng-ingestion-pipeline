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
    get_current_batch,
    set_db_connection,
    set_processed_batches,
)

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
        set_db_connection(context, conn)

        try:
            while True:
                _clear_iteration_state(context)

                select_step.run(context)

                current_batch = get_current_batch(context)
                if current_batch is None:
                    break

                _ingest_selected_batch(
                    context=context,
                    download_step=download_step,
                    extract_step=extract_step,
                    load_step=load_step,
                )

                processed_batches += 1
                logger.info(
                    "Processed export batch {}: batch_id={}, file_name={}",
                    processed_batches,
                    current_batch["batch_id"],
                    current_batch["file_name"],
                )

        finally:
            clear_db_connection(context)
            conn.close()

        set_processed_batches(context, processed_batches)

        logger.info(
            "Finished ingest-all export step: processed_batches={}",
            processed_batches,
        )


def _clear_iteration_state(context: PipelineContext) -> None:
    clear_current_batch(context)
    clear_archive_path(context)
    clear_extracted_csv_path(context)


def _ingest_selected_batch(
    *,
    context: PipelineContext,
    download_step: DownloadExportArchiveStep,
    extract_step: ExtractExportCsvStep,
    load_step: LoadExportEventsToBronzeStep,
) -> None:
    download_step.run(context)
    extract_step.run(context)
    load_step.run(context)
