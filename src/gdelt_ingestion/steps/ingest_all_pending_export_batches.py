# Copyright (C) 2026 Philipp Schüpbach
# This file is part of deng-ingestion-pipeline.
#
# deng-ingestion-pipeline is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# deng-ingestion-pipeline is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with deng-ingestion-pipeline. If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from gdelt_ingestion.db.connection import get_connection
from gdelt_ingestion.pipeline.context import PipelineContext
from gdelt_ingestion.steps.download_export_archive import DownloadExportArchiveStep
from gdelt_ingestion.steps.extract_export_csv import ExtractExportCsvStep
from gdelt_ingestion.steps.load_export_events_to_bronze import (
    LoadExportEventsToBronzeStep,
)
from gdelt_ingestion.steps.select_pending_export_batch import (
    SelectPendingExportBatchStep,
)


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
