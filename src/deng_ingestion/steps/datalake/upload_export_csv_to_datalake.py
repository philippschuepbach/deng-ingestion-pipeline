from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    get_csv_object_path,
    get_current_batch,
    get_extracted_csv_path,
)
from deng_ingestion.steps.datalake.object_storage import upload_file


@dataclass(frozen=True)
class UploadExportCsvToDatalakeStep:
    name: str = "upload_export_csv_to_datalake"

    def run(self, context: PipelineContext) -> None:
        batch = get_current_batch(context)
        if batch is None:
            logger.debug("Skipping CSV upload because no batch is selected")
            return

        extracted_csv_path = get_extracted_csv_path(context)
        if extracted_csv_path is None:
            raise ValueError("Expected extracted_csv_path in pipeline context")

        csv_object_path = get_csv_object_path(context)
        if csv_object_path is None:
            raise ValueError("Expected csv_object_path in pipeline context")

        if not extracted_csv_path.exists():
            raise ValueError(f"Missing extracted CSV file: {extracted_csv_path}")

        if not extracted_csv_path.is_file():
            raise ValueError(f"Extracted CSV path is not a file: {extracted_csv_path}")

        logger.info(
            "Uploading export CSV to datalake: batch_id={},"
            " csv_path={}, object_path={}",
            batch["batch_id"],
            extracted_csv_path,
            csv_object_path,
        )

        upload_file(
            local_path=extracted_csv_path,
            object_path=csv_object_path,
        )
