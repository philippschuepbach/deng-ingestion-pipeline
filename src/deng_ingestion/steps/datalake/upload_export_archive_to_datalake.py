from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    get_archive_object_path,
    get_archive_path,
    get_current_batch,
)
from deng_ingestion.steps.datalake.object_storage import upload_file


@dataclass(frozen=True)
class UploadExportArchiveToDatalakeStep:
    name: str = "upload_export_archive_to_datalake"

    def run(self, context: PipelineContext) -> None:
        batch = get_current_batch(context)
        if batch is None:
            logger.debug("Skipping archive upload because no batch is selected")
            return

        archive_path = get_archive_path(context)
        if archive_path is None:
            raise ValueError("Expected archive_path in pipeline context")

        archive_object_path = get_archive_object_path(context)
        if archive_object_path is None:
            raise ValueError("Expected archive_object_path in pipeline context")

        if not archive_path.exists():
            raise ValueError(f"Missing archive file for upload: {archive_path}")

        if not archive_path.is_file():
            raise ValueError(f"Archive upload path is not a file: {archive_path}")

        logger.info(
            "Uploading export archive to datalake: batch_id={},"
            " archive_path={}, object_path={}",
            batch["batch_id"],
            archive_path,
            archive_object_path,
        )

        upload_file(
            local_path=archive_path,
            object_path=archive_object_path,
        )
