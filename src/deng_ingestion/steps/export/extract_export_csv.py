from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from zipfile import ZipFile

from loguru import logger

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    get_archive_path,
    get_current_batch,
    set_extracted_csv_path,
)


@dataclass(frozen=True)
class ExtractExportCsvStep:
    name: str = "extract_export_csv"

    def run(self, context: PipelineContext) -> None:
        batch = get_current_batch(context)
        if batch is None:
            logger.debug("Skipping extraction because no batch is selected")
            return

        archive_path = get_archive_path(context)
        if archive_path is None:
            raise ValueError("Expected archive path in pipeline context")

        raw_dir = context.working_dir / "data" / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "Extracting export archive: batch_id={}, archive_path={}",
            batch["batch_id"],
            archive_path,
        )

        with ZipFile(archive_path, "r") as zip_file:
            members = zip_file.namelist()
            if len(members) != 1:
                raise ValueError(
                    f"Expected exactly one file in archive {archive_path}, found {len(members)}"
                )

            member_name = members[0]
            extracted_path = raw_dir / Path(member_name).name

            if not extracted_path.exists():
                zip_file.extract(member_name, raw_dir)
                extracted_nested_path = raw_dir / member_name

                if (
                    extracted_nested_path != extracted_path
                    and extracted_nested_path.exists()
                ):
                    extracted_nested_path.rename(extracted_path)
            else:
                logger.info(
                    "Extracted CSV already exists locally, reusing file: {}",
                    extracted_path,
                )

        set_extracted_csv_path(context, extracted_path)
