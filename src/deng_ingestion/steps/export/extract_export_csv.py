from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from shutil import copyfileobj
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

        logger.debug(
            "Extracting export archive: batch_id={}, archive_path={}",
            batch["batch_id"],
            archive_path,
        )

        with ZipFile(archive_path, "r") as zip_file:
            members = zip_file.namelist()
            if len(members) != 1:
                message = (
                    f"Expected exactly one file in archive {archive_path}, "
                    f"found {len(members)}"
                )
                raise ValueError(message)

            member_name = members[0]
            safe_extracted_path = safe_member_path(raw_dir, member_name)
            extracted_path = raw_dir / Path(member_name).name

            if not extracted_path.exists():
                safe_extracted_path.parent.mkdir(parents=True, exist_ok=True)

                with (
                    zip_file.open(member_name, "r") as source,
                    safe_extracted_path.open("wb") as target,
                ):
                    copyfileobj(source, target)

                if safe_extracted_path != extracted_path:
                    safe_extracted_path.rename(extracted_path)
            else:
                logger.debug(
                    "Extracted CSV already exists locally, reusing file: {}",
                    extracted_path,
                )

        set_extracted_csv_path(context, extracted_path)


def safe_member_path(base_dir: Path, member_name: str) -> Path:
    """Prevent Zip Slip by ensuring the ZIP member cannot escape base_dir."""
    normalized_name = member_name.replace("\\", "/")
    parts = Path(normalized_name).parts

    if normalized_name.startswith("/") or ".." in parts:
        raise ValueError(f"Unsafe ZIP member path: {member_name}")

    destination = (base_dir / normalized_name).resolve()
    base = base_dir.resolve()

    if base not in destination.parents and destination != base:
        raise ValueError(f"Unsafe ZIP extraction target: {destination}")

    return destination
