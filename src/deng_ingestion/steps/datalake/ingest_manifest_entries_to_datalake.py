from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from zipfile import ZipFile

from loguru import logger

from deng_ingestion.core.http import download_binary_to_file
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    get_filtered_manifest_entries,
    get_processed_batches,
    set_processed_batches,
)
from deng_ingestion.steps.datalake.build_object_paths import (
    build_export_archive_object_path,
    build_export_csv_object_path,
)
from deng_ingestion.steps.datalake.object_storage import object_exists, upload_file


def _csv_file_name_from_archive(file_name: str) -> str:
    if file_name.lower().endswith(".zip"):
        return file_name[:-4]
    return file_name


def _extract_single_csv_from_zip(archive_path: Path, csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with ZipFile(archive_path) as zip_file:
        csv_members = [
            member
            for member in zip_file.namelist()
            if not member.endswith("/") and member.lower().endswith(".csv")
        ]

        if len(csv_members) != 1:
            raise ValueError(
                f"Expected exactly one CSV file in archive {archive_path}, "
                f"found {len(csv_members)}"
            )

        with zip_file.open(csv_members[0]) as source:
            with csv_path.open("wb") as target:
                shutil.copyfileobj(source, target)


@dataclass(frozen=True)
class IngestManifestEntriesToDatalakeStep:
    name: str = "ingest_manifest_entries_to_datalake"

    def run(self, context: PipelineContext) -> None:
        entries = get_filtered_manifest_entries(context)
        if entries is None:
            raise ValueError("Expected filtered manifest entries in pipeline context")

        archives_dir = context.working_dir / "data" / "datalake" / "raw" / "archives"
        csv_dir = context.working_dir / "data" / "datalake" / "bronze" / "events"

        archives_dir.mkdir(parents=True, exist_ok=True)
        csv_dir.mkdir(parents=True, exist_ok=True)

        processed_batches = get_processed_batches(context)

        for entry in entries:
            if entry.file_type != "export":
                logger.debug(
                    "Skipping non-export manifest entry: file_name={}, file_type={}",
                    entry.file_name,
                    entry.file_type,
                )
                continue

            archive_object_path = build_export_archive_object_path(
                file_name=entry.file_name,
                gdelt_timestamp=entry.gdelt_timestamp,
            )
            csv_object_path = build_export_csv_object_path(
                file_name=entry.file_name,
                gdelt_timestamp=entry.gdelt_timestamp,
            )

            archive_exists = object_exists(archive_object_path)
            csv_exists = object_exists(csv_object_path)

            if archive_exists and csv_exists:
                logger.debug(
                    "Skipping already uploaded export batch: file_name={}",
                    entry.file_name,
                )
                continue

            archive_path = archives_dir / entry.file_name
            csv_path = csv_dir / _csv_file_name_from_archive(entry.file_name)

            if not archive_path.exists():
                logger.info(
                    "Downloading export archive for datalake ingest: "
                    "file_name={}, url={}",
                    entry.file_name,
                    entry.source_url,
                )

                download_binary_to_file(
                    entry.source_url,
                    archive_path,
                    timeout_seconds=30.0,
                    retries=3,
                )
            else:
                logger.debug(
                    "Reusing local export archive for datalake ingest: path={}",
                    archive_path,
                )

            if not archive_exists:
                logger.info(
                    "Uploading raw export archive to datalake: "
                    "file_name={}, object_path={}",
                    entry.file_name,
                    archive_object_path,
                )
                upload_file(
                    local_path=archive_path,
                    object_path=archive_object_path,
                )

            if not csv_exists:
                logger.info(
                    "Extracting export CSV for datalake ingest: "
                    "archive_path={}, csv_path={}",
                    archive_path,
                    csv_path,
                )

                _extract_single_csv_from_zip(
                    archive_path=archive_path,
                    csv_path=csv_path,
                )

                logger.info(
                    "Uploading bronze export CSV to datalake: "
                    "file_name={}, object_path={}",
                    csv_path.name,
                    csv_object_path,
                )
                upload_file(
                    local_path=csv_path,
                    object_path=csv_object_path,
                )

            processed_batches += 1
            set_processed_batches(context, processed_batches)

        logger.info(
            "Finished direct datalake export ingest: processed_batches={}",
            processed_batches,
        )
