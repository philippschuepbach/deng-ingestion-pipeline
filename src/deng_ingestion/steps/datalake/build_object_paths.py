from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from loguru import logger

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    get_current_batch,
    get_downloaded_lookup_files,
    get_lookup_dir,
    get_reused_lookup_files,
    set_archive_object_path,
    set_csv_object_path,
    set_lookup_object_paths,
)


def _build_export_archive_object_path(
    file_name: str, year: int, month: int, day: int
) -> str:
    return (
        f"raw/gdelt/export/archives/"
        f"year={year:04d}/month={month:02d}/day={day:02d}/"
        f"{file_name}"
    )


def _build_export_csv_object_path(
    file_name: str, year: int, month: int, day: int
) -> str:
    csv_file_name = file_name[:-4] if file_name.lower().endswith(".zip") else file_name

    return (
        f"bronze/gdelt/export/events/"
        f"year={year:04d}/month={month:02d}/day={day:02d}/"
        f"{csv_file_name}"
    )


def _build_lookup_object_path(file_name: str) -> str:
    return f"raw/gdelt/lookups/{file_name}"


@dataclass(frozen=True)
class BuildExportObjectPathsStep:
    name: str = "build_export_object_paths"

    def run(self, context: PipelineContext) -> None:
        current_batch = get_current_batch(context)
        if current_batch is None:
            raise ValueError("Expected current batch in pipeline context")

        file_name = current_batch.get("file_name")
        if not file_name:
            raise ValueError("Expected file_name in current batch")

        gdelt_timestamp = current_batch.get("gdelt_timestamp")
        if gdelt_timestamp is None:
            raise ValueError("Expected gdelt_timestamp in current batch")

        archive_object_path = _build_export_archive_object_path(
            file_name=file_name,
            year=gdelt_timestamp.year,
            month=gdelt_timestamp.month,
            day=gdelt_timestamp.day,
        )

        csv_object_path = _build_export_csv_object_path(
            file_name=file_name,
            year=gdelt_timestamp.year,
            month=gdelt_timestamp.month,
            day=gdelt_timestamp.day,
        )

        set_archive_object_path(context, archive_object_path)
        set_csv_object_path(context, csv_object_path)

        logger.debug(
            "Built export object paths: batch_id={},"
            " archive_object_path={}, csv_object_path={}",
            current_batch.get("batch_id"),
            archive_object_path,
            csv_object_path,
        )


@dataclass(frozen=True)
class BuildLookupObjectPathsStep:
    name: str = "build_lookup_object_paths"

    def run(self, context: PipelineContext) -> None:
        lookup_dir = get_lookup_dir(context)
        if lookup_dir is None:
            raise ValueError("Expected lookup_dir in pipeline context")

        downloaded_files = get_downloaded_lookup_files(context)
        reused_files = get_reused_lookup_files(context)

        lookup_files = list(dict.fromkeys(downloaded_files + reused_files))
        if not lookup_files:
            raise ValueError(
                "Expected downloaded or reused lookup files in pipeline context"
            )

        lookup_object_paths = {
            file_name: _build_lookup_object_path(file_name)
            for file_name in lookup_files
        }

        set_lookup_object_paths(context, lookup_object_paths)

        logger.debug(
            "Built lookup object paths: lookup_dir={}, file_count={}",
            lookup_dir,
            len(lookup_object_paths),
        )


def build_export_archive_object_path(file_name: str, gdelt_timestamp: datetime) -> str:
    return _build_export_archive_object_path(
        file_name=file_name,
        year=gdelt_timestamp.year,
        month=gdelt_timestamp.month,
        day=gdelt_timestamp.day,
    )


def build_export_csv_object_path(file_name: str, gdelt_timestamp: datetime) -> str:
    return _build_export_csv_object_path(
        file_name=file_name,
        year=gdelt_timestamp.year,
        month=gdelt_timestamp.month,
        day=gdelt_timestamp.day,
    )
