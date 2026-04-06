from __future__ import annotations

from pathlib import Path
from typing import cast

from psycopg2.extensions import connection as PgConnection

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_keys import (
    ARCHIVE_PATH,
    CURRENT_BATCH,
    CURRENT_SILVER_BATCH,
    DB_CONNECTION,
    DOWNLOADED_LOOKUP_FILES,
    EXTRACTED_CSV_PATH,
    FILTERED_MANIFEST_ENTRIES,
    GOLD_ROW_COUNT,
    INGESTED_EXPORT_BATCH_IDS,
    LAST_SILVER_INSERTED_ROWS,
    LOADED_LOOKUP_COUNTS,
    LOOKUP_DIR,
    MANIFEST_ENTRIES,
    MANIFEST_SOURCE_TYPE,
    MANIFEST_TEXT,
    PROCESSED_BATCHES,
    PROCESSED_SILVER_BATCHES,
    REGISTERED_EXPORT_BATCH_IDS,
    REMAINING_INGESTED_EXPORT_BATCH_IDS,
    REMAINING_REGISTERED_EXPORT_BATCH_IDS,
    REUSED_LOOKUP_FILES,
    SEEDED_RISK_CATEGORY_MAPPING_COUNT,
    TRANSFORMED_EXPORT_BATCH_IDS,
)
from deng_ingestion.pipeline.context_types import (
    BatchIdList,
    BatchInfo,
    LookupLoadCounts,
    SilverBatchInfo,
)


def get_manifest_text(context: PipelineContext) -> str | None:
    value = context.data.get(MANIFEST_TEXT)
    if value is None:
        return None
    return cast(str, value)


def set_manifest_text(context: PipelineContext, value: str) -> None:
    context.data[MANIFEST_TEXT] = value


def get_manifest_source_type(context: PipelineContext) -> str | None:
    value = context.data.get(MANIFEST_SOURCE_TYPE)
    if value is None:
        return None
    return cast(str, value)


def set_manifest_source_type(context: PipelineContext, value: str) -> None:
    context.data[MANIFEST_SOURCE_TYPE] = value


def get_manifest_entries(context: PipelineContext):
    return context.data.get(MANIFEST_ENTRIES)


def set_manifest_entries(context: PipelineContext, value) -> None:
    context.data[MANIFEST_ENTRIES] = value


def get_filtered_manifest_entries(context: PipelineContext):
    return context.data.get(FILTERED_MANIFEST_ENTRIES)


def set_filtered_manifest_entries(context: PipelineContext, value) -> None:
    context.data[FILTERED_MANIFEST_ENTRIES] = value


def get_current_batch(context: PipelineContext) -> BatchInfo | None:
    value = context.data.get(CURRENT_BATCH)
    if value is None:
        return None
    return cast(BatchInfo, value)


def set_current_batch(context: PipelineContext, value: BatchInfo | None) -> None:
    context.data[CURRENT_BATCH] = value


def clear_current_batch(context: PipelineContext) -> None:
    context.data.pop(CURRENT_BATCH, None)


def get_current_silver_batch(context: PipelineContext) -> SilverBatchInfo | None:
    value = context.data.get(CURRENT_SILVER_BATCH)
    if value is None:
        return None
    return cast(SilverBatchInfo, value)


def set_current_silver_batch(
    context: PipelineContext, value: SilverBatchInfo | None
) -> None:
    context.data[CURRENT_SILVER_BATCH] = value


def clear_current_silver_batch(context: PipelineContext) -> None:
    context.data.pop(CURRENT_SILVER_BATCH, None)


def get_archive_path(context: PipelineContext) -> Path | None:
    value = context.data.get(ARCHIVE_PATH)
    if value is None:
        return None
    return cast(Path, value)


def set_archive_path(context: PipelineContext, value: Path) -> None:
    context.data[ARCHIVE_PATH] = value


def clear_archive_path(context: PipelineContext) -> None:
    context.data.pop(ARCHIVE_PATH, None)


def get_extracted_csv_path(context: PipelineContext) -> Path | None:
    value = context.data.get(EXTRACTED_CSV_PATH)
    if value is None:
        return None
    return cast(Path, value)


def set_extracted_csv_path(context: PipelineContext, value: Path) -> None:
    context.data[EXTRACTED_CSV_PATH] = value


def clear_extracted_csv_path(context: PipelineContext) -> None:
    context.data.pop(EXTRACTED_CSV_PATH, None)


def get_registered_export_batch_ids(context: PipelineContext) -> BatchIdList:
    value = context.data.get(REGISTERED_EXPORT_BATCH_IDS, [])
    return cast(BatchIdList, value)


def set_registered_export_batch_ids(
    context: PipelineContext, value: BatchIdList
) -> None:
    context.data[REGISTERED_EXPORT_BATCH_IDS] = value


def get_remaining_registered_export_batch_ids(
    context: PipelineContext,
) -> BatchIdList | None:
    value = context.data.get(REMAINING_REGISTERED_EXPORT_BATCH_IDS)
    if value is None:
        return None
    return cast(BatchIdList, value)


def set_remaining_registered_export_batch_ids(
    context: PipelineContext, value: BatchIdList
) -> None:
    context.data[REMAINING_REGISTERED_EXPORT_BATCH_IDS] = value


def clear_remaining_registered_export_batch_ids(context: PipelineContext) -> None:
    context.data.pop(REMAINING_REGISTERED_EXPORT_BATCH_IDS, None)


def get_ingested_export_batch_ids(context: PipelineContext) -> BatchIdList:
    value = context.data.get(INGESTED_EXPORT_BATCH_IDS, [])
    return cast(BatchIdList, value)


def set_ingested_export_batch_ids(context: PipelineContext, value: BatchIdList) -> None:
    context.data[INGESTED_EXPORT_BATCH_IDS] = value


def get_remaining_ingested_export_batch_ids(
    context: PipelineContext,
) -> BatchIdList | None:
    value = context.data.get(REMAINING_INGESTED_EXPORT_BATCH_IDS)
    if value is None:
        return None
    return cast(BatchIdList, value)


def set_remaining_ingested_export_batch_ids(
    context: PipelineContext, value: BatchIdList
) -> None:
    context.data[REMAINING_INGESTED_EXPORT_BATCH_IDS] = value


def clear_remaining_ingested_export_batch_ids(context: PipelineContext) -> None:
    context.data.pop(REMAINING_INGESTED_EXPORT_BATCH_IDS, None)


def get_transformed_export_batch_ids(context: PipelineContext) -> BatchIdList:
    value = context.data.get(TRANSFORMED_EXPORT_BATCH_IDS, [])
    return cast(BatchIdList, value)


def set_transformed_export_batch_ids(
    context: PipelineContext, value: BatchIdList
) -> None:
    context.data[TRANSFORMED_EXPORT_BATCH_IDS] = value


def get_last_silver_inserted_rows(context: PipelineContext) -> int | None:
    value = context.data.get(LAST_SILVER_INSERTED_ROWS)
    if value is None:
        return None
    return cast(int, value)


def set_last_silver_inserted_rows(context: PipelineContext, value: int) -> None:
    context.data[LAST_SILVER_INSERTED_ROWS] = value


def clear_last_silver_inserted_rows(context: PipelineContext) -> None:
    context.data.pop(LAST_SILVER_INSERTED_ROWS, None)


def get_lookup_dir(context: PipelineContext) -> Path | None:
    value = context.data.get(LOOKUP_DIR)
    if value is None:
        return None
    return cast(Path, value)


def set_lookup_dir(context: PipelineContext, value: Path) -> None:
    context.data[LOOKUP_DIR] = value


def get_downloaded_lookup_files(context: PipelineContext) -> list[str]:
    value = context.data.get(DOWNLOADED_LOOKUP_FILES, [])
    return cast(list[str], value)


def set_downloaded_lookup_files(context: PipelineContext, value: list[str]) -> None:
    context.data[DOWNLOADED_LOOKUP_FILES] = value


def get_reused_lookup_files(context: PipelineContext) -> list[str]:
    value = context.data.get(REUSED_LOOKUP_FILES, [])
    return cast(list[str], value)


def set_reused_lookup_files(context: PipelineContext, value: list[str]) -> None:
    context.data[REUSED_LOOKUP_FILES] = value


def get_loaded_lookup_counts(context: PipelineContext) -> LookupLoadCounts:
    value = context.data.get(LOADED_LOOKUP_COUNTS, {})
    return cast(LookupLoadCounts, value)


def set_loaded_lookup_counts(context: PipelineContext, value: LookupLoadCounts) -> None:
    context.data[LOADED_LOOKUP_COUNTS] = value


def get_seeded_risk_category_mapping_count(context: PipelineContext) -> int:
    value = context.data.get(SEEDED_RISK_CATEGORY_MAPPING_COUNT, 0)
    return cast(int, value)


def set_seeded_risk_category_mapping_count(
    context: PipelineContext, value: int
) -> None:
    context.data[SEEDED_RISK_CATEGORY_MAPPING_COUNT] = value


def get_processed_batches(context: PipelineContext) -> int:
    value = context.data.get(PROCESSED_BATCHES, 0)
    return cast(int, value)


def set_processed_batches(context: PipelineContext, value: int) -> None:
    context.data[PROCESSED_BATCHES] = value


def get_processed_silver_batches(context: PipelineContext) -> int:
    value = context.data.get(PROCESSED_SILVER_BATCHES, 0)
    return cast(int, value)


def set_processed_silver_batches(context: PipelineContext, value: int) -> None:
    context.data[PROCESSED_SILVER_BATCHES] = value


def get_gold_row_count(context: PipelineContext) -> int:
    value = context.data.get(GOLD_ROW_COUNT, 0)
    return cast(int, value)


def set_gold_row_count(context: PipelineContext, value: int) -> None:
    context.data[GOLD_ROW_COUNT] = value


def get_db_connection(context: PipelineContext) -> PgConnection | None:
    value = context.data.get(DB_CONNECTION)
    if value is None:
        return None
    return cast(PgConnection, value)


def set_db_connection(context: PipelineContext, value: PgConnection) -> None:
    context.data[DB_CONNECTION] = value


def clear_db_connection(context: PipelineContext) -> None:
    context.data.pop(DB_CONNECTION, None)
