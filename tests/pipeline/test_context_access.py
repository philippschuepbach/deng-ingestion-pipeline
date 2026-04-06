from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    clear_archive_path,
    clear_current_batch,
    clear_db_connection,
    clear_last_silver_inserted_rows,
    get_archive_path,
    get_current_batch,
    get_db_connection,
    get_gold_row_count,
    get_last_silver_inserted_rows,
    get_registered_export_batch_ids,
    set_archive_path,
    set_current_batch,
    set_db_connection,
    set_gold_row_count,
    set_last_silver_inserted_rows,
    set_registered_export_batch_ids,
)
from deng_ingestion.pipeline.context_types import BatchInfo


def make_context() -> PipelineContext:
    return PipelineContext(
        run_id="test",
        execution_ts=datetime(2026, 4, 7, 12, 0, 0, tzinfo=UTC),
        working_dir=Path("."),
    )


def make_batch() -> BatchInfo:
    return {
        "batch_id": 123,
        "source_type": "lastupdate",
        "file_type": "export",
        "source_url": "http://data.gdeltproject.org/gdeltv2/20260407123000.export.CSV.zip",
        "file_name": "20260407123000.export.CSV.zip",
        "gdelt_timestamp": datetime(2026, 4, 7, 12, 30, 0, tzinfo=UTC),
        "status": "discovered",
        "claimed_at": None,
        "claimed_by": None,
    }


def test_current_batch_roundtrip_and_clear() -> None:
    context = make_context()
    batch = make_batch()

    assert get_current_batch(context) is None

    set_current_batch(context, batch)
    assert get_current_batch(context) == batch

    clear_current_batch(context)
    assert get_current_batch(context) is None


def test_archive_path_roundtrip_and_clear() -> None:
    context = make_context()
    archive_path = Path("/tmp/test.zip")

    assert get_archive_path(context) is None

    set_archive_path(context, archive_path)
    assert get_archive_path(context) == archive_path

    clear_archive_path(context)
    assert get_archive_path(context) is None


def test_registered_export_batch_ids_default_and_roundtrip() -> None:
    context = make_context()

    assert get_registered_export_batch_ids(context) == []

    set_registered_export_batch_ids(context, [1, 2, 3])
    assert get_registered_export_batch_ids(context) == [1, 2, 3]


def test_last_silver_inserted_rows_roundtrip_and_clear() -> None:
    context = make_context()

    assert get_last_silver_inserted_rows(context) is None

    set_last_silver_inserted_rows(context, 42)
    assert get_last_silver_inserted_rows(context) == 42

    clear_last_silver_inserted_rows(context)
    assert get_last_silver_inserted_rows(context) is None


def test_gold_row_count_defaults_to_zero_and_can_be_set() -> None:
    context = make_context()

    assert get_gold_row_count(context) == 0

    set_gold_row_count(context, 99)
    assert get_gold_row_count(context) == 99


def test_db_connection_roundtrip_and_clear() -> None:
    context = make_context()
    dummy_connection = object()

    assert get_db_connection(context) is None

    set_db_connection(context, dummy_connection)  # type: ignore[arg-type]
    assert get_db_connection(context) is dummy_connection

    clear_db_connection(context)
    assert get_db_connection(context) is None
