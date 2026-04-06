from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from deng_ingestion.manifest.entry import ManifestEntry
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    get_registered_export_batch_ids,
    set_filtered_manifest_entries,
    set_manifest_source_type,
)
from deng_ingestion.steps.manifest import (
    register_manifest_batches_for_current_run as step_module,
)
from deng_ingestion.steps.manifest.register_manifest_batches_for_current_run import (
    RegisterManifestBatchesForCurrentRunStep,
)


def make_context() -> PipelineContext:
    return PipelineContext(
        run_id="test",
        execution_ts=datetime(2026, 4, 8, 12, 0, 0, tzinfo=UTC),
        working_dir=Path("."),
    )


def make_entry(
    *,
    file_type: str,
    file_name: str,
    source_url: str,
) -> ManifestEntry:
    return ManifestEntry(
        file_size_bytes=123,
        md5_hash="0123456789abcdef0123456789abcdef",
        source_url=source_url,
        file_name=file_name,
        gdelt_timestamp=datetime(2026, 4, 8, 12, 0, 0, tzinfo=UTC),
        file_type=file_type,
    )


class FakeCursor:
    def __init__(self, rows: list[tuple[int, str]]) -> None:
        self._rows = list(rows)
        self._current_row: tuple[int, str] | None = None
        self.executed: list[dict[str, object]] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params: dict[str, object]) -> None:
        self.executed.append(params)
        if not self._rows:
            raise AssertionError("No fake rows left for fetchone()")
        self._current_row = self._rows.pop(0)

    def fetchone(self) -> tuple[int, str] | None:
        return self._current_row


class FakeConnection:
    def __init__(self, rows: list[tuple[int, str]]) -> None:
        self.cursor_obj = FakeCursor(rows)
        self.committed = False

    def __enter__(self) -> FakeConnection:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> FakeCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.committed = True


def test_register_manifest_batches_for_current_run_collects_only_export_batch_ids(
    monkeypatch,
) -> None:
    context = make_context()
    set_manifest_source_type(context, "lastupdate")
    set_filtered_manifest_entries(
        context,
        [
            make_entry(
                file_type="export",
                file_name="20260408120000.export.CSV.zip",
                source_url="http://data.gdeltproject.org/gdeltv2/20260408120000.export.CSV.zip",
            ),
            make_entry(
                file_type="mentions",
                file_name="20260408120500.mentions.CSV.zip",
                source_url="http://data.gdeltproject.org/gdeltv2/20260408120500.mentions.CSV.zip",
            ),
            make_entry(
                file_type="export",
                file_name="20260408121000.export.CSV.zip",
                source_url="http://data.gdeltproject.org/gdeltv2/20260408121000.export.CSV.zip",
            ),
        ],
    )

    fake_conn = FakeConnection(
        [
            (101, "export"),
            (202, "mentions"),
            (303, "export"),
        ]
    )

    monkeypatch.setattr(step_module, "get_connection", lambda: fake_conn)

    step = RegisterManifestBatchesForCurrentRunStep()
    step.run(context)

    assert get_registered_export_batch_ids(context) == [101, 303]
    assert fake_conn.committed is True
    assert len(fake_conn.cursor_obj.executed) == 3


def test_register_manifest_batches_for_current_run_deduplicates_export_batch_ids(
    monkeypatch,
) -> None:
    context = make_context()
    set_manifest_source_type(context, "lastupdate")
    set_filtered_manifest_entries(
        context,
        [
            make_entry(
                file_type="export",
                file_name="20260408120000.export.CSV.zip",
                source_url="http://data.gdeltproject.org/gdeltv2/20260408120000.export.CSV.zip",
            ),
            make_entry(
                file_type="export",
                file_name="20260408120000.export.CSV.zip",
                source_url="http://data.gdeltproject.org/gdeltv2/20260408120000.export.CSV.zip",
            ),
        ],
    )

    fake_conn = FakeConnection(
        [
            (101, "export"),
            (101, "export"),
        ]
    )

    monkeypatch.setattr(step_module, "get_connection", lambda: fake_conn)

    step = RegisterManifestBatchesForCurrentRunStep()
    step.run(context)

    assert get_registered_export_batch_ids(context) == [101]
    assert fake_conn.committed is True


def test_register_manifest_batches_for_current_run_passes_source_type_to_insert(
    monkeypatch,
) -> None:
    context = make_context()
    set_manifest_source_type(context, "masterfilelist")
    set_filtered_manifest_entries(
        context,
        [
            make_entry(
                file_type="export",
                file_name="20260408120000.export.CSV.zip",
                source_url="http://data.gdeltproject.org/gdeltv2/20260408120000.export.CSV.zip",
            ),
        ],
    )

    fake_conn = FakeConnection([(555, "export")])

    monkeypatch.setattr(step_module, "get_connection", lambda: fake_conn)

    step = RegisterManifestBatchesForCurrentRunStep()
    step.run(context)

    assert fake_conn.cursor_obj.executed[0]["source_type"] == "masterfilelist"
    assert get_registered_export_batch_ids(context) == [555]
