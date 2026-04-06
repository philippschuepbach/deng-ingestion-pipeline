from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    get_archive_path,
    set_current_batch,
)
from deng_ingestion.pipeline.context_types import BatchInfo
from deng_ingestion.steps.export import download_export_archive as step_module
from deng_ingestion.steps.export.download_export_archive import (
    DownloadExportArchiveStep,
)


def make_context(tmp_path: Path) -> PipelineContext:
    return PipelineContext(
        run_id="test",
        execution_ts=datetime(2026, 4, 8, 12, 0, 0, tzinfo=UTC),
        working_dir=tmp_path,
    )


def make_batch() -> BatchInfo:
    return {
        "batch_id": 123,
        "source_type": "lastupdate",
        "file_type": "export",
        "source_url": "https://example.test/20260408120000.export.CSV.zip",
        "file_name": "20260408120000.export.CSV.zip",
        "gdelt_timestamp": datetime(2026, 4, 8, 12, 0, 0, tzinfo=UTC),
        "status": "discovered",
        "claimed_at": None,
        "claimed_by": None,
    }


class FakeConnection:
    def __init__(self) -> None:
        self.committed = False
        self.rolled_back = False

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        return None


def test_download_export_archive_downloads_and_sets_archive_path(
    monkeypatch,
    tmp_path: Path,
) -> None:
    context = make_context(tmp_path)
    batch = make_batch()
    set_current_batch(context, batch)

    fake_conn = FakeConnection()
    marked_batch_ids: list[int] = []
    download_calls: list[tuple[str, Path]] = []

    def fake_download_binary_to_file(url: str, destination: Path, **kwargs) -> None:
        download_calls.append((url, destination))
        destination.write_text("zip-bytes", encoding="utf-8")

    def fake_mark_batch_downloaded(conn, batch_id: int) -> None:
        marked_batch_ids.append(batch_id)

    monkeypatch.setattr(
        step_module,
        "get_context_connection",
        lambda context: (fake_conn, False),
    )
    monkeypatch.setattr(
        step_module,
        "download_binary_to_file",
        fake_download_binary_to_file,
    )
    monkeypatch.setattr(
        step_module,
        "mark_batch_downloaded",
        fake_mark_batch_downloaded,
    )

    step = DownloadExportArchiveStep()
    step.run(context)

    expected_path = (
        tmp_path / "data" / "raw" / "archives" / "20260408120000.export.CSV.zip"
    )

    assert download_calls == [(batch["source_url"], expected_path)]
    assert marked_batch_ids == [123]
    assert fake_conn.committed is True
    assert expected_path.exists()
    assert get_archive_path(context) == expected_path
