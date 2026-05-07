from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    set_archive_object_path,
    set_archive_path,
    set_current_batch,
)
from deng_ingestion.steps.datalake.upload_export_archive_to_datalake import (
    UploadExportArchiveToDatalakeStep,
)


def _make_context(tmp_path: Path) -> PipelineContext:
    return PipelineContext(
        run_id="test-run",
        execution_ts=datetime.now(UTC),
        working_dir=tmp_path,
    )


def test_upload_export_archive_to_datalake_uploads_archive(
    tmp_path: Path, monkeypatch
) -> None:
    archive_path = tmp_path / "20251204024500.export.CSV.zip"
    archive_path.write_text("zip-content", encoding="utf-8")

    context = _make_context(tmp_path)
    set_current_batch(
        context,
        {
            "batch_id": 123,
            "source_type": "masterfilelist",
            "file_type": "export",
            "source_url": "http://example.test/20251204024500.export.CSV.zip",
            "file_name": "20251204024500.export.CSV.zip",
            "gdelt_timestamp": datetime(2025, 12, 4, 2, 45, 0, tzinfo=UTC),
            "status": "downloaded",
            "claimed_at": None,
            "claimed_by": None,
        },
    )
    set_archive_path(context, archive_path)
    set_archive_object_path(
        context,
        "raw/gdelt/export/archives/year=2025/month=12/day=04/"
        "20251204024500.export.CSV.zip",
    )

    uploaded: list[tuple[Path, str]] = []

    def fake_upload_file(*, local_path: Path, object_path: str) -> None:
        uploaded.append((local_path, object_path))

    monkeypatch.setattr(
        "deng_ingestion.steps.datalake.upload_export_archive_to_datalake.upload_file",
        fake_upload_file,
    )

    step = UploadExportArchiveToDatalakeStep()
    step.run(context)

    assert uploaded == [
        (
            archive_path,
            "raw/gdelt/export/archives/year=2025/month=12/day=04/"
            "20251204024500.export.CSV.zip",
        )
    ]


def test_upload_export_archive_to_datalake_raises_when_archive_missing(
    tmp_path: Path, monkeypatch
) -> None:
    context = _make_context(tmp_path)
    set_current_batch(
        context,
        {
            "batch_id": 123,
            "source_type": "masterfilelist",
            "file_type": "export",
            "source_url": "http://example.test/missing.zip",
            "file_name": "missing.zip",
            "gdelt_timestamp": datetime(2025, 12, 4, 2, 45, 0, tzinfo=UTC),
            "status": "downloaded",
            "claimed_at": None,
            "claimed_by": None,
        },
    )
    set_archive_path(context, tmp_path / "missing.zip")
    set_archive_object_path(
        context,
        "raw/gdelt/export/archives/year=2025/month=12/day=04/missing.zip",
    )

    monkeypatch.setattr(
        "deng_ingestion.steps.datalake.upload_export_archive_to_datalake.upload_file",
        lambda **_: None,
    )

    step = UploadExportArchiveToDatalakeStep()

    try:
        step.run(context)
    except ValueError as exc:
        assert "Missing archive file for upload" in str(exc)
    else:
        raise AssertionError("Expected ValueError")
