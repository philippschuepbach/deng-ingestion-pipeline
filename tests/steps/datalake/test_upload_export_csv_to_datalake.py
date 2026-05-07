from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    set_csv_object_path,
    set_current_batch,
    set_extracted_csv_path,
)
from deng_ingestion.steps.datalake.upload_export_csv_to_datalake import (
    UploadExportCsvToDatalakeStep,
)


def _make_context(tmp_path: Path) -> PipelineContext:
    return PipelineContext(
        run_id="test-run",
        execution_ts=datetime.now(UTC),
        working_dir=tmp_path,
    )


def test_upload_export_csv_to_datalake_uploads_csv(tmp_path: Path, monkeypatch) -> None:
    csv_path = tmp_path / "20251204024500.export.CSV"
    csv_path.write_text("csv-content", encoding="utf-8")

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
    set_extracted_csv_path(context, csv_path)
    set_csv_object_path(
        context,
        "bronze/gdelt/export/events/year=2025/month=12/day=04/"
        "20251204024500.export.CSV",
    )

    uploaded: list[tuple[Path, str]] = []

    def fake_upload_file(*, local_path: Path, object_path: str) -> None:
        uploaded.append((local_path, object_path))

    monkeypatch.setattr(
        "deng_ingestion.steps.datalake.upload_export_csv_to_datalake.upload_file",
        fake_upload_file,
    )

    step = UploadExportCsvToDatalakeStep()
    step.run(context)

    assert uploaded == [
        (
            csv_path,
            "bronze/gdelt/export/events/year=2025/month=12/day=04/"
            "20251204024500.export.CSV",
        )
    ]


def test_upload_export_csv_to_datalake_raises_when_csv_missing(
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
    set_extracted_csv_path(context, tmp_path / "missing.CSV")
    set_csv_object_path(
        context,
        "bronze/gdelt/export/events/year=2025/month=12/day=04/missing.CSV",
    )

    monkeypatch.setattr(
        "deng_ingestion.steps.datalake.upload_export_csv_to_datalake.upload_file",
        lambda **_: None,
    )

    step = UploadExportCsvToDatalakeStep()

    try:
        step.run(context)
    except ValueError as exc:
        assert "Missing extracted CSV file" in str(exc)
    else:
        raise AssertionError("Expected ValueError")
