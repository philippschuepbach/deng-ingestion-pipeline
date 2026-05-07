from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    get_uploaded_lookup_object_paths,
    set_lookup_dir,
    set_lookup_object_paths,
)
from deng_ingestion.steps.datalake.upload_lookup_files_to_datalake import (
    UploadLookupFilesToDatalakeStep,
)


def _make_context(tmp_path: Path) -> PipelineContext:
    return PipelineContext(
        run_id="test-run",
        execution_ts=datetime.now(UTC),
        working_dir=tmp_path,
    )


def test_upload_lookup_files_to_datalake_uploads_all_files(
    tmp_path: Path, monkeypatch
) -> None:
    lookup_dir = tmp_path / "lookups"
    lookup_dir.mkdir()

    (lookup_dir / "CAMEO.country.txt").write_text("country-data", encoding="utf-8")
    (lookup_dir / "FIPS.country.txt").write_text("fips-data", encoding="utf-8")

    context = _make_context(tmp_path)
    set_lookup_dir(context, lookup_dir)
    set_lookup_object_paths(
        context,
        {
            "CAMEO.country.txt": "raw/gdelt/lookups/CAMEO.country.txt",
            "FIPS.country.txt": "raw/gdelt/lookups/FIPS.country.txt",
        },
    )

    uploaded: list[tuple[Path, str]] = []

    def fake_upload_file(*, local_path: Path, object_path: str) -> None:
        uploaded.append((local_path, object_path))

    monkeypatch.setattr(
        "deng_ingestion.steps.datalake.upload_lookup_files_to_datalake.upload_file",
        fake_upload_file,
    )

    step = UploadLookupFilesToDatalakeStep()
    step.run(context)

    assert uploaded == [
        (lookup_dir / "CAMEO.country.txt", "raw/gdelt/lookups/CAMEO.country.txt"),
        (lookup_dir / "FIPS.country.txt", "raw/gdelt/lookups/FIPS.country.txt"),
    ]
    assert get_uploaded_lookup_object_paths(context) == {
        "CAMEO.country.txt": "raw/gdelt/lookups/CAMEO.country.txt",
        "FIPS.country.txt": "raw/gdelt/lookups/FIPS.country.txt",
    }


def test_upload_lookup_files_to_datalake_raises_when_file_missing(
    tmp_path: Path, monkeypatch
) -> None:
    lookup_dir = tmp_path / "lookups"
    lookup_dir.mkdir()

    context = _make_context(tmp_path)
    set_lookup_dir(context, lookup_dir)
    set_lookup_object_paths(
        context,
        {
            "CAMEO.country.txt": "raw/gdelt/lookups/CAMEO.country.txt",
        },
    )

    monkeypatch.setattr(
        "deng_ingestion.steps.datalake.upload_lookup_files_to_datalake.upload_file",
        lambda **_: None,
    )

    step = UploadLookupFilesToDatalakeStep()

    try:
        step.run(context)
    except ValueError as exc:
        assert "Missing lookup file for upload" in str(exc)
    else:
        raise AssertionError("Expected ValueError")
