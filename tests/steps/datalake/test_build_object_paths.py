from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    get_archive_object_path,
    get_csv_object_path,
    get_lookup_object_paths,
    set_current_batch,
    set_downloaded_lookup_files,
    set_lookup_dir,
    set_reused_lookup_files,
)
from deng_ingestion.steps.datalake.build_object_paths import (
    BuildExportObjectPathsStep,
    BuildLookupObjectPathsStep,
)


def _make_context(tmp_path: Path) -> PipelineContext:
    return PipelineContext(
        run_id="test-run",
        execution_ts=datetime.now(UTC),
        working_dir=tmp_path,
    )


def test_build_export_object_paths_sets_archive_and_csv_paths(tmp_path: Path) -> None:
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

    step = BuildExportObjectPathsStep()
    step.run(context)

    assert (
        get_archive_object_path(context)
        == "raw/gdelt/export/archives/year=2025/month=12/day=04/"
        "20251204024500.export.CSV.zip"
    )
    assert (
        get_csv_object_path(context)
        == "bronze/gdelt/export/events/year=2025/month=12/day=04/"
        "20251204024500.export.CSV"
    )


def test_build_lookup_object_paths_includes_downloaded_and_reused_files(
    tmp_path: Path,
) -> None:
    context = _make_context(tmp_path)

    set_lookup_dir(context, tmp_path / "lookups")
    set_downloaded_lookup_files(
        context,
        [
            "CAMEO.country.txt",
            "CAMEO.eventcodes.txt",
        ],
    )
    set_reused_lookup_files(
        context,
        [
            "FIPS.country.txt",
            "CAMEO.country.txt",
        ],
    )

    step = BuildLookupObjectPathsStep()
    step.run(context)

    assert get_lookup_object_paths(context) == {
        "CAMEO.country.txt": "raw/gdelt/lookups/CAMEO.country.txt",
        "CAMEO.eventcodes.txt": "raw/gdelt/lookups/CAMEO.eventcodes.txt",
        "FIPS.country.txt": "raw/gdelt/lookups/FIPS.country.txt",
    }


def test_build_lookup_object_paths_raises_when_no_lookup_files_exist(
    tmp_path: Path,
) -> None:
    context = _make_context(tmp_path)

    set_lookup_dir(context, tmp_path / "lookups")
    set_downloaded_lookup_files(context, [])
    set_reused_lookup_files(context, [])

    step = BuildLookupObjectPathsStep()

    try:
        step.run(context)
    except ValueError as exc:
        assert "Expected downloaded or reused lookup files" in str(exc)
    else:
        raise AssertionError("Expected ValueError")
