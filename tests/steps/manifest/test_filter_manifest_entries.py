from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from deng_ingestion.manifest.entry import ManifestEntry
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    get_filtered_manifest_entries,
    set_manifest_entries,
)
from deng_ingestion.steps.manifest.filter_manifest_entries import (
    FilterManifestEntriesStep,
)


def make_context() -> PipelineContext:
    return PipelineContext(
        run_id="test",
        execution_ts=datetime(2026, 4, 7, 12, 0, 0, tzinfo=UTC),
        working_dir=Path("."),
    )


def make_entry(
    *,
    file_type: str,
    timestamp: datetime,
    file_name: str,
) -> ManifestEntry:
    return ManifestEntry(
        file_size_bytes=123,
        md5_hash="0123456789abcdef0123456789abcdef",
        source_url=f"http://data.gdeltproject.org/gdeltv2/{file_name}",
        file_name=file_name,
        gdelt_timestamp=timestamp,
        file_type=file_type,
    )


def test_filter_manifest_entries_filters_by_allowed_file_type() -> None:
    context = make_context()
    entries = [
        make_entry(
            file_type="export",
            timestamp=datetime(2026, 4, 7, 12, 0, 0, tzinfo=UTC),
            file_name="20260407120000.export.CSV.zip",
        ),
        make_entry(
            file_type="mentions",
            timestamp=datetime(2026, 4, 7, 12, 5, 0, tzinfo=UTC),
            file_name="20260407120500.mentions.CSV.zip",
        ),
    ]
    set_manifest_entries(context, entries)

    step = FilterManifestEntriesStep(allowed_file_types=("export",))
    step.run(context)

    filtered = get_filtered_manifest_entries(context)
    assert filtered is not None
    assert len(filtered) == 1
    assert filtered[0].file_type == "export"


def test_filter_manifest_entries_filters_by_date_from() -> None:
    context = make_context()
    entries = [
        make_entry(
            file_type="export",
            timestamp=datetime(2026, 4, 7, 11, 59, 0, tzinfo=UTC),
            file_name="20260407115900.export.CSV.zip",
        ),
        make_entry(
            file_type="export",
            timestamp=datetime(2026, 4, 7, 12, 0, 0, tzinfo=UTC),
            file_name="20260407120000.export.CSV.zip",
        ),
    ]
    set_manifest_entries(context, entries)

    step = FilterManifestEntriesStep(
        allowed_file_types=("export",),
        date_from=datetime(2026, 4, 7, 12, 0, 0, tzinfo=UTC),
    )
    step.run(context)

    filtered = get_filtered_manifest_entries(context)
    assert filtered is not None
    assert len(filtered) == 1
    assert filtered[0].file_name == "20260407120000.export.CSV.zip"


def test_filter_manifest_entries_filters_by_date_to() -> None:
    context = make_context()
    entries = [
        make_entry(
            file_type="export",
            timestamp=datetime(2026, 4, 7, 12, 0, 0, tzinfo=UTC),
            file_name="20260407120000.export.CSV.zip",
        ),
        make_entry(
            file_type="export",
            timestamp=datetime(2026, 4, 7, 12, 1, 0, tzinfo=UTC),
            file_name="20260407120100.export.CSV.zip",
        ),
    ]
    set_manifest_entries(context, entries)

    step = FilterManifestEntriesStep(
        allowed_file_types=("export",),
        date_to=datetime(2026, 4, 7, 12, 0, 0, tzinfo=UTC),
    )
    step.run(context)

    filtered = get_filtered_manifest_entries(context)
    assert filtered is not None
    assert len(filtered) == 1
    assert filtered[0].file_name == "20260407120000.export.CSV.zip"


def test_filter_manifest_entries_combines_file_type_and_date_window() -> None:
    context = make_context()
    entries = [
        make_entry(
            file_type="export",
            timestamp=datetime(2026, 4, 7, 12, 0, 0, tzinfo=UTC),
            file_name="20260407120000.export.CSV.zip",
        ),
        make_entry(
            file_type="mentions",
            timestamp=datetime(2026, 4, 7, 12, 0, 0, tzinfo=UTC),
            file_name="20260407120000.mentions.CSV.zip",
        ),
        make_entry(
            file_type="export",
            timestamp=datetime(2026, 4, 7, 12, 10, 0, tzinfo=UTC),
            file_name="20260407121000.export.CSV.zip",
        ),
    ]
    set_manifest_entries(context, entries)

    step = FilterManifestEntriesStep(
        allowed_file_types=("export",),
        date_from=datetime(2026, 4, 7, 12, 0, 0, tzinfo=UTC),
        date_to=datetime(2026, 4, 7, 12, 5, 0, tzinfo=UTC),
    )
    step.run(context)

    filtered = get_filtered_manifest_entries(context)
    assert filtered is not None
    assert len(filtered) == 1
    assert filtered[0].file_name == "20260407120000.export.CSV.zip"


def test_filter_manifest_entries_raises_when_manifest_entries_missing() -> None:
    context = make_context()

    step = FilterManifestEntriesStep()

    with pytest.raises(
        ValueError, match="Expected manifest entries in pipeline context"
    ):
        step.run(context)
