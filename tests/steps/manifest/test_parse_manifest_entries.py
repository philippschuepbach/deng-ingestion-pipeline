from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    get_manifest_entries,
    set_manifest_text,
)
from deng_ingestion.steps.manifest.parse_manifest_entries import (
    ParseManifestEntriesStep,
)


def make_context() -> PipelineContext:
    return PipelineContext(
        run_id="test",
        execution_ts=datetime(2026, 4, 8, 12, 0, 0, tzinfo=UTC),
        working_dir=Path("."),
    )


def test_parse_manifest_entries_parses_valid_lines() -> None:
    context = make_context()
    set_manifest_text(
        context,
        "\n".join(
            [
                "12345 0123456789abcdef0123456789abcdef "
                "http://data.gdeltproject.org/gdeltv2/20260408120000.export.CSV.zip",
                "54321 fedcba9876543210fedcba9876543210 "
                "http://data.gdeltproject.org/gdeltv2/20260408121500.mentions.CSV.zip",
            ]
        ),
    )

    step = ParseManifestEntriesStep()
    step.run(context)

    entries = get_manifest_entries(context)
    assert entries is not None
    assert len(entries) == 2

    assert entries[0].file_name == "20260408120000.export.CSV.zip"
    assert entries[0].file_type == "export"

    assert entries[1].file_name == "20260408121500.mentions.CSV.zip"
    assert entries[1].file_type == "mentions"


def test_parse_manifest_entries_skips_invalid_and_blank_lines() -> None:
    context = make_context()
    set_manifest_text(
        context,
        "\n".join(
            [
                "",
                "not a valid manifest line",
                "12345 0123456789abcdef0123456789abcdef "
                "http://data.gdeltproject.org/gdeltv2/20260408120000.export.CSV.zip",
                "also invalid",
            ]
        ),
    )

    step = ParseManifestEntriesStep()
    step.run(context)

    entries = get_manifest_entries(context)
    assert entries is not None
    assert len(entries) == 1
    assert entries[0].file_name == "20260408120000.export.CSV.zip"


def test_parse_manifest_entries_raises_when_manifest_text_missing() -> None:
    context = make_context()

    step = ParseManifestEntriesStep()

    with pytest.raises(ValueError, match="Expected manifest text in pipeline context"):
        step.run(context)
