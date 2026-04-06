from __future__ import annotations

from datetime import UTC, datetime

import pytest

from deng_ingestion.manifest.parser import parse_manifest_line


def test_parse_manifest_line_parses_valid_export_entry() -> None:
    line = (
        "12345 "
        "0123456789abcdef0123456789abcdef "
        "http://data.gdeltproject.org/gdeltv2/20260407123000.export.CSV.zip"
    )

    entry = parse_manifest_line(line)

    assert entry is not None
    assert entry.file_type == "export"
    assert (
        entry.source_url
        == "http://data.gdeltproject.org/gdeltv2/20260407123000.export.CSV.zip"
    )
    assert entry.file_name == "20260407123000.export.CSV.zip"
    assert entry.file_size_bytes == 12345
    assert entry.md5_hash == "0123456789abcdef0123456789abcdef"
    assert entry.gdelt_timestamp == datetime(2026, 4, 7, 12, 30, 0, tzinfo=UTC)


def test_parse_manifest_line_returns_none_for_invalid_line() -> None:
    line = "this is not a valid gdelt manifest line"

    entry = parse_manifest_line(line)

    assert entry is None


def test_parse_manifest_line_parses_non_export_file_type() -> None:
    line = (
        "54321 "
        "fedcba9876543210fedcba9876543210 "
        "http://data.gdeltproject.org/gdeltv2/20260407124500.mentions.CSV.zip"
    )

    entry = parse_manifest_line(line)

    assert entry is not None
    assert entry.file_type == "mentions"
    assert entry.file_name == "20260407124500.mentions.CSV.zip"
    assert entry.file_size_bytes == 54321
    assert entry.md5_hash == "fedcba9876543210fedcba9876543210"
    assert entry.gdelt_timestamp == datetime(2026, 4, 7, 12, 45, 0, tzinfo=UTC)


def test_parse_manifest_line_extracts_filename_from_url() -> None:
    line = (
        "999 "
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa "
        "http://data.gdeltproject.org/gdeltv2/20250101000000.export.CSV.zip"
    )

    entry = parse_manifest_line(line)

    assert entry is not None
    assert entry.file_name == "20250101000000.export.CSV.zip"


def test_parse_manifest_line_raises_for_bad_timestamp_filename() -> None:
    line = (
        "100 "
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb "
        "http://data.gdeltproject.org/gdeltv2/not-a-timestamp.export.CSV.zip"
    )

    with pytest.raises(ValueError, match="does not match format"):
        parse_manifest_line(line)
