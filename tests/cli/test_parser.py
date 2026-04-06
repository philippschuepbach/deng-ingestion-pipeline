from __future__ import annotations

import pytest

from deng_ingestion.cli.parser import build_parser


def test_parser_requires_command() -> None:
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_manifest_incremental_parses() -> None:
    parser = build_parser()

    args = parser.parse_args(["manifest", "incremental"])

    assert args.command == "manifest"
    assert args.manifest_command == "incremental"


def test_manifest_backfill_parses_relative_time_args() -> None:
    parser = build_parser()

    args = parser.parse_args(
        ["manifest", "backfill", "--years", "1", "--months", "6", "--days", "2"]
    )

    assert args.command == "manifest"
    assert args.manifest_command == "backfill"
    assert args.years == 1
    assert args.months == 6
    assert args.days == 2


def test_manifest_backfill_accepts_singular_aliases() -> None:
    parser = build_parser()

    args = parser.parse_args(
        ["manifest", "backfill", "--year", "1", "--month", "2", "--day", "3"]
    )

    assert args.years == 1
    assert args.months == 2
    assert args.days == 3


def test_manifest_backfill_rejects_negative_values() -> None:
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["manifest", "backfill", "--months", "-1"])


def test_manifest_backfill_defaults_to_zero_window() -> None:
    parser = build_parser()

    args = parser.parse_args(["manifest", "backfill"])

    assert args.command == "manifest"
    assert args.manifest_command == "backfill"
    assert args.years == 0
    assert args.months == 0
    assert args.days == 0


def test_manifest_requires_subcommand() -> None:
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["manifest"])


def test_export_ingest_current_run_parses() -> None:
    parser = build_parser()

    args = parser.parse_args(["export", "ingest-current-run"])

    assert args.command == "export"
    assert args.export_command == "ingest-current-run"


def test_export_requires_subcommand() -> None:
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["export"])


@pytest.mark.parametrize(
    ("argv", "expected_command", "expected_subcommand_attr", "expected_subcommand"),
    [
        (["pipeline", "incremental"], "pipeline", "pipeline_command", "incremental"),
        (["quickstart"], "quickstart", None, None),
    ],
)
def test_pipeline_incremental_and_quickstart_parse(
    argv: list[str],
    expected_command: str,
    expected_subcommand_attr: str | None,
    expected_subcommand: str | None,
) -> None:
    parser = build_parser()

    args = parser.parse_args(argv)

    assert args.command == expected_command

    if expected_subcommand_attr is not None:
        assert getattr(args, expected_subcommand_attr) == expected_subcommand

    if expected_command == "quickstart":
        assert args.years == 0
        assert args.months == 0
        assert args.days == 0
