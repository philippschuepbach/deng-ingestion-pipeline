from __future__ import annotations

import argparse


def non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("Value must be a non-negative integer")
    return parsed


def add_relative_time_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--year",
        "--years",
        dest="years",
        type=non_negative_int,
        default=0,
        help="Years to look back from now (UTC)",
    )
    parser.add_argument(
        "--month",
        "--months",
        dest="months",
        type=non_negative_int,
        default=0,
        help="Months to look back from now (UTC)",
    )
    parser.add_argument(
        "--day",
        "--days",
        dest="days",
        type=non_negative_int,
        default=0,
        help="Days to look back from now (UTC)",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="deng-ingestion",
        description="Geopolitical risk ingestion pipeline CLI",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # ------------------------------------------------------------
    # manifest
    # ------------------------------------------------------------
    manifest_parser = subparsers.add_parser(
        "manifest",
        help="Manifest ingestion commands",
    )
    manifest_subparsers = manifest_parser.add_subparsers(
        dest="manifest_command",
        required=True,
    )

    manifest_subparsers.add_parser(
        "incremental",
        help="Load the latest export batches from lastupdate.txt",
    )

    backfill_parser = manifest_subparsers.add_parser(
        "backfill",
        help="Load historical export batches from masterfilelist.txt",
    )
    add_relative_time_args(backfill_parser)

    # ------------------------------------------------------------
    # export
    # ------------------------------------------------------------
    export_parser = subparsers.add_parser(
        "export",
        help="Export event ingestion commands",
    )
    export_subparsers = export_parser.add_subparsers(
        dest="export_command",
        required=True,
    )

    export_subparsers.add_parser(
        "ingest",
        help="Download, extract, and load the next pending export batch into bronze",
    )

    export_subparsers.add_parser(
        "ingest-all",
        help="Download, extract, and load all currently pending export batches into bronze",
    )

    # ------------------------------------------------------------
    # lookups
    # ------------------------------------------------------------
    lookups_parser = subparsers.add_parser(
        "lookups",
        help="Lookup dimension loading commands",
    )
    lookups_subparsers = lookups_parser.add_subparsers(
        dest="lookups_command",
        required=True,
    )

    lookups_subparsers.add_parser(
        "load",
        help="Download lookup files if needed, load dimension tables, and seed risk category mapping",
    )

    # ------------------------------------------------------------
    # silver
    # ------------------------------------------------------------
    silver_parser = subparsers.add_parser(
        "silver",
        help="Silver-layer transformation commands",
    )
    silver_subparsers = silver_parser.add_subparsers(
        dest="silver_command",
        required=True,
    )

    silver_subparsers.add_parser(
        "transform",
        help="Transform the next pending bronze batch into the silver layer",
    )

    silver_subparsers.add_parser(
        "transform-all",
        help="Transform all currently pending bronze batches into the silver layer",
    )

    # ------------------------------------------------------------
    # gold
    # ------------------------------------------------------------
    gold_parser = subparsers.add_parser(
        "gold",
        help="Gold-layer aggregation commands",
    )
    gold_subparsers = gold_parser.add_subparsers(
        dest="gold_command",
        required=True,
    )

    gold_subparsers.add_parser(
        "build",
        help="Rebuild the gold aggregation table from all silver events",
    )

    # ------------------------------------------------------------
    # quickstart
    # ------------------------------------------------------------
    quickstart_parser = subparsers.add_parser(
        "quickstart",
        help="Run the full local pipeline end-to-end",
    )
    add_relative_time_args(quickstart_parser)

    return parser
