from __future__ import annotations

from argparse import _SubParsersAction

from deng_ingestion.cli.parser_common import add_relative_time_args


def add_manifest_parser(subparsers: _SubParsersAction) -> None:
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
