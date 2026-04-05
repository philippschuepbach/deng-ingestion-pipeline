from __future__ import annotations

from argparse import _SubParsersAction

from deng_ingestion.cli.parser_common import add_relative_time_args


def add_quickstart_parser(subparsers: _SubParsersAction) -> None:
    quickstart_parser = subparsers.add_parser(
        "quickstart",
        help="Run the full local pipeline end-to-end",
    )
    add_relative_time_args(quickstart_parser)
