from __future__ import annotations

from argparse import _SubParsersAction


def add_silver_parser(subparsers: _SubParsersAction) -> None:
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
