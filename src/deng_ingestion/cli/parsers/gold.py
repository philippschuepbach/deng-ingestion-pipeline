from __future__ import annotations

from argparse import _SubParsersAction


def add_gold_parser(subparsers: _SubParsersAction) -> None:
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
