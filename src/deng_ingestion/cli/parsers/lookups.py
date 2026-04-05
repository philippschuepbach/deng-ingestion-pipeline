from __future__ import annotations

from argparse import _SubParsersAction


def add_lookups_parser(subparsers: _SubParsersAction) -> None:
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
