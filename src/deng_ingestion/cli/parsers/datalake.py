from __future__ import annotations

from argparse import _SubParsersAction


def add_datalake_parser(subparsers: _SubParsersAction) -> None:
    datalake_parser = subparsers.add_parser(
        "datalake",
        help="Data lake ingestion commands",
    )
    datalake_subparsers = datalake_parser.add_subparsers(
        dest="datalake_command",
        required=True,
    )

    datalake_subparsers.add_parser(
        "lookups-ingest",
        help=(
            "Download lookup files if needed, build object paths, "
            "and upload them to the data lake"
        ),
    )

    datalake_subparsers.add_parser(
        "export-ingest-current-run",
        help=(
            "Download, extract, and upload export files for batches "
            "registered in the current pipeline context"
        ),
    )
