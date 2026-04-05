from __future__ import annotations

from argparse import _SubParsersAction


def add_export_parser(subparsers: _SubParsersAction) -> None:
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

    export_subparsers.add_parser(
        "ingest-current-run",
        help="Internal/debug command: load export batches registered in the current pipeline context",
    )
