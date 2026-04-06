from __future__ import annotations

from argparse import _SubParsersAction


def add_pipeline_parser(subparsers: _SubParsersAction) -> None:
    pipeline_parser = subparsers.add_parser(
        "pipeline",
        help="Combined pipeline commands",
    )
    pipeline_subparsers = pipeline_parser.add_subparsers(
        dest="pipeline_command",
        required=True,
    )

    pipeline_subparsers.add_parser(
        "incremental",
        help=(
            "Run manifest incremental, export ingest-current-run, "
            "silver transform-current-run, and gold build in a single pipeline context"
        ),
    )
