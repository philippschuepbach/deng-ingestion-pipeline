from __future__ import annotations

from argparse import _SubParsersAction


def add_warehouse_parser(subparsers: _SubParsersAction) -> None:
    warehouse_parser = subparsers.add_parser(
        "warehouse",
        help="Data warehouse transformation commands",
    )
    warehouse_subparsers = warehouse_parser.add_subparsers(
        dest="warehouse_command",
        required=True,
    )

    warehouse_subparsers.add_parser(
        "build-events-silver",
        help=(
            "Create the bronze external table, create the partitioned and "
            "clustered events_silver table, and merge transformed events into it"
        ),
    )

    warehouse_subparsers.add_parser(
        "build-risk-alerts-gold",
        help=(
            "Create lookup and gold warehouse tables, then rebuild the "
            "partitioned and clustered risk_alerts_gold output"
        ),
    )
