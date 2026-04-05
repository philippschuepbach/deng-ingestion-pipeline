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
