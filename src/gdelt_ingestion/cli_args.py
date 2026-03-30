from __future__ import annotations

from argparse import ArgumentParser, ArgumentTypeError


def non_negative_int(value: str) -> int:
    """argparse type: integer >= 0."""
    try:
        parsed = int(value)
    except ValueError as e:
        raise ArgumentTypeError(f"Expected integer, got {value!r}") from e

    if parsed < 0:
        raise ArgumentTypeError(f"Expected value >= 0, got {parsed}")

    return parsed


def add_relative_time_args(parser: ArgumentParser) -> None:
    """Add relative time window arguments like --years 1 --months 6 --days 10."""
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
