from __future__ import annotations

import calendar
from argparse import Namespace
from datetime import datetime, timedelta, timezone


def has_relative_time_window(args: Namespace) -> bool:
    """Return True if any relative window arg was provided."""
    return (args.years > 0) or (args.months > 0) or (args.days > 0)


def shift_datetime_back(
    dt: datetime,
    *,
    years: int = 0,
    months: int = 0,
    days: int = 0,
) -> datetime:
    """
    Shift a datetime back by years/months/days using only stdlib.

    Example:
    2026-03-31 minus 1 month -> 2026-02-28
    """
    total_months_back = years * 12 + months

    total_month_index = dt.year * 12 + (dt.month - 1)
    shifted_index = total_month_index - total_months_back

    new_year = shifted_index // 12
    new_month = (shifted_index % 12) + 1
    new_day = min(dt.day, calendar.monthrange(new_year, new_month)[1])

    shifted = dt.replace(year=new_year, month=new_month, day=new_day)
    shifted = shifted - timedelta(days=days)
    return shifted


def to_gdelt_timestamp(dt: datetime) -> str:
    """Convert datetime to GDELT timestamp format YYYYMMDDHHMMSS in UTC."""
    return dt.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S")


def resolve_relative_window(
    *,
    years: int = 0,
    months: int = 0,
    days: int = 0,
    now: datetime | None = None,
) -> tuple[str, str]:
    """
    Resolve relative window to (start_ts, end_ts) in GDELT format.
    """
    if years == 0 and months == 0 and days == 0:
        raise ValueError("At least one of years, months or days must be greater than 0")

    end_dt = now or datetime.now(timezone.utc)
    start_dt = shift_datetime_back(
        end_dt,
        years=years,
        months=months,
        days=days,
    )

    return to_gdelt_timestamp(start_dt), to_gdelt_timestamp(end_dt)
