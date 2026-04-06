from __future__ import annotations

from argparse import Namespace
from datetime import UTC, datetime

from dateutil.relativedelta import relativedelta
from loguru import logger

from deng_ingestion.jobs import (
    build_backfill_manifest_job,
    build_incremental_manifest_job,
)

from .common import build_context


def handle_manifest_incremental(args: Namespace) -> None:
    logger.info("Starting manifest incremental ingestion")

    job = build_incremental_manifest_job()
    context = build_context("manifest_incremental")

    job.run(context)

    logger.info("Finished manifest incremental ingestion")


def handle_manifest_backfill(args: Namespace) -> None:
    years = args.years
    months = args.months
    days = args.days

    if years == 0 and months == 0 and days == 0:
        raise ValueError(
            "Backfill requires a non-zero relative time window, for example: --months 6"
        )

    now = datetime.now(UTC)
    date_from = now - relativedelta(
        years=years,
        months=months,
        days=days,
    )
    date_to = now

    logger.info(
        (
            "Starting manifest backfill ingestion: "
            "years={}, months={}, days={}, date_from={}, date_to={}"
        ),
        years,
        months,
        days,
        date_from.isoformat(),
        date_to.isoformat(),
    )

    job = build_backfill_manifest_job(
        date_from=date_from,
        date_to=date_to,
    )
    context = build_context(
        run_id=f"manifest_backfill_{years}y_{months}m_{days}d",
        execution_ts=now,
    )

    job.run(context)

    logger.info("Finished manifest backfill ingestion")
