from __future__ import annotations

from argparse import Namespace

from loguru import logger

from deng_ingestion.jobs import (
    build_events_silver_warehouse_job,
    build_risk_alerts_gold_warehouse_job,
)

from .common import build_context


def handle_warehouse_build_events_silver(args: Namespace) -> None:
    logger.info("Starting warehouse build-events-silver")

    job = build_events_silver_warehouse_job()
    context = build_context("warehouse_build_events_silver")

    job.run(context)

    logger.info("Finished warehouse build-events-silver")


def handle_warehouse_build_risk_alerts_gold(args: Namespace) -> None:
    logger.info("Starting warehouse build-risk-alerts-gold")

    job = build_risk_alerts_gold_warehouse_job()
    context = build_context("warehouse_build_risk_alerts_gold")

    job.run(context)

    logger.info("Finished warehouse build-risk-alerts-gold")
