from __future__ import annotations

from argparse import Namespace

from loguru import logger

from deng_ingestion.jobs import build_risk_alerts_gold_job
from deng_ingestion.pipeline.context_access import get_gold_row_count

from .common import build_context, run_job_with_context_connection


def handle_gold_build(args: Namespace) -> None:
    logger.info("Starting gold build")

    job = build_risk_alerts_gold_job()
    context = build_context("gold_build")

    run_job_with_context_connection(job, context)

    gold_row_count = get_gold_row_count(context)
    logger.info("Finished gold build: gold_row_count={}", gold_row_count)
