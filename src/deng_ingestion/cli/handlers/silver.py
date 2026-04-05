from __future__ import annotations

from argparse import Namespace

from loguru import logger

from deng_ingestion.jobs import (
    build_transform_all_events_job,
    build_transform_events_job,
)

from .common import build_context, run_job_with_context_connection


def handle_silver_transform(args: Namespace) -> None:
    logger.info("Starting silver transform")

    job = build_transform_events_job()
    context = build_context("silver_transform")

    run_job_with_context_connection(job, context)

    current_batch = context.data.get("current_silver_batch")
    if current_batch is None:
        logger.info("No pending silver batch was available")
    else:
        logger.info(
            "Finished silver transform for batch_id={}, file_name={}, inserted_rows={}",
            current_batch["batch_id"],
            current_batch["file_name"],
            context.data.get("last_silver_inserted_rows", 0),
        )


def handle_silver_transform_all(args: Namespace) -> None:
    logger.info("Starting silver transform-all")

    job = build_transform_all_events_job()
    context = build_context("silver_transform_all")

    job.run(context)

    processed_batches = context.data.get("processed_silver_batches", 0)
    logger.info(
        "Finished silver transform-all: processed_batches={}", processed_batches
    )
