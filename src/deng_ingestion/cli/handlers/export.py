from __future__ import annotations

from argparse import Namespace

from loguru import logger

from deng_ingestion.jobs import (
    build_ingest_all_export_events_job,
    build_ingest_export_events_job,
    build_ingest_registered_export_events_job,
)
from deng_ingestion.pipeline.context_access import (
    get_current_batch,
    get_ingested_export_batch_ids,
    get_processed_batches,
)

from .common import build_context, run_job_with_context_connection


def handle_export_ingest(args: Namespace) -> None:
    logger.info("Starting export ingestion")

    job = build_ingest_export_events_job()
    context = build_context("export_ingest")

    run_job_with_context_connection(job, context)

    current_batch = get_current_batch(context)
    if current_batch is None:
        logger.info("No pending export batch was available")
    else:
        logger.info(
            "Finished export ingestion for batch_id={}, file_name={}",
            current_batch["batch_id"],
            current_batch["file_name"],
        )


def handle_export_ingest_all(args: Namespace) -> None:
    logger.info("Starting export ingest-all")

    job = build_ingest_all_export_events_job()
    context = build_context("export_ingest_all")

    job.run(context)

    processed_batches = get_processed_batches(context)
    logger.info("Finished export ingest-all: processed_batches={}", processed_batches)


def handle_export_ingest_current_run(args: Namespace) -> None:
    logger.info("Starting export ingest-current-run")

    job = build_ingest_registered_export_events_job()
    context = build_context("export_ingest_current_run")

    job.run(context)

    processed_batches = get_processed_batches(context)
    ingested_export_batch_ids = get_ingested_export_batch_ids(context)

    logger.info(
        (
            "Finished export ingest-current-run: "
            "processed_batches={}, ingested_export_batch_ids={}"
        ),
        processed_batches,
        ingested_export_batch_ids,
    )
