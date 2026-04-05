from __future__ import annotations

from argparse import Namespace

from loguru import logger

from deng_ingestion.jobs import build_incremental_pipeline_job

from .common import build_context


def handle_pipeline_incremental(args: Namespace) -> None:
    logger.info("Starting pipeline incremental")

    job = build_incremental_pipeline_job()
    context = build_context("pipeline_incremental")

    job.run(context)

    logger.info(
        "Finished pipeline incremental: registered_export_batch_ids={}, ingested_export_batch_ids={}, transformed_export_batch_ids={}, gold_row_count={}",
        context.data.get("registered_export_batch_ids", []),
        context.data.get("ingested_export_batch_ids", []),
        context.data.get("transformed_export_batch_ids", []),
        context.data.get("gold_row_count", 0),
    )
