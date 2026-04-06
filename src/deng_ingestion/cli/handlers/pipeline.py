from __future__ import annotations

from argparse import Namespace

from loguru import logger

from deng_ingestion.jobs import build_incremental_pipeline_job
from deng_ingestion.pipeline.context_access import (
    get_gold_row_count,
    get_ingested_export_batch_ids,
    get_registered_export_batch_ids,
    get_transformed_export_batch_ids,
)

from .common import build_context


def handle_pipeline_incremental(args: Namespace) -> None:
    logger.info("Starting pipeline incremental")

    job = build_incremental_pipeline_job()
    context = build_context("pipeline_incremental")

    job.run(context)

    logger.info(
        (
            "Finished pipeline incremental: "
            "registered_export_batch_ids={}, "
            "ingested_export_batch_ids={}, "
            "transformed_export_batch_ids={}, "
            "gold_row_count={}"
        ),
        get_registered_export_batch_ids(context),
        get_ingested_export_batch_ids(context),
        get_transformed_export_batch_ids(context),
        get_gold_row_count(context),
    )
