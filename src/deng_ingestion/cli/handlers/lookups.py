from __future__ import annotations

from argparse import Namespace

from loguru import logger

from deng_ingestion.jobs import build_load_lookups_job

from .common import build_context, run_job_with_context_connection


def handle_lookups_load(args: Namespace) -> None:
    logger.info("Starting lookup load")

    job = build_load_lookups_job()
    context = build_context("lookups_load")

    run_job_with_context_connection(job, context)

    loaded_lookup_counts = context.data.get("loaded_lookup_counts", {})
    seeded_risk_category_mapping_count = context.data.get(
        "seeded_risk_category_mapping_count",
        0,
    )

    logger.info(
        "Finished lookup load: loaded_lookup_counts={}, seeded_risk_category_mapping_count={}",
        loaded_lookup_counts,
        seeded_risk_category_mapping_count,
    )
