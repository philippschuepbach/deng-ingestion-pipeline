from __future__ import annotations

from argparse import Namespace

from loguru import logger

from deng_ingestion.jobs import build_load_lookups_job
from deng_ingestion.pipeline.context_access import (
    get_loaded_lookup_counts,
    get_seeded_risk_category_mapping_count,
)

from .common import build_context, run_job_with_context_connection


def handle_lookups_load(args: Namespace) -> None:
    logger.info("Starting lookup load")

    job = build_load_lookups_job()
    context = build_context("lookups_load")

    run_job_with_context_connection(job, context)

    loaded_lookup_counts = get_loaded_lookup_counts(context)
    seeded_risk_category_mapping_count = get_seeded_risk_category_mapping_count(
        context,
    )

    logger.info(
        (
            "Finished lookup load: "
            "loaded_lookup_counts={}, seeded_risk_category_mapping_count={}"
        ),
        loaded_lookup_counts,
        seeded_risk_category_mapping_count,
    )
