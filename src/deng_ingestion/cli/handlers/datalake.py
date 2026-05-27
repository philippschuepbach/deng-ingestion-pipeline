from __future__ import annotations

from argparse import Namespace

from loguru import logger

from deng_ingestion.jobs import (
    build_ingest_lookup_files_to_datalake_job,
    build_ingest_registered_export_batches_to_datalake_job,
)
from deng_ingestion.pipeline.context_access import (
    get_processed_batches,
    get_uploaded_export_batch_ids,
    get_uploaded_lookup_object_paths,
)

from .common import build_context, run_job_with_context_connection


def handle_datalake_lookups_ingest(args: Namespace) -> None:
    logger.info("Starting datalake lookups-ingest")

    job = build_ingest_lookup_files_to_datalake_job()
    context = build_context("datalake_lookups_ingest")

    job.run(context)

    uploaded_lookup_object_paths = get_uploaded_lookup_object_paths(context)

    logger.info(
        "Finished datalake lookups-ingest: uploaded_lookup_files={}",
        sorted(uploaded_lookup_object_paths.keys()),
    )


def handle_datalake_export_ingest_current_run(args: Namespace) -> None:
    logger.info("Starting datalake export-ingest-current-run")

    job = build_ingest_registered_export_batches_to_datalake_job()
    context = build_context("datalake_export_ingest_current_run")

    run_job_with_context_connection(job, context)

    processed_batches = get_processed_batches(context)
    uploaded_export_batch_ids = get_uploaded_export_batch_ids(context)

    logger.info(
        (
            "Finished datalake export-ingest-current-run: "
            "processed_batches={}, uploaded_export_batch_ids={}"
        ),
        processed_batches,
        uploaded_export_batch_ids,
    )
