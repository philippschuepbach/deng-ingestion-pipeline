from __future__ import annotations

from argparse import Namespace
from datetime import UTC, datetime

from dateutil.relativedelta import relativedelta
from loguru import logger

from deng_ingestion.jobs import (
    build_backfill_datalake_direct_job,
    build_incremental_datalake_direct_job,
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


def handle_datalake_incremental(args: Namespace) -> None:
    logger.info("Starting cloud-native datalake incremental pipeline")

    context = build_context("datalake_incremental")

    lookup_job = build_ingest_lookup_files_to_datalake_job()
    export_job = build_incremental_datalake_direct_job()

    lookup_job.run(context)
    export_job.run(context)

    processed_batches = get_processed_batches(context)
    uploaded_lookup_object_paths = get_uploaded_lookup_object_paths(context)

    logger.info(
        (
            "Finished cloud-native datalake incremental pipeline: "
            "processed_batches={}, uploaded_lookup_files={}"
        ),
        processed_batches,
        sorted(uploaded_lookup_object_paths.keys()),
    )


def handle_datalake_backfill(args: Namespace) -> None:
    years = args.years
    months = args.months
    days = args.days

    if years == 0 and months == 0 and days == 0:
        raise ValueError(
            "Datalake backfill requires a non-zero relative time window, "
            "for example: --days 2"
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
            "Starting cloud-native datalake backfill pipeline: "
            "years={}, months={}, days={}, date_from={}, date_to={}"
        ),
        years,
        months,
        days,
        date_from.isoformat(),
        date_to.isoformat(),
    )

    context = build_context(
        run_id=f"datalake_backfill_{years}y_{months}m_{days}d",
        execution_ts=now,
    )

    lookup_job = build_ingest_lookup_files_to_datalake_job()
    export_job = build_backfill_datalake_direct_job(
        date_from=date_from,
        date_to=date_to,
    )

    lookup_job.run(context)
    export_job.run(context)

    processed_batches = get_processed_batches(context)
    uploaded_lookup_object_paths = get_uploaded_lookup_object_paths(context)

    logger.info(
        (
            "Finished cloud-native datalake backfill pipeline: "
            "processed_batches={}, uploaded_lookup_files={}"
        ),
        processed_batches,
        sorted(uploaded_lookup_object_paths.keys()),
    )
