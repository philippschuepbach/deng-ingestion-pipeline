from __future__ import annotations

from argparse import Namespace
from datetime import UTC, datetime

from dateutil.relativedelta import relativedelta
from loguru import logger

from deng_ingestion.db.connection import get_connection
from deng_ingestion.jobs.build_risk_alerts_gold import build_risk_alerts_gold_job
from deng_ingestion.jobs.ingest_export_events import (
    build_ingest_all_export_events_job,
    build_ingest_export_events_job,
    build_ingest_registered_export_events_job,
)
from deng_ingestion.jobs.ingest_manifest import (
    build_backfill_manifest_job,
    build_incremental_manifest_job,
)
from deng_ingestion.jobs.load_lookups import build_load_lookups_job
from deng_ingestion.jobs.run_incremental_pipeline import (
    build_incremental_pipeline_job,
)
from deng_ingestion.jobs.transform_events import (
    build_transform_all_events_job,
    build_transform_events_job,
)
from deng_ingestion.paths import PROJECT_ROOT
from deng_ingestion.pipeline.context import PipelineContext


def _build_context(
    run_id: str, execution_ts: datetime | None = None
) -> PipelineContext:
    ts = execution_ts or datetime.now(UTC)

    return PipelineContext(
        run_id=run_id,
        execution_ts=ts,
        working_dir=PROJECT_ROOT,
    )


def _has_backfill_window(args: Namespace) -> bool:
    years = getattr(args, "years", 0)
    months = getattr(args, "months", 0)
    days = getattr(args, "days", 0)

    return (years > 0) or (months > 0) or (days > 0)


def handle_manifest_incremental(args: Namespace) -> None:
    logger.info("Starting manifest incremental ingestion")

    job = build_incremental_manifest_job()
    context = _build_context("manifest_incremental")

    job.run(context)

    logger.info("Finished manifest incremental ingestion")


def handle_manifest_backfill(args: Namespace) -> None:
    years = args.years
    months = args.months
    days = args.days

    if years == 0 and months == 0 and days == 0:
        raise ValueError(
            "Backfill requires a non-zero relative time window, "
            "for example: --months 6"
        )

    now = datetime.now(UTC)
    date_from = now - relativedelta(
        years=years,
        months=months,
        days=days,
    )
    date_to = now

    logger.info(
        "Starting manifest backfill ingestion: years={}, months={}, days={}, date_from={}, date_to={}",
        years,
        months,
        days,
        date_from.isoformat(),
        date_to.isoformat(),
    )

    job = build_backfill_manifest_job(
        date_from=date_from,
        date_to=date_to,
    )
    context = _build_context(
        run_id=f"manifest_backfill_{years}y_{months}m_{days}d",
        execution_ts=now,
    )

    job.run(context)

    logger.info("Finished manifest backfill ingestion")


def handle_export_ingest(args: Namespace) -> None:
    logger.info("Starting export ingestion")

    job = build_ingest_export_events_job()
    context = _build_context("export_ingest")

    conn = get_connection()
    context.data["db_connection"] = conn

    try:
        job.run(context)
    finally:
        context.data.pop("db_connection", None)
        conn.close()

    current_batch = context.data.get("current_batch")
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
    context = _build_context("export_ingest_all")

    job.run(context)

    processed_batches = context.data.get("processed_batches", 0)
    logger.info("Finished export ingest-all: processed_batches={}", processed_batches)


def handle_export_ingest_current_run(args: Namespace) -> None:
    logger.info("Starting export ingest-current-run")

    job = build_ingest_registered_export_events_job()
    context = _build_context("export_ingest_current_run")

    job.run(context)

    processed_batches = context.data.get("processed_batches", 0)
    ingested_export_batch_ids = context.data.get("ingested_export_batch_ids", [])

    logger.info(
        "Finished export ingest-current-run: processed_batches={}, ingested_export_batch_ids={}",
        processed_batches,
        ingested_export_batch_ids,
    )


def handle_lookups_load(args: Namespace) -> None:
    logger.info("Starting lookup load")

    job = build_load_lookups_job()
    context = _build_context("lookups_load")

    conn = get_connection()
    context.data["db_connection"] = conn

    try:
        job.run(context)
    finally:
        context.data.pop("db_connection", None)
        conn.close()

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


def handle_silver_transform(args: Namespace) -> None:
    logger.info("Starting silver transform")

    job = build_transform_events_job()
    context = _build_context("silver_transform")

    conn = get_connection()
    context.data["db_connection"] = conn

    try:
        job.run(context)
    finally:
        context.data.pop("db_connection", None)
        conn.close()

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
    context = _build_context("silver_transform_all")

    job.run(context)

    processed_batches = context.data.get("processed_silver_batches", 0)
    logger.info(
        "Finished silver transform-all: processed_batches={}", processed_batches
    )


def handle_gold_build(args: Namespace) -> None:
    logger.info("Starting gold build")

    job = build_risk_alerts_gold_job()
    context = _build_context("gold_build")

    conn = get_connection()
    context.data["db_connection"] = conn

    try:
        job.run(context)
    finally:
        context.data.pop("db_connection", None)
        conn.close()

    gold_row_count = context.data.get("gold_row_count", 0)
    logger.info("Finished gold build: gold_row_count={}", gold_row_count)


def handle_pipeline_incremental(args: Namespace) -> None:
    logger.info("Starting pipeline incremental")

    job = build_incremental_pipeline_job()
    context = _build_context("pipeline_incremental")

    job.run(context)

    logger.info(
        "Finished pipeline incremental: registered_export_batch_ids={}, ingested_export_batch_ids={}, transformed_export_batch_ids={}, gold_row_count={}",
        context.data.get("registered_export_batch_ids", []),
        context.data.get("ingested_export_batch_ids", []),
        context.data.get("transformed_export_batch_ids", []),
        context.data.get("gold_row_count", 0),
    )


def handle_quickstart(args: Namespace) -> None:
    logger.info("Starting quickstart")

    handle_lookups_load(args)

    if _has_backfill_window(args):
        logger.info(
            "Quickstart selected backfill mode: years={}, months={}, days={}",
            args.years,
            args.months,
            args.days,
        )
        handle_manifest_backfill(args)
    else:
        logger.info("Quickstart selected incremental mode")
        handle_manifest_incremental(args)

    handle_export_ingest_all(args)
    handle_silver_transform_all(args)
    handle_gold_build(args)

    logger.info("Finished quickstart")


def dispatch(args: Namespace) -> None:
    if args.command == "manifest":
        if args.manifest_command == "incremental":
            handle_manifest_incremental(args)
            return

        if args.manifest_command == "backfill":
            handle_manifest_backfill(args)
            return

    if args.command == "export":
        if args.export_command == "ingest":
            handle_export_ingest(args)
            return

        if args.export_command == "ingest-all":
            handle_export_ingest_all(args)
            return

        if args.export_command == "ingest-current-run":
            handle_export_ingest_current_run(args)
            return

    if args.command == "lookups":
        if args.lookups_command == "load":
            handle_lookups_load(args)
            return

    if args.command == "silver":
        if args.silver_command == "transform":
            handle_silver_transform(args)
            return

        if args.silver_command == "transform-all":
            handle_silver_transform_all(args)
            return

    if args.command == "gold":
        if args.gold_command == "build":
            handle_gold_build(args)
            return

    if args.command == "pipeline":
        if args.pipeline_command == "incremental":
            handle_pipeline_incremental(args)
            return

    if args.command == "quickstart":
        handle_quickstart(args)
        return

    raise ValueError(
        "Unsupported CLI command combination: "
        f"command={args.command}, "
        f"manifest_command={getattr(args, 'manifest_command', None)}, "
        f"export_command={getattr(args, 'export_command', None)}, "
        f"lookups_command={getattr(args, 'lookups_command', None)}, "
        f"silver_command={getattr(args, 'silver_command', None)}, "
        f"gold_command={getattr(args, 'gold_command', None)}, "
        f"pipeline_command={getattr(args, 'pipeline_command', None)}"
    )
