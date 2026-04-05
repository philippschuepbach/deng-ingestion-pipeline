from __future__ import annotations

from argparse import Namespace
from datetime import UTC, datetime

from deng_ingestion.db.connection import get_connection
from deng_ingestion.core.paths import PROJECT_ROOT
from deng_ingestion.pipeline.context import PipelineContext


def build_context(run_id: str, execution_ts: datetime | None = None) -> PipelineContext:
    ts = execution_ts or datetime.now(UTC)

    return PipelineContext(
        run_id=run_id,
        execution_ts=ts,
        working_dir=PROJECT_ROOT,
    )


def has_backfill_window(args: Namespace) -> bool:
    years = getattr(args, "years", 0)
    months = getattr(args, "months", 0)
    days = getattr(args, "days", 0)

    return (years > 0) or (months > 0) or (days > 0)


def run_job_with_context_connection(job, context: PipelineContext) -> None:
    conn = get_connection()
    context.data["db_connection"] = conn

    try:
        job.run(context)
    finally:
        context.data.pop("db_connection", None)
        conn.close()
