from __future__ import annotations

from dataclasses import dataclass

from deng_ingestion.core.paths import PROJECT_ROOT
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.steps.warehouse.bigquery_client import run_sql_file


@dataclass(frozen=True)
class MergeEventsSilverStep:
    name: str = "merge_events_silver"

    def run(self, context: PipelineContext) -> None:
        run_sql_file(PROJECT_ROOT / "sql" / "bigquery" / "merge_events_silver.sql")
