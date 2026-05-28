from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from deng_ingestion.core.paths import PROJECT_ROOT
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.steps.warehouse import build_risk_alerts_gold as build_module
from deng_ingestion.steps.warehouse import (
    create_risk_alerts_gold_table as create_module,
)
from deng_ingestion.steps.warehouse.build_risk_alerts_gold import (
    BuildRiskAlertsGoldWarehouseStep,
)
from deng_ingestion.steps.warehouse.create_risk_alerts_gold_table import (
    CreateRiskAlertsGoldTableStep,
)


def make_context(tmp_path: Path) -> PipelineContext:
    return PipelineContext(
        run_id="test",
        execution_ts=datetime(2026, 4, 8, 12, 0, 0, tzinfo=UTC),
        working_dir=tmp_path,
    )


def test_create_risk_alerts_gold_table_runs_expected_sql(
    monkeypatch,
    tmp_path: Path,
) -> None:
    executed_paths: list[Path] = []

    monkeypatch.setattr(
        create_module,
        "run_sql_file",
        lambda path: executed_paths.append(path),
    )

    CreateRiskAlertsGoldTableStep().run(make_context(tmp_path))

    assert executed_paths == [
        PROJECT_ROOT / "sql" / "bigquery" / "create_risk_alerts_gold.sql"
    ]


def test_build_risk_alerts_gold_runs_expected_sql(
    monkeypatch,
    tmp_path: Path,
) -> None:
    executed_paths: list[Path] = []

    monkeypatch.setattr(
        build_module,
        "run_sql_file",
        lambda path: executed_paths.append(path),
    )

    BuildRiskAlertsGoldWarehouseStep().run(make_context(tmp_path))

    assert executed_paths == [
        PROJECT_ROOT / "sql" / "bigquery" / "build_risk_alerts_gold.sql"
    ]
