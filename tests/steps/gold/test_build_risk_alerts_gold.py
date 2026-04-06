from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import get_gold_row_count
from deng_ingestion.steps.gold import build_risk_alerts_gold as step_module
from deng_ingestion.steps.gold.build_risk_alerts_gold import BuildRiskAlertsGoldStep


def make_context(tmp_path: Path) -> PipelineContext:
    return PipelineContext(
        run_id="test",
        execution_ts=datetime(2026, 4, 8, 12, 0, 0, tzinfo=UTC),
        working_dir=tmp_path,
    )


class FakeCursor:
    def __init__(self) -> None:
        self.executed_sql: list[str] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str) -> None:
        self.executed_sql.append(sql)

    def fetchone(self) -> tuple[int]:
        return (7,)


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_obj = FakeCursor()
        self.committed = False
        self.rolled_back = False

    def cursor(self) -> FakeCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        return None


def test_build_risk_alerts_gold_sets_row_count_and_commits(
    monkeypatch,
    tmp_path: Path,
) -> None:
    context = make_context(tmp_path)
    fake_conn = FakeConnection()

    monkeypatch.setattr(
        step_module,
        "get_context_connection",
        lambda context: (fake_conn, False),
    )

    step = BuildRiskAlertsGoldStep()
    step.run(context)

    assert fake_conn.committed is True
    assert get_gold_row_count(context) == 7

    assert "TRUNCATE TABLE risk_alerts_gold" in fake_conn.cursor_obj.executed_sql[0]
    assert "INSERT INTO risk_alerts_gold" in fake_conn.cursor_obj.executed_sql[1]
    assert (
        "SELECT COUNT(*) FROM risk_alerts_gold" in fake_conn.cursor_obj.executed_sql[2]
    )
