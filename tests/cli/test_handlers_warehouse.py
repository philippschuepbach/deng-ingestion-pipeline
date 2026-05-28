from __future__ import annotations

from argparse import Namespace

import pytest

import deng_ingestion.cli.handlers.warehouse as warehouse_handlers


class DummyJob:
    def __init__(self) -> None:
        self.ran = False
        self.context = None

    def run(self, context) -> None:
        self.ran = True
        self.context = context


def test_handle_warehouse_build_risk_alerts_gold_runs_job_with_expected_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = DummyJob()

    def fake_build_risk_alerts_gold_warehouse_job() -> DummyJob:
        return job

    monkeypatch.setattr(
        warehouse_handlers,
        "build_risk_alerts_gold_warehouse_job",
        fake_build_risk_alerts_gold_warehouse_job,
    )

    warehouse_handlers.handle_warehouse_build_risk_alerts_gold(Namespace())

    assert job.ran is True
    assert job.context is not None
    assert job.context.run_id == "warehouse_build_risk_alerts_gold"
