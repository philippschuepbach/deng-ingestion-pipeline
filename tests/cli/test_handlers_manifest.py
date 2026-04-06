from __future__ import annotations

from argparse import Namespace
from datetime import UTC, datetime

import pytest

import deng_ingestion.cli.handlers.manifest as manifest_handlers


class DummyJob:
    def __init__(self) -> None:
        self.ran = False
        self.context = None

    def run(self, context) -> None:
        self.ran = True
        self.context = context


def test_handle_manifest_backfill_rejects_zero_window() -> None:
    args = Namespace(years=0, months=0, days=0)

    with pytest.raises(
        ValueError,
        match="Backfill requires a non-zero relative time window",
    ):
        manifest_handlers.handle_manifest_backfill(args)


def test_handle_manifest_incremental_runs_job_with_expected_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = DummyJob()

    def fake_build_incremental_manifest_job() -> DummyJob:
        return job

    monkeypatch.setattr(
        manifest_handlers,
        "build_incremental_manifest_job",
        fake_build_incremental_manifest_job,
    )

    args = Namespace()
    manifest_handlers.handle_manifest_incremental(args)

    assert job.ran is True
    assert job.context is not None
    assert job.context.run_id == "manifest_incremental"


def test_handle_manifest_backfill_builds_job_with_expected_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixed_now = datetime(2026, 4, 7, 12, 0, 0, tzinfo=UTC)
    captured: dict[str, object] = {}

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return fixed_now

    job = DummyJob()

    def fake_build_backfill_manifest_job(*, date_from, date_to) -> DummyJob:
        captured["date_from"] = date_from
        captured["date_to"] = date_to
        return job

    monkeypatch.setattr(manifest_handlers, "datetime", FixedDateTime)
    monkeypatch.setattr(
        manifest_handlers,
        "build_backfill_manifest_job",
        fake_build_backfill_manifest_job,
    )

    args = Namespace(years=0, months=6, days=1)
    manifest_handlers.handle_manifest_backfill(args)

    assert captured["date_to"] == fixed_now
    assert captured["date_from"] == datetime(2025, 10, 6, 12, 0, 0, tzinfo=UTC)

    assert job.ran is True
    assert job.context is not None
    assert job.context.run_id == "manifest_backfill_0y_6m_1d"
    assert job.context.execution_ts == fixed_now
