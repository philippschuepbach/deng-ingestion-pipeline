from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    get_downloaded_lookup_files,
    get_lookup_dir,
    get_reused_lookup_files,
)
from deng_ingestion.steps.lookup import download_lookup_files as step_module
from deng_ingestion.steps.lookup.download_lookup_files import DownloadLookupFilesStep


def make_context(tmp_path: Path) -> PipelineContext:
    return PipelineContext(
        run_id="test",
        execution_ts=datetime(2026, 4, 8, 12, 0, 0, tzinfo=UTC),
        working_dir=tmp_path,
    )


def test_download_lookup_files_downloads_missing_and_reuses_existing(
    monkeypatch,
    tmp_path: Path,
) -> None:
    context = make_context(tmp_path)
    lookup_dir = tmp_path / "data" / "lookups"
    lookup_dir.mkdir(parents=True, exist_ok=True)

    existing_file = lookup_dir / "existing.txt"
    existing_file.write_text("already here", encoding="utf-8")

    monkeypatch.setattr(
        step_module,
        "GDELT_LOOKUP_FILES",
        ["existing.txt", "missing.txt"],
    )
    monkeypatch.setattr(
        step_module,
        "GDELT_LOOKUPS_BASE_URL",
        "https://example.test/",
    )

    calls: list[tuple[str, Path]] = []

    def fake_download_binary_to_file(url: str, destination: Path, **kwargs) -> None:
        calls.append((url, destination))
        destination.write_text("downloaded", encoding="utf-8")

    monkeypatch.setattr(
        step_module,
        "download_binary_to_file",
        fake_download_binary_to_file,
    )

    step = DownloadLookupFilesStep()
    step.run(context)

    assert calls == [("https://example.test/missing.txt", lookup_dir / "missing.txt")]
    assert get_lookup_dir(context) == lookup_dir
    assert get_downloaded_lookup_files(context) == ["missing.txt"]
    assert get_reused_lookup_files(context) == ["existing.txt"]
    assert (lookup_dir / "missing.txt").exists()
