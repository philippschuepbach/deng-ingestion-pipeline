from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.core.http import download_binary_to_file
from deng_ingestion.lookups.config import (
    GDELT_LOOKUP_FILES,
    GDELT_LOOKUPS_BASE_URL,
)
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    set_downloaded_lookup_files,
    set_lookup_dir,
    set_reused_lookup_files,
)


@dataclass(frozen=True)
class DownloadLookupFilesStep:
    name: str = "download_lookup_files"

    def run(self, context: PipelineContext) -> None:
        lookup_dir = context.working_dir / "data" / "lookups"
        lookup_dir.mkdir(parents=True, exist_ok=True)

        downloaded_files: list[str] = []
        reused_files: list[str] = []

        for file_name in GDELT_LOOKUP_FILES:
            target_path = lookup_dir / file_name

            if target_path.exists():
                logger.debug(
                    "Lookup file already exists locally, reusing file: {}", target_path
                )
                reused_files.append(file_name)
                continue

            source_url = f"{GDELT_LOOKUPS_BASE_URL}{file_name}"

            logger.info("Downloading lookup file: {} -> {}", source_url, target_path)

            download_binary_to_file(
                source_url,
                target_path,
                timeout_seconds=15.0,
                retries=3,
            )
            downloaded_files.append(file_name)

        set_lookup_dir(context, lookup_dir)
        set_downloaded_lookup_files(context, downloaded_files)
        set_reused_lookup_files(context, reused_files)

        logger.info(
            "Finished lookup file sync: downloaded_files={}, reused_files={}",
            len(downloaded_files),
            len(reused_files),
        )
