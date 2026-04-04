# Copyright (C) 2026 Philipp Schüpbach
# This file is part of deng-ingestion-pipeline.
#
# deng-ingestion-pipeline is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# deng-ingestion-pipeline is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with deng-ingestion-pipeline. If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.request import urlopen

from loguru import logger

from gdelt_ingestion.lookups.config import (
    GDELT_LOOKUP_FILES,
    GDELT_LOOKUPS_BASE_URL,
)
from gdelt_ingestion.pipeline.context import PipelineContext


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
            temp_path = target_path.with_suffix(target_path.suffix + ".part")

            logger.info("Downloading lookup file: {} -> {}", source_url, target_path)

            try:
                with urlopen(source_url) as response, temp_path.open("wb") as target:
                    target.write(response.read())

                temp_path.replace(target_path)
                downloaded_files.append(file_name)

            except Exception:
                if temp_path.exists():
                    temp_path.unlink()
                raise

        context.data["lookup_dir"] = lookup_dir
        context.data["downloaded_lookup_files"] = downloaded_files
        context.data["reused_lookup_files"] = reused_files

        logger.info(
            "Finished lookup file sync: downloaded_files={}, reused_files={}",
            len(downloaded_files),
            len(reused_files),
        )
