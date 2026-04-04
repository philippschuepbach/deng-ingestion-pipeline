# Copyright (C) 2026 Philipp Schüpbach
# This file is part of gdelt-ingestion-pipeline.
#
# gdelt-ingestion-pipeline is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# gdelt-ingestion-pipeline is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with gdelt-ingestion-pipeline. If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from zipfile import ZipFile

from loguru import logger

from gdelt_ingestion.pipeline.context import PipelineContext


@dataclass(frozen=True)
class ExtractExportCsvStep:
    name: str = "extract_export_csv"

    def run(self, context: PipelineContext) -> None:
        batch = context.data.get("current_batch")
        if batch is None:
            logger.info("Skipping extraction because no batch is selected")
            return

        archive_path: Path = context.data["archive_path"]
        raw_dir = context.working_dir / "data" / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "Extracting export archive: batch_id={}, archive_path={}",
            batch["batch_id"],
            archive_path,
        )

        with ZipFile(archive_path, "r") as zip_file:
            members = zip_file.namelist()
            if len(members) != 1:
                raise ValueError(
                    f"Expected exactly one file in archive {archive_path}, found {len(members)}"
                )

            member_name = members[0]
            extracted_path = raw_dir / Path(member_name).name

            if not extracted_path.exists():
                zip_file.extract(member_name, raw_dir)
                extracted_nested_path = raw_dir / member_name

                if (
                    extracted_nested_path != extracted_path
                    and extracted_nested_path.exists()
                ):
                    extracted_nested_path.rename(extracted_path)
            else:
                logger.info(
                    "Extracted CSV already exists locally, reusing file: {}",
                    extracted_path,
                )

        context.data["extracted_csv_path"] = extracted_path
