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

from loguru import logger

from gdelt_ingestion.bronze.export_columns import (
    build_copy_sql,
    build_insert_from_temp_sql,
    build_temp_import_table_sql,
)
from gdelt_ingestion.db.connection import get_context_connection
from gdelt_ingestion.pipeline.context import PipelineContext


@dataclass(frozen=True)
class LoadExportEventsToBronzeStep:
    name: str = "load_export_events_to_bronze"

    def run(self, context: PipelineContext) -> None:
        batch = context.data.get("current_batch")
        if batch is None:
            logger.debug("Skipping bronze load because no batch is selected")
            return

        csv_path: Path = context.data["extracted_csv_path"]
        temp_table_name = "tmp_gdelt_export_import"

        logger.info(
            "Loading export CSV into events_bronze: batch_id={}, csv_path={}",
            batch["batch_id"],
            csv_path,
        )

        create_temp_sql = build_temp_import_table_sql(temp_table_name)
        copy_sql = build_copy_sql(temp_table_name)
        insert_sql = build_insert_from_temp_sql(temp_table_name)

        update_sql = """
        UPDATE pipeline_batches
        SET
            status = 'loaded',
            loaded_at = NOW()
        WHERE batch_id = %(batch_id)s
        """

        conn, owns_connection = get_context_connection(context)

        try:
            with conn.cursor() as cursor:
                cursor.execute(create_temp_sql)

                with csv_path.open("r", encoding="utf-8", errors="replace") as handle:
                    cursor.copy_expert(copy_sql, handle)

                cursor.execute(insert_sql, {"batch_id": batch["batch_id"]})
                inserted_rows = cursor.rowcount

                cursor.execute(update_sql, {"batch_id": batch["batch_id"]})

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            if owns_connection:
                conn.close()

        logger.info(
            "Finished bronze load for batch_id={}, inserted_rows={}",
            batch["batch_id"],
            inserted_rows,
        )
