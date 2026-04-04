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

from loguru import logger

from gdelt_ingestion.db.connection import get_context_connection
from gdelt_ingestion.pipeline.context import PipelineContext


@dataclass(frozen=True)
class SelectPendingSilverBatchStep:
    name: str = "select_pending_silver_batch"

    def run(self, context: PipelineContext) -> None:
        sql = """
        SELECT
            pb.batch_id,
            pb.source_type,
            pb.file_type,
            pb.source_url,
            pb.file_name,
            pb.gdelt_timestamp,
            pb.status
        FROM pipeline_batches pb
        WHERE pb.file_type = 'export'
          AND pb.status = 'loaded'
          AND EXISTS (
              SELECT 1
              FROM events_bronze eb
              WHERE eb.batch_id = pb.batch_id
          )
          AND NOT EXISTS (
              SELECT 1
              FROM events_silver es
              WHERE es.batch_id = pb.batch_id
          )
        ORDER BY pb.gdelt_timestamp ASC
        LIMIT 1
        """

        conn, owns_connection = get_context_connection(context)

        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                row = cursor.fetchone()
        finally:
            if owns_connection:
                conn.close()

        if row is None:
            logger.info("No pending silver batch found")
            context.data["current_silver_batch"] = None
            return

        batch = {
            "batch_id": row[0],
            "source_type": row[1],
            "file_type": row[2],
            "source_url": row[3],
            "file_name": row[4],
            "gdelt_timestamp": row[5],
            "status": row[6],
        }

        logger.info(
            "Selected pending silver batch: batch_id={}, file_name={}, status={}",
            batch["batch_id"],
            batch["file_name"],
            batch["status"],
        )

        context.data["current_silver_batch"] = batch
