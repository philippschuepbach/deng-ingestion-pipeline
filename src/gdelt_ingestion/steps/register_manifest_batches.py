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

from gdelt_ingestion.db.connection import get_connection
from gdelt_ingestion.pipeline.context import PipelineContext


@dataclass(frozen=True)
class RegisterManifestBatchesStep:
    name: str = "register_manifest_batches"

    def run(self, context: PipelineContext) -> None:
        source_type = context.data["manifest_source_type"]
        entries = context.data["filtered_manifest_entries"]

        sql = """
        INSERT INTO pipeline_batches (
            source_type,
            file_type,
            source_url,
            file_name,
            file_size_bytes,
            md5_hash,
            gdelt_timestamp
        )
        VALUES (
            %(source_type)s,
            %(file_type)s,
            %(source_url)s,
            %(file_name)s,
            %(file_size_bytes)s,
            %(md5_hash)s,
            %(gdelt_timestamp)s
        )
        ON CONFLICT (source_url) DO UPDATE
        SET
            file_size_bytes = EXCLUDED.file_size_bytes,
            md5_hash = EXCLUDED.md5_hash,
            gdelt_timestamp = EXCLUDED.gdelt_timestamp
        """

        with get_connection() as conn:
            with conn.cursor() as cursor:
                for entry in entries:
                    cursor.execute(
                        sql,
                        {
                            "source_type": source_type,
                            "file_type": entry.file_type,
                            "source_url": entry.source_url,
                            "file_name": entry.file_name,
                            "file_size_bytes": entry.file_size_bytes,
                            "md5_hash": entry.md5_hash,
                            "gdelt_timestamp": entry.gdelt_timestamp,
                        },
                    )
            conn.commit()
