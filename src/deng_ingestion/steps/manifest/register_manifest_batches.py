from __future__ import annotations

from dataclasses import dataclass

from deng_ingestion.db.connection import get_connection
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    get_filtered_manifest_entries,
    get_manifest_source_type,
)


@dataclass(frozen=True)
class RegisterManifestBatchesStep:
    name: str = "register_manifest_batches"

    def run(self, context: PipelineContext) -> None:
        source_type = get_manifest_source_type(context)
        if source_type is None:
            raise ValueError("Expected manifest source type in pipeline context")

        entries = get_filtered_manifest_entries(context)
        if entries is None:
            raise ValueError("Expected filtered manifest entries in pipeline context")

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
