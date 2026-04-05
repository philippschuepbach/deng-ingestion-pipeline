from __future__ import annotations

from dataclasses import dataclass

from deng_ingestion.db.connection import get_connection
from deng_ingestion.pipeline.context import PipelineContext


@dataclass(frozen=True)
class RegisterManifestBatchesForCurrentRunStep:
    name: str = "register_manifest_batches_for_current_run"

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
        RETURNING batch_id, file_type
        """

        registered_export_batch_ids: list[int] = []

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

                    batch_id, file_type = cursor.fetchone()

                    if file_type == "export":
                        registered_export_batch_ids.append(batch_id)

            conn.commit()

        context.data["registered_export_batch_ids"] = list(
            dict.fromkeys(registered_export_batch_ids)
        )
