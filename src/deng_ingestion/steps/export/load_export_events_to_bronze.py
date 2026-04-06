from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.db.connection import get_context_connection
from deng_ingestion.db.pipeline_batches import (
    mark_batch_failed,
    mark_batch_loaded,
)
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    get_current_batch,
    get_extracted_csv_path,
)
from deng_ingestion.steps.export.export_bronze_sql import (
    build_copy_sql,
    build_insert_from_temp_sql,
    build_invalid_numeric_summary_sql,
    build_temp_import_table_sql,
)


@dataclass(frozen=True)
class LoadExportEventsToBronzeStep:
    name: str = "load_export_events_to_bronze"

    def run(self, context: PipelineContext) -> None:
        batch = get_current_batch(context)
        if batch is None:
            logger.debug("Skipping bronze load because no batch is selected")
            return

        csv_path = get_extracted_csv_path(context)
        if csv_path is None:
            raise ValueError("Expected extracted CSV path in pipeline context")

        temp_table_name = "tmp_gdelt_export_import"

        logger.info(
            "Loading export CSV into events_bronze: batch_id={}, csv_path={}",
            batch["batch_id"],
            csv_path,
        )

        create_temp_sql = build_temp_import_table_sql(temp_table_name)
        copy_sql = build_copy_sql(temp_table_name)
        invalid_numeric_summary_sql = build_invalid_numeric_summary_sql(temp_table_name)
        insert_sql = build_insert_from_temp_sql(temp_table_name)

        conn, owns_connection = get_context_connection(context)

        try:
            with conn.cursor() as cursor:
                cursor.execute(create_temp_sql)

                with csv_path.open("r", encoding="utf-8", errors="replace") as handle:
                    cursor.copy_expert(copy_sql, handle)

                cursor.execute(invalid_numeric_summary_sql)
                invalid_numeric_rows = cursor.fetchall()

                if invalid_numeric_rows:
                    for (
                        column_name,
                        invalid_count,
                        sample_value,
                    ) in invalid_numeric_rows:
                        logger.warning(
                            (
                                "Found invalid numeric value in column '{}': "
                                "batch_id={}, column_name={}, "
                                "invalid_count={}, sample_value={}"
                            ),
                            batch["batch_id"],
                            column_name,
                            invalid_count,
                            sample_value,
                        )

                cursor.execute(insert_sql, {"batch_id": batch["batch_id"]})
                inserted_rows = cursor.rowcount

            mark_batch_loaded(conn, batch["batch_id"])
            conn.commit()

        except Exception as exc:
            conn.rollback()

            try:
                mark_batch_failed(conn, batch["batch_id"], exc)
                conn.commit()
            except Exception:
                conn.rollback()
                logger.exception(
                    "Failed to persist failed batch state: batch_id={}",
                    batch["batch_id"],
                )

            raise

        finally:
            if owns_connection:
                conn.close()

        logger.info(
            "Finished bronze load for batch_id={}, inserted_rows={}",
            batch["batch_id"],
            inserted_rows,
        )
