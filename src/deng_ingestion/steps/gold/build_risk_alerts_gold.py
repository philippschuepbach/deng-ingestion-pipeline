from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.db.connection import get_context_connection
from deng_ingestion.db.sql_loader import load_sql
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import set_gold_row_count


@dataclass(frozen=True)
class BuildRiskAlertsGoldStep:
    name: str = "build_risk_alerts_gold"

    def run(self, context: PipelineContext) -> None:
        logger.debug("Building risk_alerts_gold via full refresh")

        truncate_sql = "TRUNCATE TABLE risk_alerts_gold RESTART IDENTITY"
        insert_sql = load_sql("gold", "build_risk_alerts_gold.sql")

        count_sql = "SELECT COUNT(*) FROM risk_alerts_gold"

        conn, owns_connection = get_context_connection(context)

        try:
            with conn.cursor() as cursor:
                cursor.execute(truncate_sql)
                cursor.execute(insert_sql)
                cursor.execute(count_sql)
                count_row = cursor.fetchone()
                if count_row is None:
                    raise ValueError(
                        "Expected row count result from risk_alerts_gold count query"
                    )

                gold_row_count: int = count_row[0]

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            if owns_connection:
                conn.close()

        set_gold_row_count(context, gold_row_count)

        logger.debug(
            "Finished building risk_alerts_gold: row_count={}",
            gold_row_count,
        )
