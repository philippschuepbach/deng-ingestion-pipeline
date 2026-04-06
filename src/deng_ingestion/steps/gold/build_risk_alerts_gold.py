from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.db.connection import get_context_connection
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import set_gold_row_count


@dataclass(frozen=True)
class BuildRiskAlertsGoldStep:
    name: str = "build_risk_alerts_gold"

    def run(self, context: PipelineContext) -> None:
        logger.info("Building risk_alerts_gold via full refresh")

        truncate_sql = "TRUNCATE TABLE risk_alerts_gold RESTART IDENTITY"

        insert_sql = """
        INSERT INTO risk_alerts_gold (
            time_window_start,
            time_window_end,
            country_code,
            country_name,
            total_event_count,
            protest_event_count,
            conflict_event_count,
            diplomatic_tension_event_count,
            total_mentions,
            total_sources,
            total_articles,
            avg_goldstein_scale,
            avg_tone,
            weighted_instability_score,
            is_alert
        )
        SELECT
            DATE_TRUNC('hour', es.event_added_ts) AS time_window_start,
            DATE_TRUNC('hour', es.event_added_ts) + INTERVAL '1 hour' AS time_window_end,
            es.focus_country_code AS country_code,
            COALESCE(fcc.country_name, 'UNKNOWN: ' || es.focus_country_code) AS country_name,

            COUNT(*)::INTEGER AS total_event_count,
            SUM(CASE WHEN es.is_protest_related THEN 1 ELSE 0 END)::INTEGER AS protest_event_count,
            SUM(CASE WHEN es.is_conflict_related THEN 1 ELSE 0 END)::INTEGER AS conflict_event_count,
            SUM(CASE WHEN es.is_diplomatic_tension_related THEN 1 ELSE 0 END)::INTEGER AS diplomatic_tension_event_count,

            COALESCE(SUM(es.num_mentions), 0)::INTEGER AS total_mentions,
            COALESCE(SUM(es.num_sources), 0)::INTEGER AS total_sources,
            COALESCE(SUM(es.num_articles), 0)::INTEGER AS total_articles,

            ROUND(AVG(es.goldstein_scale)::numeric, 2) AS avg_goldstein_scale,
            AVG(es.avg_tone) AS avg_tone,

            (
                SUM(CASE WHEN es.is_conflict_related THEN 3.0 ELSE 0.0 END)
                + SUM(CASE WHEN es.is_protest_related THEN 2.0 ELSE 0.0 END)
                + SUM(CASE WHEN es.is_diplomatic_tension_related THEN 1.0 ELSE 0.0 END)
            ) AS weighted_instability_score,

            (
                (
                    SUM(CASE WHEN es.is_conflict_related THEN 3.0 ELSE 0.0 END)
                    + SUM(CASE WHEN es.is_protest_related THEN 2.0 ELSE 0.0 END)
                    + SUM(CASE WHEN es.is_diplomatic_tension_related THEN 1.0 ELSE 0.0 END)
                ) >= 5.0
            ) AS is_alert
        FROM events_silver es
        LEFT JOIN dim_fips_country_codes fcc
          ON es.focus_country_code = fcc.country_code
        WHERE es.focus_country_code IS NOT NULL
        GROUP BY
            DATE_TRUNC('hour', es.event_added_ts),
            es.focus_country_code,
            COALESCE(fcc.country_name, 'UNKNOWN: ' || es.focus_country_code)
        ORDER BY
            time_window_start,
            country_code
        """

        count_sql = "SELECT COUNT(*) FROM risk_alerts_gold"

        conn, owns_connection = get_context_connection(context)

        try:
            with conn.cursor() as cursor:
                cursor.execute(truncate_sql)
                cursor.execute(insert_sql)
                cursor.execute(count_sql)
                gold_row_count = cursor.fetchone()[0]

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            if owns_connection:
                conn.close()

        set_gold_row_count(context, gold_row_count)

        logger.info(
            "Finished building risk_alerts_gold: row_count={}",
            gold_row_count,
        )
