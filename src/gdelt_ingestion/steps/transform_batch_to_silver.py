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
class TransformBatchToSilverStep:
    name: str = "transform_batch_to_silver"

    def run(self, context: PipelineContext) -> None:
        batch = context.data.get("current_silver_batch")
        if batch is None:
            logger.debug("Skipping silver transformation because no batch is selected")
            return

        logger.info(
            "Transforming bronze batch to silver: batch_id={}, file_name={}",
            batch["batch_id"],
            batch["file_name"],
        )

        sql = """
        INSERT INTO events_silver (
            batch_id,
            global_event_id,
            event_date,
            event_added_ts,
            event_code,
            event_root_code,
            quad_class,
            goldstein_scale,
            actor1_name,
            actor1_country_code,
            actor1_known_group_code,
            actor2_name,
            actor2_country_code,
            actor2_known_group_code,
            focus_country_code,
            focus_location_name,
            focus_geo_type,
            focus_geo_lat,
            focus_geo_long,
            num_mentions,
            num_sources,
            num_articles,
            avg_tone,
            source_url,
            is_protest_related,
            is_conflict_related,
            is_diplomatic_tension_related
        )
        SELECT
            b.batch_id,
            b.global_event_id,
            TO_DATE(b.day::text, 'YYYYMMDD') AS event_date,
            TO_TIMESTAMP(b.date_added::text, 'YYYYMMDDHH24MISS') AS event_added_ts,
            c.event_code,
            c.event_root_code,
            b.quad_class,
            c.goldstein_scale,
            b.actor1_name,
            b.actor1_country_code,
            b.actor1_known_group_code,
            b.actor2_name,
            b.actor2_country_code,
            b.actor2_known_group_code,
            b.action_geo_country_code AS focus_country_code,
            b.action_geo_fullname AS focus_location_name,
            b.action_geo_type AS focus_geo_type,
            b.action_geo_lat AS focus_geo_lat,
            b.action_geo_long AS focus_geo_long,
            b.num_mentions,
            b.num_sources,
            b.num_articles,
            b.avg_tone,
            b.source_url,
            COALESCE(m.is_protest_related, FALSE) AS is_protest_related,
            COALESCE(m.is_conflict_related, FALSE) AS is_conflict_related,
            COALESCE(m.is_diplomatic_tension_related, FALSE) AS is_diplomatic_tension_related
        FROM events_bronze b
        JOIN dim_cameo_event_codes c
          ON b.event_code = c.event_code
        LEFT JOIN dim_risk_category_mapping m
          ON c.event_root_code = m.event_root_code
        WHERE b.batch_id = %(batch_id)s
          AND b.day IS NOT NULL
          AND b.date_added IS NOT NULL
        ON CONFLICT (global_event_id) DO NOTHING
        """

        conn, owns_connection = get_context_connection(context)

        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, {"batch_id": batch["batch_id"]})
                inserted_rows = cursor.rowcount
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            if owns_connection:
                conn.close()

        context.data["last_silver_inserted_rows"] = inserted_rows

        logger.info(
            "Finished silver transformation for batch_id={}, inserted_rows={}",
            batch["batch_id"],
            inserted_rows,
        )
