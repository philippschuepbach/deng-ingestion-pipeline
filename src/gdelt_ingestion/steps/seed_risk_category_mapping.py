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

RISK_CATEGORY_MAPPING_ROWS: list[dict[str, str | bool]] = [
    {
        "event_root_code": "11",
        "is_protest_related": False,
        "is_conflict_related": False,
        "is_diplomatic_tension_related": True,
    },
    {
        "event_root_code": "12",
        "is_protest_related": False,
        "is_conflict_related": False,
        "is_diplomatic_tension_related": True,
    },
    {
        "event_root_code": "13",
        "is_protest_related": False,
        "is_conflict_related": False,
        "is_diplomatic_tension_related": True,
    },
    {
        "event_root_code": "14",
        "is_protest_related": True,
        "is_conflict_related": False,
        "is_diplomatic_tension_related": False,
    },
    {
        "event_root_code": "16",
        "is_protest_related": False,
        "is_conflict_related": False,
        "is_diplomatic_tension_related": True,
    },
    {
        "event_root_code": "18",
        "is_protest_related": False,
        "is_conflict_related": True,
        "is_diplomatic_tension_related": False,
    },
    {
        "event_root_code": "19",
        "is_protest_related": False,
        "is_conflict_related": True,
        "is_diplomatic_tension_related": False,
    },
    {
        "event_root_code": "20",
        "is_protest_related": False,
        "is_conflict_related": True,
        "is_diplomatic_tension_related": False,
    },
]


@dataclass(frozen=True)
class SeedRiskCategoryMappingStep:
    name: str = "seed_risk_category_mapping"

    def run(self, context: PipelineContext) -> None:
        logger.info("Seeding dim_risk_category_mapping")

        conn, owns_connection = get_context_connection(context)

        try:
            with conn.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO dim_risk_category_mapping (
                        event_root_code,
                        is_protest_related,
                        is_conflict_related,
                        is_diplomatic_tension_related
                    )
                    VALUES (
                        %(event_root_code)s,
                        %(is_protest_related)s,
                        %(is_conflict_related)s,
                        %(is_diplomatic_tension_related)s
                    )
                    ON CONFLICT (event_root_code) DO UPDATE
                    SET
                        is_protest_related = EXCLUDED.is_protest_related,
                        is_conflict_related = EXCLUDED.is_conflict_related,
                        is_diplomatic_tension_related = EXCLUDED.is_diplomatic_tension_related
                    """,
                    RISK_CATEGORY_MAPPING_ROWS,
                )

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            if owns_connection:
                conn.close()

        context.data["seeded_risk_category_mapping_count"] = len(
            RISK_CATEGORY_MAPPING_ROWS
        )

        logger.info(
            "Seeded dim_risk_category_mapping: row_count={}",
            len(RISK_CATEGORY_MAPPING_ROWS),
        )
