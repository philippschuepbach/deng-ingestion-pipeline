from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.db.connection import get_context_connection
from deng_ingestion.db.sql_loader import load_sql
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    set_seeded_risk_category_mapping_count,
)

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
        logger.debug("Seeding dim_risk_category_mapping")

        conn, owns_connection = get_context_connection(context)

        try:
            with conn.cursor() as cursor:
                insert_sql = load_sql("lookup", "seed_risk_category_mapping.sql")
                cursor.executemany(
                    insert_sql,
                    RISK_CATEGORY_MAPPING_ROWS,
                )

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            if owns_connection:
                conn.close()

        set_seeded_risk_category_mapping_count(
            context,
            len(RISK_CATEGORY_MAPPING_ROWS),
        )

        logger.debug(
            "Seeded dim_risk_category_mapping: row_count={}",
            len(RISK_CATEGORY_MAPPING_ROWS),
        )
