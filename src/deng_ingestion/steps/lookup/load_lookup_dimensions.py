from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from loguru import logger
from psycopg2.extensions import cursor as PgCursor

from deng_ingestion.db.connection import get_context_connection
from deng_ingestion.lookups.loader import (
    load_cameo_country_codes,
    load_cameo_event_roots_and_codes,
    load_cameo_known_groups,
    load_fips_country_codes,
)
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    get_lookup_dir,
    set_loaded_lookup_counts,
)
from deng_ingestion.pipeline.context_types import LookupLoadCounts

type FipsCountryRows = list[dict[str, str]]
type CameoCountryRows = list[dict[str, str]]
type CameoKnownGroupRows = list[dict[str, str | bool]]
type CameoEventRootRows = list[dict[str, str]]
type CameoEventCodeRows = list[dict[str, str | float]]


@dataclass(frozen=True)
class LoadLookupDimensionsStep:
    name: str = "load_lookup_dimensions"

    def run(self, context: PipelineContext) -> None:
        lookup_dir = _resolve_lookup_dir(context)

        logger.debug("Loading lookup dimensions from {}", lookup_dir)

        (
            fips_country_codes,
            cameo_country_codes,
            cameo_known_groups,
            cameo_event_roots,
            cameo_event_codes,
        ) = _load_lookup_source_rows(lookup_dir)

        conn, owns_connection = get_context_connection(context)

        try:
            with conn.cursor() as cursor:
                _upsert_fips_country_codes(cursor, fips_country_codes)
                _upsert_cameo_country_codes(cursor, cameo_country_codes)
                _upsert_cameo_known_groups(cursor, cameo_known_groups)
                _upsert_cameo_event_roots(cursor, cameo_event_roots)
                _upsert_cameo_event_codes(cursor, cameo_event_codes)

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            if owns_connection:
                conn.close()

        loaded_lookup_counts = _build_loaded_lookup_counts(
            fips_country_codes,
            cameo_country_codes,
            cameo_known_groups,
            cameo_event_roots,
            cameo_event_codes,
        )

        set_loaded_lookup_counts(context, loaded_lookup_counts)

        logger.debug(
            (
                "Loaded lookup dimensions: "
                "fips_countries={}, cameo_countries={}, "
                "known_groups={}, event_roots={}, event_codes={}"
            ),
            loaded_lookup_counts["dim_fips_country_codes"],
            loaded_lookup_counts["dim_cameo_country_codes"],
            loaded_lookup_counts["dim_cameo_known_groups"],
            loaded_lookup_counts["dim_cameo_event_roots"],
            loaded_lookup_counts["dim_cameo_event_codes"],
        )


def _build_loaded_lookup_counts(
    fips_country_codes: FipsCountryRows,
    cameo_country_codes: CameoCountryRows,
    cameo_known_groups: CameoKnownGroupRows,
    cameo_event_roots: CameoEventRootRows,
    cameo_event_codes: CameoEventCodeRows,
) -> LookupLoadCounts:
    return {
        "dim_fips_country_codes": len(fips_country_codes),
        "dim_cameo_country_codes": len(cameo_country_codes),
        "dim_cameo_known_groups": len(cameo_known_groups),
        "dim_cameo_event_roots": len(cameo_event_roots),
        "dim_cameo_event_codes": len(cameo_event_codes),
    }


def _resolve_lookup_dir(context: PipelineContext) -> Path:
    lookup_dir = get_lookup_dir(context) or (context.working_dir / "data" / "lookups")

    if not lookup_dir.exists():
        raise FileNotFoundError(f"Lookup directory does not exist: {lookup_dir}")

    return lookup_dir


def _load_lookup_source_rows(
    lookup_dir: Path,
) -> tuple[
    FipsCountryRows,
    CameoCountryRows,
    CameoKnownGroupRows,
    CameoEventRootRows,
    CameoEventCodeRows,
]:
    fips_country_codes = load_fips_country_codes(lookup_dir)
    cameo_country_codes = load_cameo_country_codes(lookup_dir)
    cameo_known_groups = load_cameo_known_groups(lookup_dir)
    cameo_event_roots, cameo_event_codes = load_cameo_event_roots_and_codes(lookup_dir)

    return (
        fips_country_codes,
        cameo_country_codes,
        cameo_known_groups,
        cameo_event_roots,
        cameo_event_codes,
    )


def _upsert_fips_country_codes(
    cursor: PgCursor,
    rows: FipsCountryRows,
) -> None:
    cursor.executemany(
        """
        INSERT INTO dim_fips_country_codes (
            country_code,
            country_name
        )
        VALUES (
            %(country_code)s,
            %(country_name)s
        )
        ON CONFLICT (country_code) DO UPDATE
        SET country_name = EXCLUDED.country_name
        """,
        rows,
    )


def _upsert_cameo_country_codes(
    cursor: PgCursor,
    rows: CameoCountryRows,
) -> None:
    cursor.executemany(
        """
        INSERT INTO dim_cameo_country_codes (
            country_code,
            country_name
        )
        VALUES (
            %(country_code)s,
            %(country_name)s
        )
        ON CONFLICT (country_code) DO UPDATE
        SET country_name = EXCLUDED.country_name
        """,
        rows,
    )


def _upsert_cameo_known_groups(
    cursor: PgCursor,
    rows: CameoKnownGroupRows,
) -> None:
    cursor.executemany(
        """
        INSERT INTO dim_cameo_known_groups (
            known_group_code,
            known_group_name,
            is_ambiguous
        )
        VALUES (
            %(known_group_code)s,
            %(known_group_name)s,
            %(is_ambiguous)s
        )
        ON CONFLICT (known_group_code) DO UPDATE
        SET
            known_group_name = EXCLUDED.known_group_name,
            is_ambiguous = EXCLUDED.is_ambiguous
        """,
        rows,
    )


def _upsert_cameo_event_roots(
    cursor: PgCursor,
    rows: CameoEventRootRows,
) -> None:
    cursor.executemany(
        """
        INSERT INTO dim_cameo_event_roots (
            event_root_code,
            event_root_description
        )
        VALUES (
            %(event_root_code)s,
            %(event_root_description)s
        )
        ON CONFLICT (event_root_code) DO UPDATE
        SET event_root_description = EXCLUDED.event_root_description
        """,
        rows,
    )


def _upsert_cameo_event_codes(
    cursor: PgCursor,
    rows: CameoEventCodeRows,
) -> None:
    cursor.executemany(
        """
        INSERT INTO dim_cameo_event_codes (
            event_code,
            event_base_code,
            event_root_code,
            event_description,
            goldstein_scale
        )
        VALUES (
            %(event_code)s,
            %(event_base_code)s,
            %(event_root_code)s,
            %(event_description)s,
            %(goldstein_scale)s
        )
        ON CONFLICT (event_code) DO UPDATE
        SET
            event_base_code = EXCLUDED.event_base_code,
            event_root_code = EXCLUDED.event_root_code,
            event_description = EXCLUDED.event_description,
            goldstein_scale = EXCLUDED.goldstein_scale
        """,
        rows,
    )
