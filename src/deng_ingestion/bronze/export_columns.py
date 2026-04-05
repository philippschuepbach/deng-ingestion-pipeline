from __future__ import annotations

EVENT_SOURCE_COLUMNS: list[str] = [
    "global_event_id",
    "day",
    "month_year",
    "year",
    "fraction_date",
    "actor1_code",
    "actor1_name",
    "actor1_country_code",
    "actor1_known_group_code",
    "actor1_ethnic_code",
    "actor1_religion1_code",
    "actor1_religion2_code",
    "actor1_type1_code",
    "actor1_type2_code",
    "actor1_type3_code",
    "actor2_code",
    "actor2_name",
    "actor2_country_code",
    "actor2_known_group_code",
    "actor2_ethnic_code",
    "actor2_religion1_code",
    "actor2_religion2_code",
    "actor2_type1_code",
    "actor2_type2_code",
    "actor2_type3_code",
    "is_root_event",
    "event_code",
    "event_base_code",
    "event_root_code",
    "quad_class",
    "goldstein_scale",
    "num_mentions",
    "num_sources",
    "num_articles",
    "avg_tone",
    "actor1_geo_type",
    "actor1_geo_fullname",
    "actor1_geo_country_code",
    "actor1_geo_adm1_code",
    "actor1_geo_adm2_code",
    "actor1_geo_lat",
    "actor1_geo_long",
    "actor1_geo_feature_id",
    "actor2_geo_type",
    "actor2_geo_fullname",
    "actor2_geo_country_code",
    "actor2_geo_adm1_code",
    "actor2_geo_adm2_code",
    "actor2_geo_lat",
    "actor2_geo_long",
    "actor2_geo_feature_id",
    "action_geo_type",
    "action_geo_fullname",
    "action_geo_country_code",
    "action_geo_adm1_code",
    "action_geo_adm2_code",
    "action_geo_lat",
    "action_geo_long",
    "action_geo_feature_id",
    "date_added",
    "source_url",
]

INTEGER_COLUMNS: set[str] = {
    "global_event_id",
    "day",
    "month_year",
    "year",
    "is_root_event",
    "quad_class",
    "num_mentions",
    "num_sources",
    "num_articles",
    "actor1_geo_type",
    "actor2_geo_type",
    "action_geo_type",
    "date_added",
}

DECIMAL_COLUMNS: set[str] = {
    "fraction_date",
    "goldstein_scale",
    "avg_tone",
    "actor1_geo_lat",
    "actor1_geo_long",
    "actor2_geo_lat",
    "actor2_geo_long",
    "action_geo_lat",
    "action_geo_long",
}


def _safe_int_expr(column_name: str) -> str:
    return f"""
    CASE
        WHEN NULLIF(BTRIM({column_name}), '') IS NULL THEN NULL
        WHEN BTRIM({column_name}) ~ '^-?\\d+$' THEN BTRIM({column_name})::BIGINT
        ELSE NULL
    END
    """.strip()


def _safe_decimal_expr(column_name: str) -> str:
    return f"""
    CASE
        WHEN NULLIF(BTRIM({column_name}), '') IS NULL THEN NULL
        WHEN BTRIM({column_name}) ~ '^-?(\\d+(\\.\\d+)?|\\.\\d+)$' THEN BTRIM({column_name})::DOUBLE PRECISION
        ELSE NULL
    END
    """.strip()


def _select_expression(column_name: str) -> str:
    if column_name in INTEGER_COLUMNS:
        return f"{_safe_int_expr(column_name)} AS {column_name}"

    if column_name in DECIMAL_COLUMNS:
        return f"{_safe_decimal_expr(column_name)} AS {column_name}"

    return f"NULLIF({column_name}, '') AS {column_name}"


def build_temp_import_table_sql(table_name: str = "tmp_gdelt_export_import") -> str:
    column_definitions = ",\n        ".join(
        f"{column_name} TEXT" for column_name in EVENT_SOURCE_COLUMNS
    )

    return f"""
    CREATE TEMP TABLE {table_name} (
        {column_definitions}
    ) ON COMMIT DROP
    """


def build_copy_sql(table_name: str = "tmp_gdelt_export_import") -> str:
    column_list = ", ".join(EVENT_SOURCE_COLUMNS)
    return f"""
    COPY {table_name} ({column_list})
    FROM STDIN
    WITH (
        FORMAT CSV,
        DELIMITER E'\\t',
        NULL '',
        HEADER FALSE
    )
    """


def build_insert_from_temp_sql(table_name: str = "tmp_gdelt_export_import") -> str:
    source_columns = ", ".join(EVENT_SOURCE_COLUMNS)
    select_columns = ",\n        ".join(
        _select_expression(column_name) for column_name in EVENT_SOURCE_COLUMNS
    )

    return f"""
    INSERT INTO events_bronze (
        batch_id,
        {source_columns}
    )
    SELECT
        %(batch_id)s,
        {select_columns}
    FROM {table_name}
    ON CONFLICT (batch_id, global_event_id) DO NOTHING
    """


def build_invalid_numeric_summary_sql(
    table_name: str = "tmp_gdelt_export_import",
) -> str:
    parts: list[str] = []

    for column_name in sorted(INTEGER_COLUMNS):
        parts.append(f"""
            SELECT
                '{column_name}' AS column_name,
                COUNT(*)::BIGINT AS invalid_count,
                MIN(BTRIM({column_name})) AS sample_value
            FROM {table_name}
            WHERE NULLIF(BTRIM({column_name}), '') IS NOT NULL
              AND NOT (BTRIM({column_name}) ~ '^-?\\d+$')
            """.strip())

    for column_name in sorted(DECIMAL_COLUMNS):
        parts.append(f"""
            SELECT
                '{column_name}' AS column_name,
                COUNT(*)::BIGINT AS invalid_count,
                MIN(BTRIM({column_name})) AS sample_value
            FROM {table_name}
            WHERE NULLIF(BTRIM({column_name}), '') IS NOT NULL
              AND NOT (BTRIM({column_name}) ~ '^-?(\\d+(\\.\\d+)?|\\.\\d+)$')
            """.strip())

    union_sql = "\nUNION ALL\n".join(parts)

    return f"""
    SELECT
        column_name,
        invalid_count,
        sample_value
    FROM (
        {union_sql}
    ) invalid_values
    WHERE invalid_count > 0
    ORDER BY invalid_count DESC, column_name ASC
    """
