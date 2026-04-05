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


def build_temp_import_table_sql(table_name: str = "tmp_gdelt_export_import") -> str:
    return f"""
    CREATE TEMP TABLE {table_name} (
        global_event_id BIGINT,
        day INTEGER,
        month_year INTEGER,
        year SMALLINT,
        fraction_date NUMERIC(8,4),

        actor1_code TEXT,
        actor1_name TEXT,
        actor1_country_code TEXT,
        actor1_known_group_code TEXT,
        actor1_ethnic_code TEXT,
        actor1_religion1_code TEXT,
        actor1_religion2_code TEXT,
        actor1_type1_code TEXT,
        actor1_type2_code TEXT,
        actor1_type3_code TEXT,

        actor2_code TEXT,
        actor2_name TEXT,
        actor2_country_code TEXT,
        actor2_known_group_code TEXT,
        actor2_ethnic_code TEXT,
        actor2_religion1_code TEXT,
        actor2_religion2_code TEXT,
        actor2_type1_code TEXT,
        actor2_type2_code TEXT,
        actor2_type3_code TEXT,

        is_root_event SMALLINT,
        event_code TEXT,
        event_base_code TEXT,
        event_root_code TEXT,
        quad_class SMALLINT,
        goldstein_scale NUMERIC(3,1),
        num_mentions INTEGER,
        num_sources INTEGER,
        num_articles INTEGER,
        avg_tone DOUBLE PRECISION,

        actor1_geo_type SMALLINT,
        actor1_geo_fullname TEXT,
        actor1_geo_country_code TEXT,
        actor1_geo_adm1_code TEXT,
        actor1_geo_adm2_code TEXT,
        actor1_geo_lat DOUBLE PRECISION,
        actor1_geo_long DOUBLE PRECISION,
        actor1_geo_feature_id TEXT,

        actor2_geo_type SMALLINT,
        actor2_geo_fullname TEXT,
        actor2_geo_country_code TEXT,
        actor2_geo_adm1_code TEXT,
        actor2_geo_adm2_code TEXT,
        actor2_geo_lat DOUBLE PRECISION,
        actor2_geo_long DOUBLE PRECISION,
        actor2_geo_feature_id TEXT,

        action_geo_type SMALLINT,
        action_geo_fullname TEXT,
        action_geo_country_code TEXT,
        action_geo_adm1_code TEXT,
        action_geo_adm2_code TEXT,
        action_geo_lat DOUBLE PRECISION,
        action_geo_long DOUBLE PRECISION,
        action_geo_feature_id TEXT,

        date_added BIGINT,
        source_url TEXT
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
    select_columns = ", ".join(EVENT_SOURCE_COLUMNS)

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
