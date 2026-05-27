MERGE `{{PROJECT_ID}}.{{BIGQUERY_DATASET}}.events_silver` AS target
USING (
    WITH parsed AS (
        SELECT
            SAFE_CAST(global_event_id AS INT64) AS global_event_id,
            SAFE.PARSE_DATE('%Y%m%d', sql_date) AS event_date,
            SAFE.PARSE_TIMESTAMP('%Y%m%d%H%M%S', date_added) AS event_added_ts,

            NULLIF(event_code, '') AS event_code,
            NULLIF(event_base_code, '') AS event_base_code,
            NULLIF(event_root_code, '') AS event_root_code,
            SAFE_CAST(quad_class AS INT64) AS quad_class,
            SAFE_CAST(goldstein_scale AS FLOAT64) AS goldstein_scale,

            NULLIF(actor1_name, '') AS actor1_name,
            NULLIF(actor1_country_code, '') AS actor1_country_code,
            NULLIF(actor1_known_group_code, '') AS actor1_known_group_code,
            NULLIF(actor2_name, '') AS actor2_name,
            NULLIF(actor2_country_code, '') AS actor2_country_code,
            NULLIF(actor2_known_group_code, '') AS actor2_known_group_code,

            COALESCE(
                NULLIF(action_geo_country_code, ''),
                NULLIF(actor1_geo_country_code, ''),
                NULLIF(actor2_geo_country_code, ''),
                NULLIF(actor1_country_code, ''),
                NULLIF(actor2_country_code, '')
            ) AS focus_country_code,

            COALESCE(
                NULLIF(action_geo_fullname, ''),
                NULLIF(actor1_geo_fullname, ''),
                NULLIF(actor2_geo_fullname, '')
            ) AS focus_location_name,

            COALESCE(
                SAFE_CAST(action_geo_type AS INT64),
                SAFE_CAST(actor1_geo_type AS INT64),
                SAFE_CAST(actor2_geo_type AS INT64)
            ) AS focus_geo_type,

            COALESCE(
                SAFE_CAST(action_geo_lat AS FLOAT64),
                SAFE_CAST(actor1_geo_lat AS FLOAT64),
                SAFE_CAST(actor2_geo_lat AS FLOAT64)
            ) AS focus_geo_lat,

            COALESCE(
                SAFE_CAST(action_geo_long AS FLOAT64),
                SAFE_CAST(actor1_geo_long AS FLOAT64),
                SAFE_CAST(actor2_geo_long AS FLOAT64)
            ) AS focus_geo_long,

            SAFE_CAST(num_mentions AS INT64) AS num_mentions,
            SAFE_CAST(num_sources AS INT64) AS num_sources,
            SAFE_CAST(num_articles AS INT64) AS num_articles,
            SAFE_CAST(avg_tone AS FLOAT64) AS avg_tone,
            NULLIF(source_url, '') AS source_url,

            event_root_code IN ('14') AS is_protest_related,
            event_root_code IN ('18', '19', '20') AS is_conflict_related,
            event_root_code IN ('11', '12', '13', '16', '17')
                AS is_diplomatic_tension_related
        FROM `{{PROJECT_ID}}.{{BIGQUERY_DATASET}}.events_bronze_external`
    ),
    deduplicated AS (
        SELECT * EXCEPT(row_number)
        FROM (
            SELECT
                parsed.*,
                ROW_NUMBER() OVER (
                    PARTITION BY global_event_id
                    ORDER BY event_added_ts DESC
                ) AS row_number
            FROM parsed
            WHERE global_event_id IS NOT NULL
              AND event_date IS NOT NULL
              AND event_added_ts IS NOT NULL
        )
        WHERE row_number = 1
    )
    SELECT
        *,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM deduplicated
) AS source
ON target.global_event_id = source.global_event_id

WHEN NOT MATCHED THEN
INSERT (
    global_event_id,
    event_date,
    event_added_ts,
    event_code,
    event_base_code,
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
    is_diplomatic_tension_related,
    loaded_at
)
VALUES (
    source.global_event_id,
    source.event_date,
    source.event_added_ts,
    source.event_code,
    source.event_base_code,
    source.event_root_code,
    source.quad_class,
    source.goldstein_scale,
    source.actor1_name,
    source.actor1_country_code,
    source.actor1_known_group_code,
    source.actor2_name,
    source.actor2_country_code,
    source.actor2_known_group_code,
    source.focus_country_code,
    source.focus_location_name,
    source.focus_geo_type,
    source.focus_geo_lat,
    source.focus_geo_long,
    source.num_mentions,
    source.num_sources,
    source.num_articles,
    source.avg_tone,
    source.source_url,
    source.is_protest_related,
    source.is_conflict_related,
    source.is_diplomatic_tension_related,
    source.loaded_at
);
