CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.{{BIGQUERY_DATASET}}.events_silver` (
    global_event_id INT64 NOT NULL,
    event_date DATE NOT NULL,
    event_added_ts TIMESTAMP NOT NULL,

    event_code STRING,
    event_base_code STRING,
    event_root_code STRING,
    quad_class INT64,
    goldstein_scale FLOAT64,

    actor1_name STRING,
    actor1_country_code STRING,
    actor1_known_group_code STRING,
    actor2_name STRING,
    actor2_country_code STRING,
    actor2_known_group_code STRING,

    focus_country_code STRING,
    focus_location_name STRING,
    focus_geo_type INT64,
    focus_geo_lat FLOAT64,
    focus_geo_long FLOAT64,

    num_mentions INT64,
    num_sources INT64,
    num_articles INT64,
    avg_tone FLOAT64,
    source_url STRING,

    is_protest_related BOOL NOT NULL,
    is_conflict_related BOOL NOT NULL,
    is_diplomatic_tension_related BOOL NOT NULL,

    loaded_at TIMESTAMP NOT NULL
)
PARTITION BY event_date
CLUSTER BY focus_country_code, event_root_code;
