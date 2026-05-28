CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.{{BIGQUERY_DATASET}}.risk_alerts_gold` (
    time_window_start TIMESTAMP NOT NULL,
    time_window_end TIMESTAMP NOT NULL,

    country_code STRING NOT NULL,
    country_name STRING NOT NULL,

    total_event_count INT64 NOT NULL,
    relevant_event_count INT64 NOT NULL,
    protest_event_count INT64 NOT NULL,
    conflict_event_count INT64 NOT NULL,
    diplomatic_tension_event_count INT64 NOT NULL,

    total_mentions INT64 NOT NULL,
    total_sources INT64 NOT NULL,
    total_articles INT64 NOT NULL,

    avg_goldstein_scale FLOAT64,
    avg_tone FLOAT64,

    negative_goldstein_sum FLOAT64 NOT NULL,
    baseline_negative_goldstein_mean FLOAT64,
    baseline_negative_goldstein_stddev FLOAT64,
    weighted_instability_score FLOAT64 NOT NULL,
    is_alert BOOL NOT NULL,

    built_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(time_window_start)
CLUSTER BY country_code, is_alert;
