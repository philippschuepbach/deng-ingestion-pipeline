TRUNCATE TABLE `{{PROJECT_ID}}.{{BIGQUERY_DATASET}}.risk_alerts_gold`;

INSERT INTO `{{PROJECT_ID}}.{{BIGQUERY_DATASET}}.risk_alerts_gold` (
    time_window_start,
    time_window_end,
    country_code,
    country_name,
    total_event_count,
    relevant_event_count,
    protest_event_count,
    conflict_event_count,
    diplomatic_tension_event_count,
    total_mentions,
    total_sources,
    total_articles,
    avg_goldstein_scale,
    avg_tone,
    negative_goldstein_sum,
    baseline_negative_goldstein_mean,
    baseline_negative_goldstein_stddev,
    weighted_instability_score,
    is_alert,
    built_at
)
WITH events_with_window AS (
    SELECT
        TIMESTAMP_TRUNC(es.event_added_ts, HOUR) AS time_window_start,
        es.focus_country_code AS country_code,
        COALESCE(
            fips.country_name,
            CONCAT('UNKNOWN: ', es.focus_country_code)
        ) AS country_name,
        es.num_mentions,
        es.num_sources,
        es.num_articles,
        es.goldstein_scale,
        es.avg_tone,
        es.is_protest_related,
        es.is_conflict_related,
        es.is_diplomatic_tension_related,
        (
            es.is_protest_related
            OR es.is_conflict_related
            OR es.is_diplomatic_tension_related
        ) AS is_risk_relevant
    FROM `{{PROJECT_ID}}.{{BIGQUERY_DATASET}}.events_silver` AS es
    LEFT JOIN `{{PROJECT_ID}}.{{BIGQUERY_DATASET}}.dim_fips_country_codes_external`
        AS fips
      ON es.focus_country_code = fips.country_code
    WHERE es.focus_country_code IS NOT NULL
),
country_hour AS (
    SELECT
        time_window_start,
        TIMESTAMP_ADD(time_window_start, INTERVAL 1 HOUR) AS time_window_end,
        country_code,
        country_name,

        COUNT(*) AS total_event_count,
        COUNTIF(is_risk_relevant) AS relevant_event_count,
        COUNTIF(is_protest_related) AS protest_event_count,
        COUNTIF(is_conflict_related) AS conflict_event_count,
        COUNTIF(is_diplomatic_tension_related)
            AS diplomatic_tension_event_count,

        SUM(COALESCE(num_mentions, 0)) AS total_mentions,
        SUM(COALESCE(num_sources, 0)) AS total_sources,
        SUM(COALESCE(num_articles, 0)) AS total_articles,

        ROUND(AVG(goldstein_scale), 2) AS avg_goldstein_scale,
        AVG(avg_tone) AS avg_tone,

        SUM(
            IF(
                is_risk_relevant,
                GREATEST(-COALESCE(goldstein_scale, 0.0), 0.0),
                0.0
            )
        ) AS negative_goldstein_sum
    FROM events_with_window
    GROUP BY
        time_window_start,
        country_code,
        country_name
),
baselined AS (
    SELECT
        country_hour.*,
        AVG(negative_goldstein_sum) OVER (
            PARTITION BY country_code
            ORDER BY time_window_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS baseline_negative_goldstein_mean,
        STDDEV_SAMP(negative_goldstein_sum) OVER (
            PARTITION BY country_code
            ORDER BY time_window_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS baseline_negative_goldstein_stddev
    FROM country_hour
),
scored AS (
    SELECT
        *,
        CASE
            WHEN baseline_negative_goldstein_mean IS NULL THEN 0.0
            ELSE ROUND(
                SAFE_DIVIDE(
                    negative_goldstein_sum - baseline_negative_goldstein_mean,
                    GREATEST(
                        COALESCE(baseline_negative_goldstein_stddev, 0.0),
                        1.0
                    )
                ),
                2
            )
        END AS weighted_instability_score
    FROM baselined
)
SELECT
    time_window_start,
    time_window_end,
    country_code,
    country_name,
    total_event_count,
    relevant_event_count,
    protest_event_count,
    conflict_event_count,
    diplomatic_tension_event_count,
    total_mentions,
    total_sources,
    total_articles,
    avg_goldstein_scale,
    avg_tone,
    negative_goldstein_sum,
    baseline_negative_goldstein_mean,
    baseline_negative_goldstein_stddev,
    weighted_instability_score,
    (
        relevant_event_count >= 3
        AND weighted_instability_score >= 2.0
    ) AS is_alert,
    CURRENT_TIMESTAMP() AS built_at
FROM scored;
