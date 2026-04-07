INSERT INTO risk_alerts_gold (
    time_window_start,
    time_window_end,
    country_code,
    country_name,
    total_event_count,
    protest_event_count,
    conflict_event_count,
    diplomatic_tension_event_count,
    total_mentions,
    total_sources,
    total_articles,
    avg_goldstein_scale,
    avg_tone,
    weighted_instability_score,
    is_alert
)
SELECT
    DATE_TRUNC('hour', es.event_added_ts) AS time_window_start,
    DATE_TRUNC('hour', es.event_added_ts) + INTERVAL '1 hour' AS time_window_end,
    es.focus_country_code AS country_code,
    COALESCE(fcc.country_name, 'UNKNOWN: ' || es.focus_country_code) AS country_name,

    COUNT(*)::INTEGER AS total_event_count,
    SUM(CASE WHEN es.is_protest_related THEN 1 ELSE 0 END)::INTEGER AS protest_event_count,
    SUM(CASE WHEN es.is_conflict_related THEN 1 ELSE 0 END)::INTEGER AS conflict_event_count,
    SUM(CASE WHEN es.is_diplomatic_tension_related THEN 1 ELSE 0 END)::INTEGER AS diplomatic_tension_event_count,

    COALESCE(SUM(es.num_mentions), 0)::INTEGER AS total_mentions,
    COALESCE(SUM(es.num_sources), 0)::INTEGER AS total_sources,
    COALESCE(SUM(es.num_articles), 0)::INTEGER AS total_articles,

    ROUND(AVG(es.goldstein_scale)::numeric, 2) AS avg_goldstein_scale,
    AVG(es.avg_tone) AS avg_tone,

    COALESCE(
        ROUND(
            AVG(
                CASE
                    WHEN es.is_conflict_related
                      OR es.is_protest_related
                      OR es.is_diplomatic_tension_related
                    THEN GREATEST(-es.goldstein_scale, 0)
                    ELSE NULL
                END
            )::numeric,
            2
        ),
        0.0
    ) AS weighted_instability_score,

    (
        SUM(
            CASE
                WHEN es.is_conflict_related
                  OR es.is_protest_related
                  OR es.is_diplomatic_tension_related
                THEN 1
                ELSE 0
            END
        ) >= 3
        AND
        COALESCE(
            AVG(
                CASE
                    WHEN es.is_conflict_related
                      OR es.is_protest_related
                      OR es.is_diplomatic_tension_related
                    THEN GREATEST(-es.goldstein_scale, 0)
                    ELSE NULL
                END
            ),
            0.0
        ) >= 2.0
    ) AS is_alert
FROM events_silver es
LEFT JOIN dim_fips_country_codes fcc
  ON es.focus_country_code = fcc.country_code
WHERE es.focus_country_code IS NOT NULL
GROUP BY
    DATE_TRUNC('hour', es.event_added_ts),
    es.focus_country_code,
    COALESCE(fcc.country_name, 'UNKNOWN: ' || es.focus_country_code)
ORDER BY
    time_window_start,
    country_code;
