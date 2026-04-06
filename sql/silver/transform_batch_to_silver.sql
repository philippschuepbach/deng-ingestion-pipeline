INSERT INTO events_silver (
    batch_id,
    global_event_id,
    event_date,
    event_added_ts,
    event_code,
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
    is_diplomatic_tension_related
)
SELECT
    b.batch_id,
    b.global_event_id,
    TO_DATE(b.day::text, 'YYYYMMDD') AS event_date,
    TO_TIMESTAMP(b.date_added::text, 'YYYYMMDDHH24MISS') AS event_added_ts,
    c.event_code,
    c.event_root_code,
    b.quad_class,
    c.goldstein_scale,
    b.actor1_name,
    b.actor1_country_code,
    b.actor1_known_group_code,
    b.actor2_name,
    b.actor2_country_code,
    b.actor2_known_group_code,
    b.action_geo_country_code AS focus_country_code,
    b.action_geo_fullname AS focus_location_name,
    b.action_geo_type AS focus_geo_type,
    b.action_geo_lat AS focus_geo_lat,
    b.action_geo_long AS focus_geo_long,
    b.num_mentions,
    b.num_sources,
    b.num_articles,
    b.avg_tone,
    b.source_url,
    COALESCE(m.is_protest_related, FALSE) AS is_protest_related,
    COALESCE(m.is_conflict_related, FALSE) AS is_conflict_related,
    COALESCE(m.is_diplomatic_tension_related, FALSE) AS is_diplomatic_tension_related
FROM events_bronze b
JOIN dim_cameo_event_codes c
  ON b.event_code = c.event_code
LEFT JOIN dim_risk_category_mapping m
  ON c.event_root_code = m.event_root_code
WHERE b.batch_id = %(batch_id)s
  AND b.day IS NOT NULL
  AND b.date_added IS NOT NULL
ON CONFLICT (global_event_id) DO NOTHING;
