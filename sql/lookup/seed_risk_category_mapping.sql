INSERT INTO dim_risk_category_mapping (
    event_root_code,
    is_protest_related,
    is_conflict_related,
    is_diplomatic_tension_related
)
VALUES (
    %(event_root_code)s,
    %(is_protest_related)s,
    %(is_conflict_related)s,
    %(is_diplomatic_tension_related)s
)
ON CONFLICT (event_root_code) DO UPDATE
SET
    is_protest_related = EXCLUDED.is_protest_related,
    is_conflict_related = EXCLUDED.is_conflict_related,
    is_diplomatic_tension_related = EXCLUDED.is_diplomatic_tension_related;
