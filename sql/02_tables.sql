-- Control table for GDELT batch ingestion.
-- Stores metadata about each discovered batch file, its processing state,
-- and key timestamps throughout the ingestion lifecycle.
CREATE TABLE pipeline_batches (
    batch_id BIGSERIAL PRIMARY KEY,
    source_type TEXT NOT NULL,
    file_type TEXT NOT NULL,
    source_url TEXT NOT NULL UNIQUE,
    file_name TEXT NOT NULL,
    file_size_bytes BIGINT,
    md5_hash TEXT,
    gdelt_timestamp TIMESTAMP NOT NULL,
    discovered_at TIMESTAMP NOT NULL DEFAULT NOW(),
    downloaded_at TIMESTAMP,
    loaded_at TIMESTAMP,
    claimed_at TIMESTAMPTZ,
    claimed_by TEXT,
    status pipeline_batch_status NOT NULL DEFAULT 'discovered',
    error_message TEXT
);

-- Lookup table for 2-character FIPS country codes used in GDELT geography fields.
-- Stores the canonical country code and its human-readable geographic label.
CREATE TABLE dim_fips_country_codes (
    country_code TEXT PRIMARY KEY,
    country_name TEXT NOT NULL
);

-- Lookup table for 3-character CAMEO country codes used in actor country fields.
-- Stores the canonical code and its human-readable country or region label.
CREATE TABLE dim_cameo_country_codes (
    country_code TEXT PRIMARY KEY,
    country_name TEXT NOT NULL
);

-- Lookup table for 3-character CAMEO known-group codes used in actor known-group fields.
-- Stores the canonical group code, its human-readable name, and a flag for ambiguous
-- mappings when the raw source lookup contains conflicting labels for the same code.
CREATE TABLE dim_cameo_known_groups (
    known_group_code TEXT PRIMARY KEY,
    known_group_name TEXT NOT NULL,
    is_ambiguous BOOLEAN NOT NULL DEFAULT FALSE
);

-- Lookup table for unique CAMEO root event codes.
-- Stores the top-level event taxonomy used for higher-level aggregation
-- and downstream category mapping.

CREATE TABLE dim_cameo_event_roots (
    event_root_code TEXT PRIMARY KEY,
    event_root_description TEXT NOT NULL
);

-- Lookup table for CAMEO event codes.
-- Stores the canonical event code, its hierarchy, description,
-- and the associated Goldstein score.
CREATE TABLE dim_cameo_event_codes (
    event_code TEXT PRIMARY KEY,
    event_base_code TEXT NOT NULL,
    event_root_code TEXT NOT NULL REFERENCES dim_cameo_event_roots(event_root_code),
    event_description TEXT NOT NULL,
    goldstein_scale NUMERIC(3,1) NOT NULL
);

-- Curated mapping table that translates GDELT/CAMEO root event codes into
-- analyst-oriented risk categories used in this project, such as protest,
-- conflict, and diplomatic tension.
CREATE TABLE dim_risk_category_mapping (
    event_root_code TEXT PRIMARY KEY REFERENCES dim_cameo_event_roots(event_root_code),
    is_protest_related BOOLEAN NOT NULL DEFAULT FALSE,
    is_conflict_related BOOLEAN NOT NULL DEFAULT FALSE,
    is_diplomatic_tension_related BOOLEAN NOT NULL DEFAULT FALSE
);

-- Bronze-layer table for raw GDELT 2.0 event records.
-- It stores the full Event table schema from the source file, plus technical
-- ingestion metadata for traceability and batch-level lineage.
CREATE TABLE events_bronze (
    event_row_id BIGSERIAL PRIMARY KEY,
    batch_id BIGINT NOT NULL REFERENCES pipeline_batches(batch_id),
    raw_ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),

    global_event_id BIGINT NOT NULL,
    day INTEGER NOT NULL,
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
    source_url TEXT,

    UNIQUE (batch_id, global_event_id)
);

-- Curated event layer for geopolitical risk analysis.
-- Stores decoded GDELT events with cleaned timestamps, enriched event metadata,
-- and a focus country used for downstream aggregation.

CREATE TABLE events_silver (
    silver_event_id BIGSERIAL PRIMARY KEY,

    batch_id BIGINT NOT NULL REFERENCES pipeline_batches(batch_id),
    global_event_id BIGINT NOT NULL UNIQUE,

    event_date DATE NOT NULL,
    event_added_ts TIMESTAMP NOT NULL,

    event_code TEXT NOT NULL REFERENCES dim_cameo_event_codes(event_code),
    event_root_code TEXT NOT NULL REFERENCES dim_cameo_event_roots(event_root_code),
    quad_class SMALLINT CHECK (quad_class BETWEEN 1 AND 4),
    goldstein_scale NUMERIC(3,1) NOT NULL,

    actor1_name TEXT,
    actor1_country_code TEXT,
    actor1_known_group_code TEXT,
    actor2_name TEXT,
    actor2_country_code TEXT,
    actor2_known_group_code TEXT,

    focus_country_code TEXT,
    focus_location_name TEXT,
    focus_geo_type SMALLINT,
    focus_geo_lat DOUBLE PRECISION,
    focus_geo_long DOUBLE PRECISION,

    num_mentions INTEGER,
    num_sources INTEGER,
    num_articles INTEGER,
    avg_tone DOUBLE PRECISION,
    source_url TEXT,

    is_protest_related BOOLEAN NOT NULL,
    is_conflict_related BOOLEAN NOT NULL,
    is_diplomatic_tension_related BOOLEAN NOT NULL,

    transformed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Gold-layer aggregation table for geopolitical risk monitoring.
-- Stores country-level event aggregates per time window, enriched with
-- instability-related metrics and a weighted score for downstream alerting
-- and analytical reporting.

CREATE TABLE risk_alerts_gold (
    alert_window_id BIGSERIAL PRIMARY KEY,

    time_window_start TIMESTAMP NOT NULL,
    time_window_end TIMESTAMP NOT NULL,

    country_code TEXT NOT NULL,
    country_name TEXT NOT NULL,

    total_event_count INTEGER NOT NULL,
    protest_event_count INTEGER NOT NULL,
    conflict_event_count INTEGER NOT NULL,
    diplomatic_tension_event_count INTEGER NOT NULL,

    total_mentions INTEGER NOT NULL,
    total_sources INTEGER NOT NULL,
    total_articles INTEGER NOT NULL,

    avg_goldstein_scale NUMERIC(4,2),
    avg_tone DOUBLE PRECISION,

    weighted_instability_score DOUBLE PRECISION NOT NULL,
    is_alert BOOLEAN NOT NULL DEFAULT FALSE,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    UNIQUE (time_window_start, time_window_end, country_code),
    CHECK (time_window_end > time_window_start)
);

-- Supports claim-based selection of export batches for download/load steps.
CREATE INDEX idx_pipeline_batches_export_claim
    ON pipeline_batches (file_type, status, claimed_at, gdelt_timestamp);

-- Supports claim-based selection of already loaded batches for silver transformation.
CREATE INDEX idx_pipeline_batches_loaded_claim
    ON pipeline_batches (status, claimed_at, gdelt_timestamp);

-- Supports EXISTS checks on bronze batch presence.
CREATE INDEX idx_events_bronze_batch_id
    ON events_bronze (batch_id);

-- Supports NOT EXISTS / lookup checks for already transformed silver batches.
CREATE INDEX idx_events_silver_batch_id
    ON events_silver (batch_id);

-- Supports common gold queries by latest window and country.
CREATE INDEX idx_risk_alerts_gold_window_country
    ON risk_alerts_gold (time_window_start, country_code);

-- Supports descending "latest window first" style monitoring queries.
CREATE INDEX idx_risk_alerts_gold_window_score
    ON risk_alerts_gold (time_window_start DESC, weighted_instability_score DESC);
