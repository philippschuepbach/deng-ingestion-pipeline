CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.gdelt_events (
    -- Event Identifiers
    global_event_id INT PRIMARY KEY,
    day_date INT,
    month_year INT,
    year_period INT,
    fraction_date FLOAT8,

    -- Actor 1
    actor1_code VARCHAR(50),
    actor1_name VARCHAR(255),
    actor1_country_code VARCHAR(10),
    actor1_known_group_code VARCHAR(50),
    actor1_type1_code VARCHAR(50),

    -- Actor 2
    actor2_code VARCHAR(50),
    actor2_name VARCHAR(255),
    actor2_country_code VARCHAR(10),

    -- Event Action
    is_root_event INT,
    event_code VARCHAR(10), -- Kept as string per
    event_base_code VARCHAR(10), -- Kept as string per
    event_root_code VARCHAR(10), -- Kept as string per
    quad_class INT,

    -- Event Impact
    goldstein_scale FLOAT8,
    num_mentions INT,
    num_sources INT,
    num_articles INT,
    avg_tone FLOAT8,

    -- Geography (Actor 1)
    actor1_geo_type INT,
    actor1_geo_fullname VARCHAR(255),
    actor1_geo_country_code VARCHAR(10),
    actor1_geo_lat FLOAT8,
    actor1_geo_long FLOAT8,

    -- Data Management Fields (New)
    date_added BIGINT, -- YYYYMMDDHHMMSS [cite: 181]
    source_url TEXT,   --

    -- Metadata
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
