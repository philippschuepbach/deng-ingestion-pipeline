# Data Dictionary

## Source Data Overview

The pipeline is based on the **GDELT event export files**.

Each raw event record includes:
- event identifiers and dates
- actor information
- event classification codes
- geographic references
- media volume metrics
- source URLs

## Lookup Dimensions

Several reference lookup tables are used to decode and structure the raw event data.

### `dim_fips_country_codes`

Maps 2-character FIPS country codes used in GDELT geography fields to human-readable country names.

| Column | Meaning |
|---|---|
| `country_code` | 2-character FIPS country code |
| `country_name` | Human-readable country or region name |

### `dim_cameo_country_codes`

Maps 3-character CAMEO country codes used in actor-related fields.

| Column | Meaning |
|---|---|
| `country_code` | 3-character CAMEO country code |
| `country_name` | Human-readable country or region name |

### `dim_cameo_known_groups`

Maps 3-character known-group codes found in actor fields.

| Column | Meaning |
|---|---|
| `known_group_code` | 3-character group code |
| `known_group_name` | Human-readable group name |
| `is_ambiguous` | Indicates whether the mapping was found to be ambiguous during lookup analysis |

### `dim_cameo_event_roots`

Stores top-level CAMEO event root categories.

| Column | Meaning |
|---|---|
| `event_root_code` | 2-character root event code |
| `event_root_description` | Human-readable root event description |

### `dim_cameo_event_codes`

Stores detailed event codes and their hierarchy.

| Column | Meaning |
|---|---|
| `event_code` | Canonical CAMEO event code |
| `event_base_code` | Intermediate event category derived from the detailed event code |
| `event_root_code` | Top-level event category |
| `event_description` | Human-readable description of the event code |
| `goldstein_scale` | Goldstein conflict/cooperation score associated with the event code |

### `dim_risk_category_mapping`

Curated mapping layer used to assign event-root codes to the custom analytical categories.

| Column | Meaning |
|---|---|
| `event_root_code` | CAMEO root event code |
| `is_protest_related` | Whether the root code is treated as protest-related |
| `is_conflict_related` | Whether the root code is treated as conflict-related |
| `is_diplomatic_tension_related` | Whether the root code is treated as diplomatic-tension-related |

## Control Table

### `pipeline_batches`

Tracks discovered manifest entries and batch processing lifecycle information.

| Column | Meaning |
|---|---|
| `batch_id` | Surrogate key for the batch |
| `source_type` | Manifest source, e.g. `lastupdate` or `masterfilelist` |
| `file_type` | GDELT file type, e.g. `export` |
| `source_url` | Original batch file URL |
| `file_name` | Batch file name |
| `file_size_bytes` | File size from the manifest |
| `md5_hash` | MD5 checksum from the manifest |
| `gdelt_timestamp` | Timestamp embedded in the GDELT file name |
| `discovered_at` | Local timestamp when the batch was registered |
| `downloaded_at` | Timestamp when the archive was downloaded |
| `loaded_at` | Timestamp when the archive was loaded into bronze |
| `status` | Batch processing state |
| `error_message` | Optional error information |

## Bronze Layer

### `events_bronze`

Stores raw GDELT event records with batch lineage.

### Important bronze fields

| Column | Meaning |
|---|---|
| `batch_id` | Batch lineage reference |
| `global_event_id` | Unique event identifier from GDELT |
| `day` | Event date in `YYYYMMDD` format |
| `event_code` | Detailed CAMEO event code |
| `event_root_code` | Top-level event category code |
| `quad_class` | High-level conflict/cooperation category |
| `goldstein_scale` | Event conflict/cooperation score |
| `num_mentions` | Number of mentions associated with the event |
| `num_sources` | Number of distinct sources |
| `num_articles` | Number of source articles |
| `avg_tone` | Average tone score |
| `action_geo_country_code` | Geographic focus country code |
| `action_geo_fullname` | Geographic focus location name |
| `action_geo_lat` | Geographic latitude |
| `action_geo_long` | Geographic longitude |
| `source_url` | Original source URL |

## Silver Layer

### `events_silver`

Stores cleaned, decoded, and analyst-oriented event records.

### Important silver fields

| Column | Meaning |
|---|---|
| `silver_event_id` | Surrogate key |
| `batch_id` | Batch lineage reference |
| `global_event_id` | Original global event identifier |
| `event_date` | Parsed event date |
| `event_added_ts` | Parsed ingestion timestamp from the source record |
| `event_code` | Detailed event code |
| `event_root_code` | Top-level event category |
| `quad_class` | High-level conflict/cooperation category |
| `goldstein_scale` | Goldstein score |
| `actor1_name` | Primary actor name |
| `actor1_country_code` | Primary actor country code |
| `actor1_known_group_code` | Primary actor known-group code |
| `actor2_name` | Secondary actor name |
| `actor2_country_code` | Secondary actor country code |
| `actor2_known_group_code` | Secondary actor known-group code |
| `focus_country_code` | Geographic focus country code used for downstream aggregation |
| `focus_location_name` | Geographic focus location name |
| `focus_geo_type` | Geographic focus type |
| `focus_geo_lat` | Geographic focus latitude |
| `focus_geo_long` | Geographic focus longitude |
| `num_mentions` | Mention count |
| `num_sources` | Source count |
| `num_articles` | Article count |
| `avg_tone` | Average tone score |
| `source_url` | Original source URL |
| `is_protest_related` | Project-specific protest flag |
| `is_conflict_related` | Project-specific conflict flag |
| `is_diplomatic_tension_related` | Project-specific diplomatic-tension flag |
| `transformed_at` | Timestamp when the silver row was created |

## Gold Layer

### `risk_alerts_gold`

Stores hourly country-level risk summaries derived from the silver layer.

### Important gold fields

| Column | Meaning |
|---|---|
| `alert_window_id` | Surrogate key |
| `time_window_start` | Start of the one-hour aggregation window |
| `time_window_end` | End of the one-hour aggregation window |
| `country_code` | Country code used for aggregation |
| `country_name` | Human-readable country name |
| `total_event_count` | Total number of events in the window |
| `protest_event_count` | Number of protest-related events |
| `conflict_event_count` | Number of conflict-related events |
| `diplomatic_tension_event_count` | Number of diplomatic-tension-related events |
| `total_mentions` | Sum of mentions across all grouped events |
| `total_sources` | Sum of sources across all grouped events |
| `total_articles` | Sum of articles across all grouped events |
| `avg_goldstein_scale` | Average Goldstein score across the grouped events |
| `avg_tone` | Average tone across the grouped events |
| `weighted_instability_score` | Project-specific aggregated instability score |
| `is_alert` | Simple alert flag based on the score threshold |
| `created_at` | Timestamp when the gold row was built |
