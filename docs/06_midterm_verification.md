# 06 Midterm Verification

## Purpose

This document provides a compact reviewer path to verify that the pipeline runs successfully end to end in the local environment.

It focuses on the recommended execution path for the midterm review:
- start the local services
- run the manual Kestra parent flow
- verify the resulting bronze, silver, and gold data in PostgreSQL

## Recommended Verification Path

> [!IMPORTANT]
> For the midterm review, the recommended execution path is the Kestra manual parent flow `pipeline_run_manual`.

## 1. Start the local environment

From the repository root, start the required services:

```bash
docker compose up -d
```

This starts the local project environment including:

* PostgreSQL
* pgAdmin
* Kestra
* the pipeline container

## 2. Open the local services

Open the following services in your browser:

* **pgAdmin:** `http://localhost:8085`
* **Kestra:** `http://localhost:8080`

## 3. Run the pipeline in Kestra

> [!NOTE]
> Expected runtime depends on the selected execution mode. An incremental run should usually finish much faster than a historical backfill. A small backfill such as `days = 2` is suitable for review and local validation, while larger backfill windows may take significantly longer.

Use the Kestra UI to run the manual parent flow:

* **Namespace:** `hslu.geopolitical_risk.main`
* **Flow:** `pipeline_run_manual`

Recommended verification options:

### Incremental run

Run the flow without inputs.

This triggers:

* lookup loading
* incremental manifest sync
* export ingestion
* silver transformation
* gold rebuild

### Small backfill run

Optionally run the flow with a small backfill window, for example:

* `days = 2`

This verifies the historical manifest path before running the downstream pipeline.

## 4. Log in to pgAdmin

Use the following credentials:

* **Email:** `root@root.ch`
* **Password:** `root`

Then register the PostgreSQL server with:

* **Host:** `pgdatabase`
* **Port:** `5432`
* **Database:** `gdelt`
* **Username:** `root`
* **Password:** `root`

## 5. Verify the pipeline results

### 5.1 Check discovered and processed batches

```sql
SELECT
    batch_id,
    file_name,
    status,
    gdelt_timestamp,
    downloaded_at,
    loaded_at,
    claimed_at,
    claimed_by
FROM pipeline_batches
ORDER BY batch_id;
```

Expected result:

* at least one batch should exist
* successfully ingested export batches should have `status = 'loaded'`
* `downloaded_at` and `loaded_at` should be populated for processed batches
* `claimed_at` and `claimed_by` should normally be `NULL` after successful completion

### 5.2 Check bronze row counts

```sql
SELECT
    batch_id,
    COUNT(*) AS bronze_rows
FROM events_bronze
GROUP BY batch_id
ORDER BY batch_id;
```

Expected result:

* at least one processed batch should have bronze rows

### 5.3 Check silver row counts

```sql
SELECT
    batch_id,
    COUNT(*) AS silver_rows
FROM events_silver
GROUP BY batch_id
ORDER BY batch_id;
```

Expected result:

* at least one processed batch should have silver rows
* silver row counts should correspond to transformed bronze batches

### 5.4 Check gold output

```sql
SELECT
    time_window_start,
    country_code,
    country_name,
    total_event_count,
    protest_event_count,
    conflict_event_count,
    diplomatic_tension_event_count,
    weighted_instability_score,
    is_alert
FROM risk_alerts_gold
ORDER BY time_window_start DESC, weighted_instability_score DESC
LIMIT 20;
```

Expected result:

* the gold table should contain hourly country-level summary rows
* `weighted_instability_score` and `is_alert` should be populated

## 6. Optional CLI-only verification

If you want to verify the project without Kestra, you can run the pipeline directly in the pipeline container.

### Incremental run

```bash
docker compose run --rm pipeline uv run --no-dev deng-ingestion quickstart
```

### Small backfill run

```bash
docker compose run --rm pipeline uv run --no-dev deng-ingestion quickstart --days 2
```

This is optional. The recommended reviewer path remains the Kestra manual parent flow.

## 7. What the reviewer should see

A successful verification should demonstrate:

* the local environment starts reproducibly with Docker Compose
* orchestration works through Kestra
* raw export batches are registered and loaded into bronze
* transformed analyst-oriented records are available in silver
* hourly country-level summaries are available in gold

## Troubleshooting

### The pipeline runs for a long time

Use an incremental run without inputs or reduce the backfill window to a small value such as `days = 2`.

### No data appears in bronze, silver, or gold

Check:

* whether the Kestra flow completed successfully
* whether PostgreSQL is running
* whether the reviewer is connected to the correct database in pgAdmin

### A batch remains claimed

If a run is interrupted, `claimed_at` and `claimed_by` may temporarily show an in-progress claim. A successful rerun should normally clear the claim state.
