# Orchestration

## Why Kestra was chosen

Kestra was selected because the existing pipeline is already structured as a set of clear CLI commands and stage boundaries.

This makes it a good fit for orchestration through declarative flow definitions, reusable subflows, scheduled runs, and manual parameterized executions.

## Current Local Setup

The local orchestration environment consists of:

* **`kestra`**
  Runs the Kestra server and UI

* **`kestra_postgres`**
  Stores Kestra metadata such as flows, executions, and orchestration state

* **`pgdatabase`**
  Stores the project data pipeline tables

* **`pgadmin`**
  Provides database inspection for pipeline verification

## Flow Storage

Flows are stored in the repository under:

```text
kestra/flows/
```

They are mounted into the Kestra container and synchronized automatically.

## Namespace Structure

The following Kestra namespaces are used:

* **`hslu.geopolitical_risk.main`**
* **`hslu.geopolitical_risk.subflows`**

The namespace structure separates:

* **main entry flows**
* **reusable pipeline subflows**

## Current Flow Structure

### Main flows

* **`pipeline_run_manual`**
  Manual parent flow that orchestrates the complete local pipeline and supports parameterized backfill windows

* **`pipeline_run_scheduled`**
  Scheduled parent flow for recurring incremental runs

* **`cloud_pipeline_run_manual`**
  Manual parent flow for the final cloud pipeline. It ingests GDELT export data into Google Cloud Storage, builds the BigQuery silver layer, and rebuilds the BigQuery gold monitoring output.

* **`cloud_pipeline_run_scheduled`**
  Scheduled parent flow for recurring cloud pipeline runs. The schedule is disabled by default to avoid unintended cloud executions during local development and review.

### Subflows

* **`load_lookups`**
* **`manifest_sync`**
* **`export_ingest_all`**
* **`silver_transform_all`**
* **`gold_build`**
* **`cloud_datalake_ingest`**
* **`cloud_events_silver_build`**
* **`cloud_risk_alerts_gold_build`**

## Current Execution Model

Kestra orchestrates the existing CLI through commands such as:

```bash
uv run --no-dev --no-sync deng-ingestion ...
```

This keeps the business logic inside the Python pipeline. Kestra is used only for orchestration, scheduling, and execution visibility.

## Manual Pipeline Execution

The manual parent flow supports parameterized time windows through:

* `years`
* `months`
* `days`

If all values are `0`, the pipeline runs in **incremental** mode.

If any value is greater than `0`, the pipeline runs a **manifest backfill** before continuing with the downstream stages.

This makes the manual flow suitable for testing, demonstrations, and controlled historical backfills.

## Scheduled Pipeline Execution

The scheduled parent flow is intended for recurring local batch execution. It keeps the analytical layers refreshed with new data.

## Cloud Pipeline Execution

The cloud-oriented final pipeline is exposed through:

* **Namespace:** `hslu.geopolitical_risk.main`
* **Manual flow:** `cloud_pipeline_run_manual`
* **Scheduled flow:** `cloud_pipeline_run_scheduled`

The manual cloud flow accepts the same relative backfill inputs as the local manual flow:

* `years`
* `months`
* `days`

If all values are `0`, it runs the incremental cloud data lake ingestion path. If any value is greater than `0`, it runs the cloud data lake backfill path before building the BigQuery silver and gold outputs.

The cloud flows expect the Google Cloud settings in `.env`, including:

* `GOOGLE_APPLICATION_CREDENTIALS`
* `GOOGLE_CLOUD_PROJECT`
* `OBJECT_STORAGE_BUCKET`
* `BIGQUERY_DATASET`
