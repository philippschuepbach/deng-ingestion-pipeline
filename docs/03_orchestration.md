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

### Subflows

* **`load_lookups`**
* **`manifest_sync`**
* **`export_ingest_all`**
* **`silver_transform_all`**
* **`gold_build`**

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
