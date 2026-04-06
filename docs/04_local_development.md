# Local Development

## Prerequisites

The following tools are expected to be available locally:

- Docker
- Docker Compose
- uv
- Git

## Environment Setup

Create a local `.env` file in the project root.

Example:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=gdelt
POSTGRES_USER=root
POSTGRES_PASSWORD=root
LOG_LEVEL=INFO
````

### Notes

* When running Python locally outside Docker, `POSTGRES_HOST=localhost` is usually correct.
* When running the pipeline inside the Docker `pipeline` service, the database host is `pgdatabase`.
* The local `.env` file is mainly relevant for non-Docker local execution.

## Start Local Infrastructure

Start PostgreSQL and pgAdmin:

```bash
docker compose up -d pgdatabase pgadmin
```

Stop them again with:

```bash
docker compose down
```

Reset the PostgreSQL volume and re-run initialization scripts:

```bash
docker compose down -v
docker compose up -d pgdatabase pgadmin
```

## Python Environment Setup

Install the project dependencies once from the repository root:

```bash
uv sync
```

After that, CLI commands can be run directly with uv run.

## Running the Pipeline Locally with uv

All CLI commands can be run directly from the project root.

General pattern:

```bash
uv run deng-ingestion <command>
```

## Full End-to-End Run

### Incremental quickstart

```bash
uv run deng-ingestion quickstart
```

### Backfill quickstart

```bash
uv run deng-ingestion quickstart --days 2
uv run deng-ingestion quickstart --months 1
uv run deng-ingestion quickstart --years 1 --months 2 --days 10
```

The `quickstart` command runs the full local pipeline:

1. lookup loading
2. manifest ingestion
3. export ingestion into bronze
4. silver transformation
5. gold build

### Incremental pipeline run

This command runs the incremental pipeline in one chained execution context:

```bash
uv run deng-ingestion pipeline incremental
```

Equivalent direct module form:

```bash
uv run -m deng_ingestion.cli.main pipeline incremental
```

The incremental pipeline command runs:

1. manifest incremental
2. export ingest-current-run
3. silver transform-current-run
4. gold build

## Running the Pipeline in Individual Steps

### 1. Lookup loading

Downloads lookup files if missing, loads dimension tables, and seeds the curated risk-category mapping.

```bash
uv run deng-ingestion lookups load
```

### 2. Manifest ingestion

#### Incremental manifest ingestion

Uses `lastupdate.txt` to register the latest available export batches.

```bash
uv run deng-ingestion manifest incremental
```

#### Historical backfill manifest ingestion

Uses `masterfilelist.txt` and filters by a relative time window.

```bash
uv run deng-ingestion manifest backfill --days 2
uv run deng-ingestion manifest backfill --months 6
uv run deng-ingestion manifest backfill --years 1 --months 2 --days 10
```

### 3. Export ingestion into bronze

#### Ingest one pending export batch

```bash
uv run deng-ingestion export ingest
```

#### Ingest all pending export batches

```bash
uv run deng-ingestion export ingest-all
```

This step:

* downloads the export ZIP archive if needed
* extracts the event CSV
* loads the raw event records into `events_bronze`

### 4. Silver transformation

#### Transform one pending bronze batch

```bash
uv run deng-ingestion silver transform
```

#### Transform all pending bronze batches

```bash
uv run deng-ingestion silver transform-all
```

This step:

* parses and standardizes timestamps
* joins event code metadata
* derives project-specific risk flags
* creates the analyst-oriented event-level silver layer

### 5. Gold aggregation

```bash
uv run deng-ingestion gold build
```

Builds the `risk_alerts_gold` table from the silver layer.

## Running the Pipeline Through Docker

The same CLI commands can be executed through one-off pipeline containers.

General pattern:

```bash
docker compose run --rm pipeline uv run --no-dev deng-ingestion <command>
```

### Examples

#### Incremental quickstart

```bash
docker compose run --rm pipeline uv run --no-dev deng-ingestion quickstart
```

#### Backfill quickstart

```bash
docker compose run --rm pipeline uv run --no-dev deng-ingestion quickstart --days 2
```

#### Incremental pipeline run

```bash
docker compose run --rm pipeline uv run --no-dev deng-ingestion pipeline incremental
```

#### Manual step execution

```bash
docker compose run --rm pipeline uv run --no-dev deng-ingestion lookups load
docker compose run --rm pipeline uv run --no-dev deng-ingestion manifest backfill --days 2
docker compose run --rm pipeline uv run --no-dev deng-ingestion export ingest-all
docker compose run --rm pipeline uv run --no-dev deng-ingestion silver transform-all
docker compose run --rm pipeline uv run --no-dev deng-ingestion gold build
```

## Local Data Reuse

Downloaded files are stored under the local `data/` directory and reused across runs.

Important locations:

* `data/lookups/`
  Downloaded reference lookup files

* `data/raw/archives/`
  Downloaded export ZIP archives

* `data/raw/`
  Extracted export CSV files

Repeated backfills or reruns therefore do not need to re-download everything if the files already exist locally.

## Database Inspection

For the reviewer-oriented verification path and the core validation queries, see `06_midterm_verification.md`.

The query below is useful during development for drill-down inspection:

```sql
SELECT
    global_event_id,
    event_date,
    event_code,
    event_root_code,
    actor1_name,
    actor2_name,
    focus_country_code,
    is_protest_related,
    is_conflict_related,
    is_diplomatic_tension_related,
    source_url
FROM events_silver
WHERE focus_country_code = 'IR'
ORDER BY event_date DESC
LIMIT 20;
```

## Logging

The log level is controlled through the `LOG_LEVEL` environment variable.

Recommended default:

```env
LOG_LEVEL=INFO
```

Use `DEBUG` when troubleshooting local pipeline behavior.

## Suggested Development Workflow

A practical local workflow during development is:

1. start PostgreSQL and pgAdmin
2. load lookups
3. run a small manifest backfill window
4. ingest all pending export batches
5. transform all pending silver batches
6. rebuild gold
7. inspect the results in pgAdmin

Example:

```bash
docker compose up -d pgdatabase pgadmin
uv run deng-ingestion lookups load
uv run deng-ingestion manifest backfill --days 2
uv run deng-ingestion export ingest-all
uv run deng-ingestion silver transform-all
uv run deng-ingestion gold build
```
