<p align="center">
  <img src="docs/images/title-banner.png" alt="Geopolitical Risk Ingestion Pipeline" width="600">
</p>

# Geopolitical Risk Ingestion Pipeline

This project implements a reproducible end-to-end batch data pipeline for geopolitical risk analysis based on global event data.

The pipeline ingests raw event batches, stores them in PostgreSQL, transforms them into analyst-oriented bronze, silver, and gold layers, and produces hourly country-level risk summaries for monitoring and drill-down analysis.

The local setup is fully reproducible with Docker Compose and includes PostgreSQL, pgAdmin, and Kestra-based orchestration.

## 1. Repository Structure and Documentation

The most relevant parts of the repository for review are:

| Folder/File                  | Purpose                                                                 |
| ---------------------------- | ----------------------------------------------------------------------- |
| src/deng_ingestion/          | Main Python pipeline implementation                                     |
| sql/                         | Database schema and SQL transformation logic                            |
| kestra/flows/                | Kestra workflow definitions                                             |
| grafana/                     | Dashboard provisioning and dashboard JSON files                         |
| docs/                        | Project documentation and verification guide                            |
| tests/                       | Automated test suite                                                    |
| docker/                      | Container helper scripts                                                |
| README.md                    | Main project entry point                                                |
| docker-compose.yaml          | Reproducible local stack                                                |
| pyproject.toml               | Python project metadata and dependencies                                |
| .env.example                 | Example local environment configuration                                 |

### 1.1 Documentation

Additional project documentation is available in the `docs/` directory:

* [01 Architecture and Use Case](docs/01_architecture_and_use_case.md)
* [02 Data Dictionary](docs/02_data_dictionary.md)
* [03 Orchestration](docs/03_orchestration.md)
* [04 Local Development](docs/04_local_development.md)
* [05 Known Issues and Design Decisions](docs/05_known_issues_and_design_decisions.md)

### 1.2 Prerequisites

Clone this repository to your local machine:

```bash
git clone https://github.com/philippschuepbach/deng-ingestion-pipeline.git
```

And switch into the project directory:

```bash
cd deng-ingestion-pipeline
```

#### Docker-based quickstart
- [Docker](https://www.docker.com/get-started) installed and running on your machine

#### Local development
- [Docker](https://www.docker.com/get-started) installed and running on your machine
- [uv](https://docs.astral.sh/uv/) installed

### 1.3 Environment Variables

The project uses a local `.env` file for reproducible local development and Docker Compose execution.

The most important variables are:

| Variable                     | Purpose                                                                 |
| ---------------------------- | ----------------------------------------------------------------------- |
| `POSTGRES_HOST`              | Database host for local host-side Python execution, usually `localhost` |
| `POSTGRES_PORT`              | PostgreSQL port for local host-side execution                           |
| `POSTGRES_DB`                | Application database name                                               |
| `POSTGRES_USER`              | Application database username                                           |
| `POSTGRES_PASSWORD`          | Application database password                                           |
| `APP_POSTGRES_HOST`          | Database host used inside Docker Compose, usually `pgdatabase`          |
| `APP_POSTGRES_PORT`          | Database port used inside Docker Compose                                |
| `PGADMIN_DEFAULT_EMAIL`      | Login email for the local pgAdmin instance                              |
| `PGADMIN_DEFAULT_PASSWORD`   | Login password for the local pgAdmin instance                           |
| `KESTRA_POSTGRES_DB`         | Database name for Kestra metadata storage                               |
| `KESTRA_POSTGRES_USER`       | Username for the Kestra metadata database                               |
| `KESTRA_POSTGRES_PASSWORD`   | Password for the Kestra metadata database                               |
| `KESTRA_BASIC_AUTH_USERNAME` | Local login username for the Kestra UI                                  |
| `KESTRA_BASIC_AUTH_PASSWORD` | Local login password for the Kestra UI                                  |
| `KESTRA_URL`                 | Base URL used by the local Kestra setup                                 |
| `GRAFANA_ADMIN_USER`         | Local Grafana admin username                                            |
| `GRAFANA_ADMIN_PASSWORD`     | Local Grafana admin password                                            |
| `LOG_LEVEL`                  | Application log level, e.g. `INFO` or `DEBUG`                           |
| `UV_LINK_MODE`               | uv file linking mode used inside the container setup                    |

> [!NOTE]
> The values in `.env.example` are local demo defaults for reproducible testing only. They are not production credentials.

## 2. Quickstart - Local Pipeline and Reproducible Environment

> [!NOTE]
> Before you start, make sure that you've cleaned up any previous local Docker resources to avoid conflicts


Create the environment file from the example file:

```bash
cp .env.example .env
```


### Build the local environment

```bash
docker compose --profile manual build
```

### Start the local environment

```bash
docker compose up -d
```

It takes around 30 seconds for all services to start and become healthy.

### 2.1 Check that the services started successfully

After starting the stack, verify that the containers are up:

```bash
docker compose ps
```

Expected result:

* the main services should be listed as `running`
* PostgreSQL and Kestra-related services should not be restarting continuously
* health checks should report `healthy` where configured

If you want to inspect recent startup logs, run:

```bash
docker compose logs --tail=100
```

To inspect a specific service more closely, for example PostgreSQL or Kestra:

```bash
docker compose logs --tail=100 pgdatabase
docker compose logs --tail=100 kestra
```

If a service failed to start, check whether it exited, keeps restarting, or shows repeated error messages in the logs.

### 2.2 Verify the Database Connection with pgAdmin

Open pgAdmin in your browser at `http://localhost:8085`

Use the default local demo credentials from `.env.example`:

* **Email:** `root@root.ch`
* **Password:** `root`

If you changed the values in `.env`, use your local values instead.

Then register the PostgreSQL server with the application database credentials from `.env`:

* **Host:** `pgdatabase`
* **Port:** `5432`
* **Database:** `gdelt`
* **Username:** `root`
* **Password:** `root`

Run a simple test query to verify the connection:

```sql
SELECT version();
```

Expected result:

* PostgreSQL returns a version string
* this confirms that the database connection works and that pgAdmin is connected to the running local PostgreSQL instance

### 2.3 Run the pipeline directly in the pipeline container

#### Incremental run

Runs the pipeline against the latest available GDELT data without a historical backfill window.

```bash
docker compose --profile manual run --rm pipeline uv run --no-dev deng-ingestion quickstart
````

#### Historical backfill run

Runs the same pipeline, but first registers historical batches from the GDELT master file list before continuing with downstream ingestion and transformation.

The backfill window is controlled through relative time parameters such as:

* `--days`
* `--months`
* `--years`

Example:

```bash
docker compose --profile manual run --rm pipeline uv run --no-dev deng-ingestion quickstart --days 1
```

This processes a small historical backfill window of the last 2 days before continuing with the downstream pipeline.

> [!NOTE]
> A larger backfill window can take a long time in the local setup because many historical batches may need to be registered, downloaded, ingested, and transformed. For local testing, a small window such as `--days 2` is recommended.


### 2.4 Verify the pipeline results

#### Check discovered and processed batches

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
ORDER BY batch_id
LIMIT 1000;
```

Expected result:

* at least one batch should exist
* successfully ingested export batches should have `status = 'loaded'`
* `downloaded_at` and `loaded_at` should be populated for processed batches
* `claimed_at` and `claimed_by` should normally be `NULL` after successful completion

#### Check bronze row counts

```sql
SELECT
    batch_id,
    COUNT(*) AS bronze_rows
FROM events_bronze
GROUP BY batch_id
ORDER BY batch_id
LIMIT 1000;
```

Expected result:

* at least one processed batch should have bronze rows

#### Check silver row counts

```sql
SELECT
    batch_id,
    COUNT(*) AS silver_rows
FROM events_silver
GROUP BY batch_id
ORDER BY batch_id
LIMIT 1000;
```

Expected result:

* at least one processed batch should have silver rows
* silver row counts should correspond to transformed bronze batches

#### Check gold output

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

## 3. Workflow Orchestration with Kestra

Open the Kestra UI in your browser at `http://localhost:8080`

Use the default local demo credentials from `.env.example`:

* **Email:** `admin@kestra.io`
* **Password:** `Admin1234!`

If you changed the values in `.env`, use your local values instead.

### 3.1 Manual Workflow Execution

Use the Kestra UI to run the manual parent flow:

* **Namespace:** `hslu.geopolitical_risk.main`
* **Flow:** `pipeline_run_manual`

You can run it:

* without inputs for an incremental execution (leaving inputs at 0)
* or with `years`, `months`, and `days` for a historical backfill

Example:

* `days = 2` → backfill the last 2 days before running the downstream pipeline

> [!NOTE]
> Expected runtime depends on the selected execution mode. An incremental run should usually finish much faster than a historical backfill. A small backfill such as `days = 2` is suitable for review and local validation, while larger backfill windows may take significantly longer.

#### Verify the flow execution

Repeat 2.4 to verify that the pipeline run through Kestra produced the expected results in the database.

### 3.2 Scheduled Workflow Execution

A scheduled Kestra flow is included for recurring incremental execution:

- **Namespace:** `hslu.geopolitical_risk.main`
- **Flow:** `pipeline_run_scheduled`

This scheduled flow is configured to run every 15 minutes. It is disabled by default to avoid unintended executions during local development.

A disabled scheduled trigger may still appear in the Kestra UI under "Next Executions". In observed local tests, the trigger did not execute while disabled, so this is treated as a UI inconsistency rather than an active scheduled run.

## 4. Cleanup

To stop the local environment and remove the project containers, networks, and volumes, run:

```bash
docker compose --volumes --remove-orphans
```

If you used the Pipeline container (`docker compose --profile manual run --rm pipeline...`), you need to specify the `manual` profile to remove the containers created with that profile:

```bash
docker compose --profile manual down --volumes --remove-orphans
```

This removes the main runtime resources created by the local Docker Compose setup.

### Optional: remove locally built images

If you also want to remove the locally built project images, run:

```bash
docker image rm geopolitical-risk-ingestion-pipeline:latest deng-ingestion-pipeline-kestra:latest
```

### Optional: prune unused Docker resources

To remove unused Docker images, stopped containers, and unused networks, run:

```bash
docker system prune
```

To also remove unused Docker volumes, run:

```bash
docker system prune --volumes
```

> [!WARNING]
> The prune commands are not limited to this project. They may remove unused Docker resources from other local projects as well.

## 5. Troubleshooting

### 5.1 The pipeline runs for a long time

Use an incremental run without inputs or reduce the backfill window to a small value such as `days = 2`.

### 5.2 No data appears in bronze, silver, or gold

Check:

* whether the Kestra flow completed successfully
* whether PostgreSQL is running
* whether you are connected to the correct database in pgAdmin
* whether `.env` was created from `.env.example` before starting the stack

### 5.3 A batch remains claimed

If a run is interrupted, `claimed_at` and `claimed_by` may temporarily show an in-progress claim. A successful rerun should normally clear the claim state.

## 6. Additional Information

### 6.1 Ingestion Script Entry Point

The main Python CLI entry point for the ingestion pipeline is:

* `src/deng_ingestion/cli/main.py`

It is exposed through the `deng-ingestion` command defined in `pyproject.toml` and is used to start the local ingestion and pipeline workflows.

Examples:

```bash
docker compose run --rm pipeline uv run --no-dev deng-ingestion quickstart
docker compose run --rm pipeline uv run --no-dev deng-ingestion quickstart --days 2
```

### 6.2 Data Source Attribution

This project uses event data from the **GDELT Project**.

This repository is an independent educational project and is not affiliated with or endorsed by the GDELT Project.

For more information, see:

* [https://www.gdeltproject.org/](https://www.gdeltproject.org/)

### 6.3 License

This project is licensed under the **GNU Affero General Public License v3.0 or later** (**AGPL-3.0-or-later**).

See the [LICENSE](./LICENSE) file for the full license text.
