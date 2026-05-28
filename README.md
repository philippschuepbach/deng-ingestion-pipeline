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
docker compose down --volumes --remove-orphans
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

## 5. Final Cloud Pipeline - Terraform, GCS, BigQuery, and Kestra

This section covers the final cloud-oriented part of the project.

It provisions the Google Cloud data lake and warehouse infrastructure with Terraform, ingests GDELT data into Google Cloud Storage, builds the BigQuery silver layer, and then builds the BigQuery gold monitoring output.

### 5.1 Cloud prerequisites

Before starting this section, make sure you have:

* a Google Cloud project with billing enabled
* the Google Cloud Storage and BigQuery APIs enabled in that project
* a Google Cloud service account JSON key with permissions to create and use a Cloud Storage bucket and a BigQuery dataset
* Terraform installed locally
* the Docker Compose stack from section 2 available for Kestra orchestration

For the smoothest reviewer workflow, place the service account JSON file in the repository under:

```text
keys/
```

Example:

```text
keys/my-service-account.json
```

The `keys/*.json` path is ignored by Git.

### 5.2 Provision the cloud infrastructure

Run the provisioning helper from the repository root:

```bash
./scripts/provision_gcp_infra.sh
```

The executable bit is tracked in Git, so this should work after a normal clone. If your environment loses executable permissions, run:

```bash
bash scripts/provision_gcp_infra.sh
```

If you use fish, run the same command:

```fish
./scripts/provision_gcp_infra.sh
```

The script uses Bash internally through its shebang.

If `.env` does not exist yet, the script creates it from `.env.example`.

If `GOOGLE_CLOUD_PROJECT` is missing or still set to the placeholder, the script asks for your Google Cloud project ID:

```text
Google Cloud project ID:
```

Enter your project ID, for example:

```text
my-gcp-project-123
```

If `GOOGLE_APPLICATION_CREDENTIALS` is empty, the script asks for the path to your service account JSON file:

```text
Google Cloud credentials JSON path:
```

Recommended input:

```text
./keys/my-service-account.json
```

The script then:

* normalizes `.env` line endings
* generates unique `OBJECT_STORAGE_BUCKET` and `BIGQUERY_DATASET` values if they are empty
* writes `terraform/terraform.tfvars` from `.env`
* runs `terraform init`
* runs `terraform fmt`
* runs `terraform validate`
* runs `terraform apply -var-file="terraform.tfvars"`

When Terraform asks for confirmation, type:

```text
yes
```

Expected result:

* a Google Cloud Storage bucket is created
* a BigQuery dataset is created
* `.env` contains the generated `OBJECT_STORAGE_BUCKET` and `BIGQUERY_DATASET`
* `terraform/terraform.tfvars` contains the same project, bucket, and dataset values

### 5.3 Start Kestra for the cloud flow

If the Docker Compose stack is not already running, start it:

```bash
docker compose up -d
```

Open Kestra in your browser:

```text
http://localhost:8080
```

Use the credentials from `.env` or `.env.example`:

* **Email:** `admin@kestra.io`
* **Password:** `Admin1234!`

### 5.4 Prepare credentials for the Kestra UI

For manual review runs, the cloud Kestra flow accepts credentials directly through the UI.

Create a single-line base64 value from your service account JSON file:

```bash
base64 -w0 keys/my-service-account.json
```

On macOS, use:

```bash
base64 -i keys/my-service-account.json | tr -d '\n'
```

Copy the full output. It should be one long line.

> [!NOTE]
> Paste the base64 output into Kestra, not the raw JSON file content.

### 5.5 Run the cloud pipeline manually

In the Kestra UI, run:

* **Namespace:** `hslu.geopolitical_risk.main`
* **Flow:** `cloud_pipeline_run_manual`

For a normal incremental run, keep:

* `years = 0`
* `months = 0`
* `days = 0`

Paste the base64 service account value into:

```text
google_credentials_json_base64
```

Then execute the flow.

Expected result:

* `cloud_datalake_ingest` succeeds
* `cloud_events_silver_build` succeeds
* `cloud_risk_alerts_gold_build` succeeds

For a small historical backfill, set for example:

* `days = 1`

and leave `years` and `months` at `0`.

### 5.6 Verify the cloud data lake in Google Cloud Storage

Open the Google Cloud Storage bucket from `.env`:

```text
OBJECT_STORAGE_BUCKET
```

Expected object prefixes:

```text
raw/gdelt/lookups/
raw/gdelt/export/archives/
bronze/gdelt/export/events/
```

Expected result:

* lookup files exist under `raw/gdelt/lookups/`
* raw ZIP archives exist under `raw/gdelt/export/archives/`
* extracted event CSV files exist under `bronze/gdelt/export/events/`

### 5.7 Verify the BigQuery warehouse

Open BigQuery and replace `YOUR_PROJECT.YOUR_DATASET` in the queries below with the values from `.env`:

```text
GOOGLE_CLOUD_PROJECT.BIGQUERY_DATASET
```

#### Check that the expected tables exist

```sql
SELECT
  table_name,
  table_type,
  creation_time
FROM `YOUR_PROJECT.YOUR_DATASET.INFORMATION_SCHEMA.TABLES`
WHERE table_name IN (
  'events_bronze_external',
  'dim_fips_country_codes_external',
  'events_silver',
  'risk_alerts_gold'
)
ORDER BY table_name;
```

Expected result:

* the bronze external table exists
* the FIPS lookup external table exists
* the silver table exists
* the gold table exists

#### Check the silver layer

```sql
SELECT
  COUNT(*) AS silver_rows,
  MIN(event_added_ts) AS earliest_event,
  MAX(event_added_ts) AS latest_event
FROM `YOUR_PROJECT.YOUR_DATASET.events_silver`;
```

Expected result:

* `silver_rows` is greater than `0`
* `earliest_event` and `latest_event` are populated

#### Check the gold output

```sql
SELECT
  COUNT(*) AS gold_rows,
  MIN(time_window_start) AS earliest_window,
  MAX(time_window_start) AS latest_window,
  COUNTIF(is_alert) AS alert_rows
FROM `YOUR_PROJECT.YOUR_DATASET.risk_alerts_gold`;
```

Expected result:

* `gold_rows` is greater than `0`
* the window timestamps are populated
* `alert_rows` returns a count

#### Inspect recent country-level monitoring rows

```sql
SELECT
  time_window_start,
  country_code,
  country_name,
  total_event_count,
  relevant_event_count,
  protest_event_count,
  conflict_event_count,
  diplomatic_tension_event_count,
  negative_goldstein_sum,
  weighted_instability_score,
  is_alert
FROM `YOUR_PROJECT.YOUR_DATASET.risk_alerts_gold`
ORDER BY time_window_start DESC, weighted_instability_score DESC
LIMIT 50;
```

Expected result:

* rows are grouped by country and hourly time window
* risk-relevant event counts are populated
* `weighted_instability_score` and `is_alert` are populated

#### Verify partitioning and clustering

```sql
SELECT ddl
FROM `YOUR_PROJECT.YOUR_DATASET.INFORMATION_SCHEMA.TABLES`
WHERE table_name = 'risk_alerts_gold';
```

Expected result:

* `risk_alerts_gold` is partitioned by `DATE(time_window_start)`
* `risk_alerts_gold` is clustered by `country_code, is_alert`

The silver table is created with:

* partitioning by `event_date`
* clustering by `focus_country_code, event_root_code`

This matches the expected access pattern: analysts filter by time window and country, then inspect risk categories and event roots.

### 5.8 Scheduled cloud pipeline

A scheduled cloud flow is included but disabled by default:

* **Namespace:** `hslu.geopolitical_risk.main`
* **Flow:** `cloud_pipeline_run_scheduled`

The schedule is configured as:

```text
0 * * * *
```

This means once per hour.

It is disabled by default to avoid accidental cloud executions and costs during review.

> [!NOTE]
> The manual flow is recommended for peer review because credentials can be pasted into the Kestra UI for that run. For unattended scheduled execution, use a container-visible credentials path such as `./keys/my-service-account.json` in `.env`.

### 5.9 Optional cloud cleanup

If you want to remove the cloud resources created by Terraform, run:

```bash
cd terraform
terraform destroy -var-file="terraform.tfvars"
```

> [!WARNING]
> The storage bucket uses `force_destroy = true`, so destroying the Terraform resources can delete bucket contents.

## 6. Troubleshooting

### 6.1 The pipeline runs for a long time

Use an incremental run without inputs or reduce the backfill window to a small value such as `days = 2`.

### 6.2 No data appears in bronze, silver, or gold

Check:

* whether the Kestra flow completed successfully
* whether PostgreSQL is running
* whether you are connected to the correct database in pgAdmin
* whether `.env` was created from `.env.example` before starting the stack

### 6.3 A batch remains claimed

If a run is interrupted, `claimed_at` and `claimed_by` may temporarily show an in-progress claim. A successful rerun should normally clear the claim state.

## 7. Additional Information

### 7.1 Ingestion Script Entry Point

The main Python CLI entry point for the ingestion pipeline is:

* `src/deng_ingestion/cli/main.py`

It is exposed through the `deng-ingestion` command defined in `pyproject.toml` and is used to start the local ingestion and pipeline workflows.

Examples:

```bash
docker compose run --rm pipeline uv run --no-dev deng-ingestion quickstart
docker compose run --rm pipeline uv run --no-dev deng-ingestion quickstart --days 2
```

### 7.2 Data Source Attribution

This project uses event data from the **GDELT Project**.

This repository is an independent educational project and is not affiliated with or endorsed by the GDELT Project.

For more information, see:

* [https://www.gdeltproject.org/](https://www.gdeltproject.org/)

### 7.3 License

This project is licensed under the **GNU Affero General Public License v3.0 or later** (**AGPL-3.0-or-later**).

See the [LICENSE](./LICENSE) file for the full license text.
