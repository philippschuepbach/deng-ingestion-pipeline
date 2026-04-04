# gdelt-ingestion

## Project Overview
This project implements a robust data ingestion pipeline for the GDELT dataset, leveraging modern Python practices and tools. The pipeline is designed to be modular, maintainable, and easily deployable using Docker and Terraform. It includes components for fetching the latest GDELT data, downloading and extracting it safely, and loading it into both PostgreSQL and Google BigQuery for further analysis.

## Quick Start
1. Clone the repository and navigate to the project directory.
2. Follow the setup instructions in the "Prerequisites & Setup" section below to install necessary tools and dependencies.
3. Run the pipeline locally using the provided CLI or deploy it using Docker and Terraform.

```bash
cp .env.example .env
docker compose up -d
uv run python -m gdelt_ingestion.cli ingest-local
```

## Project Structure

**Key entrypoints:**
- `src/gdelt_ingestion/cli.py` – main CLI entrypoint (local runs + orchestration hooks)
- `orchestration/airflow/...` – Airflow DAG definition
- `orchestration/kestra/...` – workflow definition for Kestra
- `terraform/` – cloud infrastructure (GCS + BigQuery, etc.)
- `sql/` – SQL scripts for database setup and queries
```
.
├─ README.md
├─ docker-compose.yml
├─ .env.example
├─ pyproject.toml
├─ src/
│  └─ gdelt_ingestion/
│     ├─ __init__.py
│     ├─ config.py
│     ├─ logging_config.py
│     ├─ cli.py
│     │
│     ├─ gdelt/
│     │  ├─ __init__.py
│     │  ├─ lastupdate.py
│     │  ├─ download.py
│     │  └─ extract.py
│     │
│     ├─ storage/
│     │  ├─ __init__.py
│     │  ├─ postgres.py
│     │  ├─ gcs.py
│     │  └─ bigquery.py
│     │
│     ├─ transform/
│     │  ├─ __init__.py
│     │  └─ transforms.py
│     │
│     └─ orchestration/
│        ├─ airflow/
│        │  └─ dags/
│        │     └─ pipeline_dag.py
│        └─ kestra/
│           └─ workflow.yml
│
├─ terraform/
│  ├─ main.tf
│  ├─ variables.tf
│  ├─ outputs.tf
│  └─ README.md
│
├─ sql/
│  ├─ init_postgres.sql
│  └─ example_queries.sql
│
└─ tests/
   ├─ test_lastupdate.py
   ├─ test_extract.py
   └─ ...
```

## Pipeline Overview

### Local pipeline
1. **Fetch Latest URL:** Retrieve the latest GDELT data URL from the "lastupdate" endpoint.
2. **Download Data:** Download the ZIP file from the extracted URL.
3. **Extract Data:** Safely extract the contents of the ZIP file, ensuring no path traversal vulnerabilities.
4. **Load to PostgreSQL:** Load the raw data into a PostgreSQL database for initial storage and querying.
5. **Upload to GCS:** Upload the raw data file to Google Cloud Storage for backup and further processing.
6. **Load to BigQuery:** Load the data into Google BigQuery for scalable analysis and querying.

### Cloud pipeline
1. **Upload to GCS:** Upload the raw data file to Google Cloud Storage for backup and further processing.
2. **Transform Data:** Apply necessary transformations to the raw data to prepare it for analysis (e.g., cleaning, normalization).
3. **Load to BigQuery:** Load the data into Google BigQuery for scalable analysis and querying.

## Configuration
All configuration parameters (e.g., database credentials, GCP settings, file paths) are managed through environment variables. A `.env.example` file is provided as a template. Make sure to create a `.env` file with the appropriate values before running the pipeline.

## Development & Testing
The project includes unit tests for critical components. To run the tests, use the following command:
```
uv run pytest
```
Make sure to have the necessary test dependencies installed in your environment.

## Terraform & Deployment
The `terraform/` directory contains infrastructure-as-code definitions for deploying the pipeline on cloud platforms. Follow the instructions in `terraform/README.md` for setting up and deploying the infrastructure.


## License

This project is licensed under the GNU Affero General Public License v3.0 or later
(AGPL-3.0-or-later).

See the [LICENSE](./LICENSE) file for the full license text.
