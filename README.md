<p align="center">
  <img src="docs/images/title-banner.png" alt="Geopolitical Risk Ingestion Pipeline" width="600">
</p>

# Geopolitical Risk Ingestion Pipeline

This project implements a reproducible end-to-end batch data pipeline for geopolitical risk analysis based on global event data.

The pipeline ingests raw event batches, stores them in PostgreSQL, transforms them into analyst-oriented bronze, silver, and gold layers, and produces hourly country-level risk summaries for monitoring and drill-down analysis.

The local setup is fully reproducible with Docker Compose and includes PostgreSQL, pgAdmin, and Kestra-based orchestration.

## Prerequisites

* [Docker](https://www.docker.com/get-started) installed and running on your machine

## Quickstart

### Start the local environment

```bash
docker compose up -d
```

### Open the services

* **pgAdmin:** `http://localhost:8085`
* **Kestra:** `http://localhost:8080`

### Run the pipeline

#### Kestra (recommended)

Use the Kestra UI to run the manual parent flow:

* Namespace: `hslu.geopolitical_risk.main`
* Flow: `pipeline_run_manual`

You can run it:

* without inputs for an incremental execution
* or with `years`, `months`, and `days` for a historical backfill

Example:

* `days = 2` → backfill the last 2 days before running the downstream pipeline

> [!NOTE]
> If you use a large backfill window (for example, several months), the pipeline may run for a long time. For local testing, it is recommended to start with a small backfill window (for example, 2 days) or run incrementally without inputs.

#### CLI only (optional)

If you want to run the full pipeline directly without Kestra:

```bash
docker compose run --rm pipeline uv run --no-dev deng-ingestion quickstart --days 2
```

For an incremental run:

```bash
docker compose run --rm pipeline uv run --no-dev deng-ingestion quickstart
```

## Verification

For a step-by-step reviewer path, service login details, and SQL verification queries, see:

* [06 Midterm Verification](docs/06_midterm_verification.md)

## Documentation

Additional project documentation is available in the `docs/` directory:

* [01 Architecture and Use Case](docs/01_architecture_and_use_case.md)
* [02 Data Dictionary](docs/02_data_dictionary.md)
* [03 Orchestration](docs/03_orchestration.md)
* [04 Local Development](docs/04_local_development.md)
* [05 Known Issues and Design Decisions](docs/05_known_issues_and_design_decisions.md)
* [06 Midterm Verification](docs/06_midterm_verification.md)

## Data Source Attribution

This project uses event data from the **GDELT Project**.

This repository is an independent educational project and is not affiliated with or endorsed by the GDELT Project.

For more information, see:

* [https://www.gdeltproject.org/](https://www.gdeltproject.org/)

## License

This project is licensed under the **GNU Affero General Public License v3.0 or later** (**AGPL-3.0-or-later**).

See the [LICENSE](./LICENSE) file for the full license text.
