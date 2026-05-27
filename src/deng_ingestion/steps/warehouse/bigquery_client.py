from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import google.cloud.bigquery as bigquery
import google.cloud.storage as storage
from dotenv import load_dotenv
from loguru import logger

from deng_ingestion.core.paths import PROJECT_ROOT

load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class BigQueryConfig:
    project: str
    dataset: str
    bucket: str
    bronze_prefix: str = "bronze/gdelt/export/events/"
    external_table: str = "events_bronze_external"
    silver_table: str = "events_silver"


def _get_env(*names: str, default: str | None = None, required: bool = False) -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and value != "":
            return value

    if default is not None:
        return default

    if required:
        joined_names = ", ".join(names)
        raise ValueError(
            f"Missing required environment variable. Expected one of: {joined_names}"
        )

    raise ValueError("Environment variable lookup failed unexpectedly")


def load_bigquery_config() -> BigQueryConfig:
    return BigQueryConfig(
        project=_get_env(
            "GOOGLE_CLOUD_PROJECT",
            "GCLOUD_PROJECT",
            "GCP_PROJECT",
            required=True,
        ),
        dataset=_get_env(
            "BIGQUERY_DATASET",
            "BQ_DATASET",
            required=True,
        ),
        bucket=_get_env(
            "OBJECT_STORAGE_BUCKET",
            "GCS_BUCKET",
            required=True,
        ),
    )


@lru_cache(maxsize=1)
def get_bigquery_client() -> bigquery.Client:
    config = load_bigquery_config()

    logger.debug(
        "Creating BigQuery client: project={}, dataset={}",
        config.project,
        config.dataset,
    )

    return bigquery.Client(project=config.project)


@lru_cache(maxsize=1)
def get_storage_client() -> storage.Client:
    config = load_bigquery_config()

    logger.debug(
        "Creating Storage client for warehouse pipeline: project={}, bucket={}",
        config.project,
        config.bucket,
    )

    return storage.Client(project=config.project)


def render_sql(sql: str, config: BigQueryConfig) -> str:
    return (
        sql.replace("{{PROJECT_ID}}", config.project)
        .replace("{{BIGQUERY_DATASET}}", config.dataset)
        .replace("{{OBJECT_STORAGE_BUCKET}}", config.bucket)
    )


def run_sql_file(path: Path) -> None:
    config = load_bigquery_config()
    client = get_bigquery_client()

    sql = render_sql(path.read_text(encoding="utf-8"), config)

    logger.info("Running BigQuery SQL file: path={}", path)

    job = client.query(sql)
    job.result()

    logger.debug("Finished BigQuery SQL file: path={}", path)
