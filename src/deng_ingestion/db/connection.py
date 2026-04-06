from __future__ import annotations

import os
from dataclasses import dataclass

import psycopg2
from dotenv import load_dotenv
from loguru import logger
from psycopg2.extensions import connection as PgConnection

from deng_ingestion.core.paths import PROJECT_ROOT
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import get_db_connection

load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    database: str
    username: str
    password: str
    connect_timeout_seconds: int = 10


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


def load_db_config() -> DbConfig:
    return DbConfig(
        host=_get_env("POSTGRES_HOST", "DB_HOST", "PGHOST", default="localhost"),
        port=int(_get_env("POSTGRES_PORT", "DB_PORT", "PGPORT", default="5432")),
        database=_get_env("POSTGRES_DB", "DB_NAME", "PGDATABASE", required=True),
        username=_get_env("POSTGRES_USER", "DB_USERNAME", "PGUSER", required=True),
        password=_get_env(
            "POSTGRES_PASSWORD", "DB_PASSWORD", "PGPASSWORD", required=True
        ),
        connect_timeout_seconds=int(
            _get_env(
                "POSTGRES_CONNECT_TIMEOUT",
                "DB_CONNECT_TIMEOUT",
                default="10",
            )
        ),
    )


def get_connection() -> PgConnection:
    config = load_db_config()

    logger.debug(
        "Opening PostgreSQL connection: host={}, port={}, database={}, username={}",
        config.host,
        config.port,
        config.database,
        config.username,
    )

    return psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.username,
        password=config.password,
        connect_timeout=config.connect_timeout_seconds,
    )


def get_context_connection(context: PipelineContext) -> tuple[PgConnection, bool]:
    existing_connection = get_db_connection(context)
    if existing_connection is not None:
        return existing_connection, False

    return get_connection(), True
