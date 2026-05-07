from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import boto3  # type: ignore[import-untyped]
from dotenv import load_dotenv
from loguru import logger

from deng_ingestion.core.paths import PROJECT_ROOT

load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class ObjectStorageConfig:
    bucket: str
    endpoint_url: str | None
    region: str
    access_key: str
    secret_key: str


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


def load_object_storage_config() -> ObjectStorageConfig:
    endpoint_url = _get_env(
        "OBJECT_STORAGE_ENDPOINT_URL",
        "S3_ENDPOINT_URL",
        default="",
    )

    return ObjectStorageConfig(
        bucket=_get_env(
            "OBJECT_STORAGE_BUCKET",
            "S3_BUCKET",
            required=True,
        ),
        endpoint_url=endpoint_url if endpoint_url else None,
        region=_get_env(
            "OBJECT_STORAGE_REGION",
            "AWS_DEFAULT_REGION",
            "AWS_REGION",
            default="eu-central-1",
        ),
        access_key=_get_env(
            "OBJECT_STORAGE_ACCESS_KEY",
            "AWS_ACCESS_KEY_ID",
            required=True,
        ),
        secret_key=_get_env(
            "OBJECT_STORAGE_SECRET_KEY",
            "AWS_SECRET_ACCESS_KEY",
            required=True,
        ),
    )


@lru_cache(maxsize=1)
def _create_s3_client():
    config = load_object_storage_config()

    logger.debug(
        "Creating object storage client: endpoint_url={}, region={}, bucket={}",
        config.endpoint_url,
        config.region,
        config.bucket,
    )

    return boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        region_name=config.region,
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
    )


def get_bucket_name() -> str:
    return load_object_storage_config().bucket


def upload_file(local_path: Path, object_path: str) -> None:
    if not local_path.exists():
        raise ValueError(f"Missing local file for upload: {local_path}")

    if not local_path.is_file():
        raise ValueError(f"Local upload path is not a file: {local_path}")

    normalized_object_path = object_path.lstrip("/")
    if not normalized_object_path:
        raise ValueError("Expected non-empty object_path for upload")

    bucket_name = get_bucket_name()

    logger.debug(
        "Uploading file to object storage: local_path={}, bucket={}, object_path={}",
        local_path,
        bucket_name,
        normalized_object_path,
    )

    client = _create_s3_client()
    client.upload_file(
        Filename=str(local_path),
        Bucket=bucket_name,
        Key=normalized_object_path,
    )

    logger.debug(
        "Finished upload to object storage: bucket={}, object_path={}",
        bucket_name,
        normalized_object_path,
    )
