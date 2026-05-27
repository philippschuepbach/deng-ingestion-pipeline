from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import storage  # type: ignore[import-untyped]
from loguru import logger

from deng_ingestion.core.paths import PROJECT_ROOT

load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class ObjectStorageConfig:
    bucket: str
    project: str | None = None


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
    project = _get_env(
        "GOOGLE_CLOUD_PROJECT",
        "GCLOUD_PROJECT",
        default="",
    )

    return ObjectStorageConfig(
        bucket=_get_env(
            "OBJECT_STORAGE_BUCKET",
            "GCS_BUCKET",
            required=True,
        ),
        project=project if project else None,
    )


@lru_cache(maxsize=1)
def _create_storage_client() -> storage.Client:
    config = load_object_storage_config()

    logger.debug(
        "Creating Google Cloud Storage client: project={}, bucket={}",
        config.project,
        config.bucket,
    )

    if config.project is not None:
        return storage.Client(project=config.project)

    return storage.Client()


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
        "Uploading file to Google Cloud Storage: "
        "local_path={}, bucket={}, object_path={}",
        local_path,
        bucket_name,
        normalized_object_path,
    )

    client = _create_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(normalized_object_path)

    blob.upload_from_filename(str(local_path))

    logger.debug(
        "Finished upload to Google Cloud Storage: bucket={}, object_path={}",
        bucket_name,
        normalized_object_path,
    )


def object_exists(object_path: str) -> bool:
    normalized_object_path = object_path.lstrip("/")
    if not normalized_object_path:
        raise ValueError("Expected non-empty object_path for existence check")

    bucket_name = get_bucket_name()
    client = _create_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(normalized_object_path)

    exists = blob.exists(client=client)

    logger.debug(
        "Checked object existence: bucket={}, object_path={}, exists={}",
        bucket_name,
        normalized_object_path,
        exists,
    )

    return bool(exists)
