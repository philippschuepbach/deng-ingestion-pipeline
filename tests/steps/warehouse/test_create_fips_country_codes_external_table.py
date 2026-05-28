from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.steps.warehouse import (
    create_fips_country_codes_external_table as step_module,
)
from deng_ingestion.steps.warehouse.bigquery_client import BigQueryConfig
from deng_ingestion.steps.warehouse.create_fips_country_codes_external_table import (
    CreateFipsCountryCodesExternalTableStep,
)


def make_context(tmp_path: Path) -> PipelineContext:
    return PipelineContext(
        run_id="test",
        execution_ts=datetime(2026, 4, 8, 12, 0, 0, tzinfo=UTC),
        working_dir=tmp_path,
    )


class FakeBlob:
    def __init__(self, exists: bool) -> None:
        self._exists = exists

    def exists(self, *, client) -> bool:
        return self._exists


class FakeBucket:
    def __init__(self, exists: bool) -> None:
        self.blob_path: str | None = None
        self.exists = exists

    def blob(self, path: str) -> FakeBlob:
        self.blob_path = path
        return FakeBlob(self.exists)


class FakeStorageClient:
    def __init__(self, exists: bool = True) -> None:
        self.bucket_name: str | None = None
        self.bucket_obj = FakeBucket(exists)

    def bucket(self, name: str) -> FakeBucket:
        self.bucket_name = name
        return self.bucket_obj


class FakeBigQueryClient:
    def __init__(self) -> None:
        self.deleted_table_id: str | None = None
        self.deleted_not_found_ok: bool | None = None
        self.created_table = None

    def delete_table(self, table_id: str, *, not_found_ok: bool) -> None:
        self.deleted_table_id = table_id
        self.deleted_not_found_ok = not_found_ok

    def create_table(self, table):
        self.created_table = table
        return table


def test_create_fips_country_codes_external_table_creates_expected_table(
    monkeypatch,
    tmp_path: Path,
) -> None:
    storage_client = FakeStorageClient()
    bigquery_client = FakeBigQueryClient()

    monkeypatch.setattr(
        step_module,
        "load_bigquery_config",
        lambda: BigQueryConfig(
            project="test-project",
            dataset="test_dataset",
            bucket="test-bucket",
        ),
    )
    monkeypatch.setattr(step_module, "get_storage_client", lambda: storage_client)
    monkeypatch.setattr(step_module, "get_bigquery_client", lambda: bigquery_client)

    CreateFipsCountryCodesExternalTableStep().run(make_context(tmp_path))

    assert storage_client.bucket_name == "test-bucket"
    assert storage_client.bucket_obj.blob_path == "raw/gdelt/lookups/FIPS.country.txt"

    table_id = "test-project.test_dataset.dim_fips_country_codes_external"
    assert bigquery_client.deleted_table_id == table_id
    assert bigquery_client.deleted_not_found_ok is True
    assert bigquery_client.created_table is not None
    assert bigquery_client.created_table.project == "test-project"
    assert bigquery_client.created_table.dataset_id == "test_dataset"
    assert bigquery_client.created_table.table_id == "dim_fips_country_codes_external"

    external_config = bigquery_client.created_table.external_data_configuration
    assert external_config.source_uris == [
        "gs://test-bucket/raw/gdelt/lookups/FIPS.country.txt"
    ]
    assert [field.name for field in external_config.schema] == [
        "country_code",
        "country_name",
    ]
    assert external_config.options.field_delimiter == "\t"


def test_create_fips_country_codes_external_table_raises_when_lookup_missing(
    monkeypatch,
    tmp_path: Path,
) -> None:
    storage_client = FakeStorageClient(exists=False)

    monkeypatch.setattr(
        step_module,
        "load_bigquery_config",
        lambda: BigQueryConfig(
            project="test-project",
            dataset="test_dataset",
            bucket="test-bucket",
        ),
    )
    monkeypatch.setattr(step_module, "get_storage_client", lambda: storage_client)

    with pytest.raises(ValueError, match="No FIPS country lookup file found"):
        CreateFipsCountryCodesExternalTableStep().run(make_context(tmp_path))
