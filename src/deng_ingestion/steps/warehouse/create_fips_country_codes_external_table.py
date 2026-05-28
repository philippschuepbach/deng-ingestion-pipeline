from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from google.cloud import bigquery  # type: ignore[import-untyped]
from loguru import logger

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.steps.warehouse.bigquery_client import (
    get_bigquery_client,
    get_storage_client,
    load_bigquery_config,
)

FIPS_COUNTRY_SCHEMA = [
    bigquery.SchemaField("country_code", "STRING"),
    bigquery.SchemaField("country_name", "STRING"),
]


@dataclass(frozen=True)
class CreateFipsCountryCodesExternalTableStep:
    name: str = "create_fips_country_codes_external_table"

    def run(self, context: PipelineContext) -> None:
        config = load_bigquery_config()
        object_path = "raw/gdelt/lookups/FIPS.country.txt"

        storage_client = get_storage_client()
        bucket = storage_client.bucket(config.bucket)
        blob = bucket.blob(object_path)

        if not blob.exists(client=storage_client):
            raise ValueError(
                "No FIPS country lookup file found in GCS: "
                f"gs://{config.bucket}/{object_path}"
            )

        table_id = (
            f"{config.project}.{config.dataset}."
            f"{config.fips_country_external_table}"
        )

        external_config = bigquery.ExternalConfig("CSV")
        external_config.source_uris = [f"gs://{config.bucket}/{object_path}"]
        external_config.schema = FIPS_COUNTRY_SCHEMA

        csv_options = cast(bigquery.CSVOptions, external_config.options)
        csv_options.field_delimiter = "\t"
        csv_options.skip_leading_rows = 0

        table = bigquery.Table(table_id)
        table.external_data_configuration = external_config

        client = get_bigquery_client()
        client.delete_table(table_id, not_found_ok=True)
        client.create_table(table)

        logger.info(
            "Created FIPS country external table: table_id={}, source_uri={}",
            table_id,
            external_config.source_uris[0],
        )
