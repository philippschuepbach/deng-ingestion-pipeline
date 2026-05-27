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

BRONZE_EXTERNAL_SCHEMA = [
    bigquery.SchemaField("global_event_id", "STRING"),
    bigquery.SchemaField("sql_date", "STRING"),
    bigquery.SchemaField("month_year", "STRING"),
    bigquery.SchemaField("year", "STRING"),
    bigquery.SchemaField("fraction_date", "STRING"),
    bigquery.SchemaField("actor1_code", "STRING"),
    bigquery.SchemaField("actor1_name", "STRING"),
    bigquery.SchemaField("actor1_country_code", "STRING"),
    bigquery.SchemaField("actor1_known_group_code", "STRING"),
    bigquery.SchemaField("actor1_ethnic_code", "STRING"),
    bigquery.SchemaField("actor1_religion1_code", "STRING"),
    bigquery.SchemaField("actor1_religion2_code", "STRING"),
    bigquery.SchemaField("actor1_type1_code", "STRING"),
    bigquery.SchemaField("actor1_type2_code", "STRING"),
    bigquery.SchemaField("actor1_type3_code", "STRING"),
    bigquery.SchemaField("actor2_code", "STRING"),
    bigquery.SchemaField("actor2_name", "STRING"),
    bigquery.SchemaField("actor2_country_code", "STRING"),
    bigquery.SchemaField("actor2_known_group_code", "STRING"),
    bigquery.SchemaField("actor2_ethnic_code", "STRING"),
    bigquery.SchemaField("actor2_religion1_code", "STRING"),
    bigquery.SchemaField("actor2_religion2_code", "STRING"),
    bigquery.SchemaField("actor2_type1_code", "STRING"),
    bigquery.SchemaField("actor2_type2_code", "STRING"),
    bigquery.SchemaField("actor2_type3_code", "STRING"),
    bigquery.SchemaField("is_root_event", "STRING"),
    bigquery.SchemaField("event_code", "STRING"),
    bigquery.SchemaField("event_base_code", "STRING"),
    bigquery.SchemaField("event_root_code", "STRING"),
    bigquery.SchemaField("quad_class", "STRING"),
    bigquery.SchemaField("goldstein_scale", "STRING"),
    bigquery.SchemaField("num_mentions", "STRING"),
    bigquery.SchemaField("num_sources", "STRING"),
    bigquery.SchemaField("num_articles", "STRING"),
    bigquery.SchemaField("avg_tone", "STRING"),
    bigquery.SchemaField("actor1_geo_type", "STRING"),
    bigquery.SchemaField("actor1_geo_fullname", "STRING"),
    bigquery.SchemaField("actor1_geo_country_code", "STRING"),
    bigquery.SchemaField("actor1_geo_adm1_code", "STRING"),
    bigquery.SchemaField("actor1_geo_adm2_code", "STRING"),
    bigquery.SchemaField("actor1_geo_lat", "STRING"),
    bigquery.SchemaField("actor1_geo_long", "STRING"),
    bigquery.SchemaField("actor1_geo_feature_id", "STRING"),
    bigquery.SchemaField("actor2_geo_type", "STRING"),
    bigquery.SchemaField("actor2_geo_fullname", "STRING"),
    bigquery.SchemaField("actor2_geo_country_code", "STRING"),
    bigquery.SchemaField("actor2_geo_adm1_code", "STRING"),
    bigquery.SchemaField("actor2_geo_adm2_code", "STRING"),
    bigquery.SchemaField("actor2_geo_lat", "STRING"),
    bigquery.SchemaField("actor2_geo_long", "STRING"),
    bigquery.SchemaField("actor2_geo_feature_id", "STRING"),
    bigquery.SchemaField("action_geo_type", "STRING"),
    bigquery.SchemaField("action_geo_fullname", "STRING"),
    bigquery.SchemaField("action_geo_country_code", "STRING"),
    bigquery.SchemaField("action_geo_adm1_code", "STRING"),
    bigquery.SchemaField("action_geo_adm2_code", "STRING"),
    bigquery.SchemaField("action_geo_lat", "STRING"),
    bigquery.SchemaField("action_geo_long", "STRING"),
    bigquery.SchemaField("action_geo_feature_id", "STRING"),
    bigquery.SchemaField("date_added", "STRING"),
    bigquery.SchemaField("source_url", "STRING"),
]


@dataclass(frozen=True)
class CreateBronzeEventsExternalTableStep:
    name: str = "create_bronze_events_external_table"

    def run(self, context: PipelineContext) -> None:
        config = load_bigquery_config()

        storage_client = get_storage_client()
        blobs = storage_client.list_blobs(
            config.bucket,
            prefix=config.bronze_prefix,
        )

        source_uris = [
            f"gs://{config.bucket}/{blob.name}"
            for blob in blobs
            if blob.name.endswith(".CSV")
        ]

        if not source_uris:
            raise ValueError(
                "No bronze event CSV files found in GCS prefix: "
                f"gs://{config.bucket}/{config.bronze_prefix}"
            )

        table_id = f"{config.project}.{config.dataset}.{config.external_table}"

        external_config = bigquery.ExternalConfig("CSV")
        external_config.source_uris = source_uris
        external_config.schema = BRONZE_EXTERNAL_SCHEMA
        external_config.ignore_unknown_values = True

        csv_options = cast(bigquery.CSVOptions, external_config.options)
        csv_options.field_delimiter = "\t"
        csv_options.skip_leading_rows = 0
        csv_options.allow_quoted_newlines = True

        table = bigquery.Table(table_id)
        table.external_data_configuration = external_config

        client = get_bigquery_client()
        client.delete_table(table_id, not_found_ok=True)
        client.create_table(table)

        logger.info(
            "Created bronze external table: table_id={}, source_uri_count={}",
            table_id,
            len(source_uris),
        )
