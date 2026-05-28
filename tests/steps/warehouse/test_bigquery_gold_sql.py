from __future__ import annotations

from deng_ingestion.core.paths import PROJECT_ROOT


def test_bigquery_gold_table_is_partitioned_and_clustered() -> None:
    sql = (PROJECT_ROOT / "sql" / "bigquery" / "create_risk_alerts_gold.sql").read_text(
        encoding="utf-8"
    )

    assert "PARTITION BY DATE(time_window_start)" in sql
    assert "CLUSTER BY country_code, is_alert" in sql


def test_bigquery_gold_build_uses_silver_fips_lookup_and_baseline_score() -> None:
    sql = (PROJECT_ROOT / "sql" / "bigquery" / "build_risk_alerts_gold.sql").read_text(
        encoding="utf-8"
    )

    assert "events_silver" in sql
    assert "dim_fips_country_codes_external" in sql
    assert "STDDEV_SAMP(negative_goldstein_sum)" in sql
    assert "weighted_instability_score" in sql
