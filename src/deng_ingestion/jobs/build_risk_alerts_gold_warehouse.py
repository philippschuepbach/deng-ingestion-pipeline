from __future__ import annotations

from dataclasses import dataclass, field

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.steps.warehouse.build_risk_alerts_gold import (
    BuildRiskAlertsGoldWarehouseStep,
)
from deng_ingestion.steps.warehouse.create_fips_country_codes_external_table import (
    CreateFipsCountryCodesExternalTableStep,
)
from deng_ingestion.steps.warehouse.create_risk_alerts_gold_table import (
    CreateRiskAlertsGoldTableStep,
)


@dataclass(frozen=True)
class BuildRiskAlertsGoldWarehouseJob:
    name: str = "build_risk_alerts_gold_warehouse"

    create_fips_country_table_step: CreateFipsCountryCodesExternalTableStep = field(
        default_factory=CreateFipsCountryCodesExternalTableStep
    )
    create_gold_table_step: CreateRiskAlertsGoldTableStep = field(
        default_factory=CreateRiskAlertsGoldTableStep
    )
    build_gold_step: BuildRiskAlertsGoldWarehouseStep = field(
        default_factory=BuildRiskAlertsGoldWarehouseStep
    )

    def run(self, context: PipelineContext) -> None:
        self.create_fips_country_table_step.run(context)
        self.create_gold_table_step.run(context)
        self.build_gold_step.run(context)


def build_risk_alerts_gold_warehouse_job() -> BuildRiskAlertsGoldWarehouseJob:
    return BuildRiskAlertsGoldWarehouseJob()
