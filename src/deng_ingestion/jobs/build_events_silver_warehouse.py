from __future__ import annotations

from dataclasses import dataclass, field

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.steps.warehouse.create_bronze_events_external_table import (
    CreateBronzeEventsExternalTableStep,
)
from deng_ingestion.steps.warehouse.create_events_silver_table import (
    CreateEventsSilverTableStep,
)
from deng_ingestion.steps.warehouse.merge_events_silver import MergeEventsSilverStep


@dataclass(frozen=True)
class BuildEventsSilverWarehouseJob:
    name: str = "build_events_silver_warehouse"

    create_external_table_step: CreateBronzeEventsExternalTableStep = field(
        default_factory=CreateBronzeEventsExternalTableStep
    )
    create_silver_table_step: CreateEventsSilverTableStep = field(
        default_factory=CreateEventsSilverTableStep
    )
    merge_silver_step: MergeEventsSilverStep = field(
        default_factory=MergeEventsSilverStep
    )

    def run(self, context: PipelineContext) -> None:
        self.create_external_table_step.run(context)
        self.create_silver_table_step.run(context)
        self.merge_silver_step.run(context)


def build_events_silver_warehouse_job() -> BuildEventsSilverWarehouseJob:
    return BuildEventsSilverWarehouseJob()
