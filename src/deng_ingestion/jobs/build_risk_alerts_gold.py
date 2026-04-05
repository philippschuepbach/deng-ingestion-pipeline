from __future__ import annotations

from deng_ingestion.pipeline.job import PipelineJob
from deng_ingestion.steps.build_risk_alerts_gold import BuildRiskAlertsGoldStep


def build_risk_alerts_gold_job() -> PipelineJob:
    return PipelineJob(
        name="build_risk_alerts_gold",
        steps=[
            BuildRiskAlertsGoldStep(),
        ],
    )
