from __future__ import annotations

from typing import Protocol
from dataclasses import dataclass

from deng_ingestion.pipeline.context import PipelineContext


class PipelineStep(Protocol):
    name: str

    def run(self, context: PipelineContext) -> None: ...


@dataclass(frozen=True)
class StepResult:
    rows_processed: int = 0
    message: str = ""
