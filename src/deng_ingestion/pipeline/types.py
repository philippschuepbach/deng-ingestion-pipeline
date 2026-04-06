from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from deng_ingestion.pipeline.context import PipelineContext


class PipelineStep(Protocol):
    @property
    def name(self) -> str: ...

    def run(self, context: PipelineContext) -> None: ...


@dataclass(frozen=True)
class StepResult:
    rows_processed: int = 0
    message: str = ""
