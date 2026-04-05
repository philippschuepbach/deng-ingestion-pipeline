from __future__ import annotations

from dataclasses import dataclass
from urllib.request import urlopen

from deng_ingestion.pipeline.context import PipelineContext


@dataclass(frozen=True)
class FetchManifestStep:
    name: str
    manifest_url: str
    source_type: str

    def run(self, context: PipelineContext) -> None:
        with urlopen(self.manifest_url) as response:
            manifest_text = response.read().decode("utf-8")

        context.data["manifest_text"] = manifest_text
        context.data["manifest_source_type"] = self.source_type
