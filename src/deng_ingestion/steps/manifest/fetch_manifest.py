from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.core.http import fetch_text
from deng_ingestion.pipeline.context import PipelineContext


@dataclass(frozen=True)
class FetchManifestStep:
    name: str
    manifest_url: str
    source_type: str

    def run(self, context: PipelineContext) -> None:
        logger.info(
            "Fetching manifest: source_type={}, url={}",
            self.source_type,
            self.manifest_url,
        )

        manifest_text = fetch_text(
            self.manifest_url,
            timeout_seconds=15.0,
            retries=3,
        )

        context.data["manifest_text"] = manifest_text
        context.data["manifest_source_type"] = self.source_type

        logger.info(
            "Fetched manifest successfully: source_type={}, bytes={}",
            self.source_type,
            len(manifest_text.encode("utf-8")),
        )
