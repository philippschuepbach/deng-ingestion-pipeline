from __future__ import annotations

from dataclasses import dataclass

from deng_ingestion.manifest.parser import parse_manifest_line
from deng_ingestion.pipeline.context import PipelineContext


@dataclass(frozen=True)
class ParseManifestEntriesStep:
    name: str = "parse_manifest_entries"

    def run(self, context: PipelineContext) -> None:
        manifest_text = context.data["manifest_text"]
        entries = []

        for line in manifest_text.splitlines():
            entry = parse_manifest_line(line)
            if entry is not None:
                entries.append(entry)

        context.data["manifest_entries"] = entries
