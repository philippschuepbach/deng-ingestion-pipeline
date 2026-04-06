from __future__ import annotations

from dataclasses import dataclass

from deng_ingestion.manifest.parser import parse_manifest_line
from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    get_manifest_text,
    set_manifest_entries,
)


@dataclass(frozen=True)
class ParseManifestEntriesStep:
    name: str = "parse_manifest_entries"

    def run(self, context: PipelineContext) -> None:
        manifest_text = get_manifest_text(context)
        if manifest_text is None:
            raise ValueError("Expected manifest text in pipeline context")

        entries = []

        for line in manifest_text.splitlines():
            entry = parse_manifest_line(line)
            if entry is not None:
                entries.append(entry)

        set_manifest_entries(context, entries)
