from __future__ import annotations

from argparse import Namespace

from .export import (
    handle_export_ingest,
    handle_export_ingest_all,
    handle_export_ingest_current_run,
)
from .gold import handle_gold_build
from .lookups import handle_lookups_load
from .manifest import handle_manifest_backfill, handle_manifest_incremental
from .pipeline import handle_pipeline_incremental
from .quickstart import handle_quickstart
from .silver import handle_silver_transform, handle_silver_transform_all


def dispatch(args: Namespace) -> None:
    if args.command == "manifest":
        if args.manifest_command == "incremental":
            handle_manifest_incremental(args)
            return

        if args.manifest_command == "backfill":
            handle_manifest_backfill(args)
            return

    if args.command == "export":
        if args.export_command == "ingest":
            handle_export_ingest(args)
            return

        if args.export_command == "ingest-all":
            handle_export_ingest_all(args)
            return

        if args.export_command == "ingest-current-run":
            handle_export_ingest_current_run(args)
            return

    if args.command == "lookups":
        if args.lookups_command == "load":
            handle_lookups_load(args)
            return

    if args.command == "silver":
        if args.silver_command == "transform":
            handle_silver_transform(args)
            return

        if args.silver_command == "transform-all":
            handle_silver_transform_all(args)
            return

    if args.command == "gold":
        if args.gold_command == "build":
            handle_gold_build(args)
            return

    if args.command == "pipeline":
        if args.pipeline_command == "incremental":
            handle_pipeline_incremental(args)
            return

    if args.command == "quickstart":
        handle_quickstart(args)
        return

    raise ValueError(
        "Unsupported CLI command combination: "
        f"command={args.command}, "
        f"manifest_command={getattr(args, 'manifest_command', None)}, "
        f"export_command={getattr(args, 'export_command', None)}, "
        f"lookups_command={getattr(args, 'lookups_command', None)}, "
        f"silver_command={getattr(args, 'silver_command', None)}, "
        f"gold_command={getattr(args, 'gold_command', None)}, "
        f"pipeline_command={getattr(args, 'pipeline_command', None)}"
    )
