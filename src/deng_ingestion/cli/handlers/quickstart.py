from __future__ import annotations

from argparse import Namespace

from loguru import logger

from .common import has_backfill_window
from .export import handle_export_ingest_all
from .gold import handle_gold_build
from .lookups import handle_lookups_load
from .manifest import handle_manifest_backfill, handle_manifest_incremental
from .silver import handle_silver_transform_all


def handle_quickstart(args: Namespace) -> None:
    logger.info("Starting quickstart")

    handle_lookups_load(args)

    if has_backfill_window(args):
        logger.info(
            "Quickstart selected backfill mode: years={}, months={}, days={}",
            args.years,
            args.months,
            args.days,
        )
        handle_manifest_backfill(args)
    else:
        logger.info("Quickstart selected incremental mode")
        handle_manifest_incremental(args)

    handle_export_ingest_all(args)
    handle_silver_transform_all(args)
    handle_gold_build(args)

    logger.info("Finished quickstart")
