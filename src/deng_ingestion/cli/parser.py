from __future__ import annotations

import argparse

from deng_ingestion.cli.parsers.export import add_export_parser
from deng_ingestion.cli.parsers.gold import add_gold_parser
from deng_ingestion.cli.parsers.lookups import add_lookups_parser
from deng_ingestion.cli.parsers.manifest import add_manifest_parser
from deng_ingestion.cli.parsers.pipeline import add_pipeline_parser
from deng_ingestion.cli.parsers.quickstart import add_quickstart_parser
from deng_ingestion.cli.parsers.silver import add_silver_parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="deng-ingestion",
        description="Geopolitical risk ingestion pipeline CLI",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    add_manifest_parser(subparsers)
    add_export_parser(subparsers)
    add_lookups_parser(subparsers)
    add_silver_parser(subparsers)
    add_gold_parser(subparsers)
    add_pipeline_parser(subparsers)
    add_quickstart_parser(subparsers)

    return parser
