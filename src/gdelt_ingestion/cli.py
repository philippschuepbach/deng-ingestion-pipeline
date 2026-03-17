from argparse import ArgumentParser, Namespace
from pathlib import Path

from loguru import logger

from gdelt_ingestion.logging_config import load_settings
from gdelt_ingestion.funnel import fetch_latest_gdelt
from gdelt_ingestion.logging_config import configure_logging
from gdelt_ingestion.jobs.fetch_lookups import run as run_fetch_lookups
from gdelt_ingestion.constants import GDELT_LAST_UPDATE_URL


def build_parser() -> ArgumentParser:
    """Build the command-line argument parser for the GDELT ingest CLI."""
    parser = ArgumentParser(description="GDELT Ingest CLI")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # ---- fetch-latest ----
    latest_parser = subparsers.add_parser(
        "fetch-latest",
        help="Download and extract the latest GDELT export file.",
    )

    latest_parser.add_argument(
        "--last-update-url",
        default=GDELT_LAST_UPDATE_URL,
        help="URL to lastupdate.txt",
    )

    latest_parser.add_argument(
        "--raw-data-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directory for raw GDELT files",
    )

    # ---- fetch-lookups ----
    lookup_parser = subparsers.add_parser(
        "fetch-lookups",
        help="Download all GDELT lookup tables.",
    )

    lookup_parser.add_argument(
        "--lookup-dir",
        type=Path,
        default=Path("data/lookups"),
        help="Directory for lookup tables",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args: Namespace = parser.parse_args()

    configure_logging()
    logger.info("Starting GDELT ingest CLI")
    logger.info("Selected command: {}", args.command)

    settings = load_settings()
    logger.debug("Settings loaded successfully")

    # ---- dispatch ----

    if args.command == "fetch-latest":
        csv_path = fetch_latest_gdelt(
            last_update_url=args.last_update_url,
            raw_data_dir=args.raw_data_dir,
        )

        logger.success("Latest GDELT file ready at: {}", csv_path)

    elif args.command == "fetch-lookups":
        paths = run_fetch_lookups(dest_dir=args.lookup_dir)

        logger.success("Lookup files downloaded to {}", args.lookup_dir)

    else:
        parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()