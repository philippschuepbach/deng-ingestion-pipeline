from __future__ import annotations

from argparse import ArgumentParser, Namespace
from pathlib import Path

from loguru import logger

from gdelt_ingestion.cli_args import add_relative_time_args
from gdelt_ingestion.constants import GDELT_LAST_UPDATE_URL
from gdelt_ingestion.gdelt.masterlist import (
    iter_urls_in_range_from_file,
    iter_urls_in_range_from_url,
)
from gdelt_ingestion.jobs.fetch_latest import fetch_latest_gdelt
from gdelt_ingestion.jobs.fetch_lookups import run as run_fetch_lookups
from gdelt_ingestion.jobs.ingest_lookups import run as run_ingest_lookups
from gdelt_ingestion.logging_config import configure_logging, load_settings
from gdelt_ingestion.time_window import resolve_relative_window
from gdelt_ingestion.jobs.fetch_exports_from_masterlist import (
    run as run_fetch_exports_from_masterlist,
)


def add_postgres_args(parser: ArgumentParser) -> None:
    parser.add_argument(
        "--pg-user",
        default="root",
        help="PostgreSQL user",
    )
    parser.add_argument(
        "--pg-pass",
        default="root",
        help="PostgreSQL password",
    )
    parser.add_argument(
        "--pg-host",
        default="localhost",
        help="PostgreSQL host",
    )
    parser.add_argument(
        "--pg-port",
        type=int,
        default=5432,
        help="PostgreSQL port",
    )
    parser.add_argument(
        "--pg-db",
        default="gdelt",
        help="PostgreSQL database name",
    )
    parser.add_argument(
        "--schema",
        default="lookup",
        help="Target schema for lookup tables",
    )


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

    # ---- ingest-lookups ----
    ingest_lookup_parser = subparsers.add_parser(
        "ingest-lookups",
        help="Load downloaded GDELT lookup files into PostgreSQL.",
    )
    ingest_lookup_parser.add_argument(
        "--lookup-dir",
        type=Path,
        default=Path("data/lookups"),
        help="Directory containing downloaded lookup tables",
    )
    add_postgres_args(ingest_lookup_parser)

    # ---- sync-lookups ----
    sync_lookup_parser = subparsers.add_parser(
        "sync-lookups",
        help="Download all GDELT lookup tables and ingest them into PostgreSQL.",
    )
    sync_lookup_parser.add_argument(
        "--lookup-dir",
        type=Path,
        default=Path("data/lookups"),
        help="Directory for lookup tables",
    )
    add_postgres_args(sync_lookup_parser)

    # ---- list-master-urls ----
    master_parser = subparsers.add_parser(
        "list-master-urls",
        help="List GDELT file URLs from a master list for a relative time window.",
    )

    master_source_group = master_parser.add_mutually_exclusive_group(required=True)
    master_source_group.add_argument(
        "--master-url",
        help="Remote URL of the GDELT master list",
    )
    master_source_group.add_argument(
        "--master-file",
        type=Path,
        help="Local file path of a previously downloaded GDELT master list",
    )

    master_parser.add_argument(
        "--file-type",
        action="append",
        choices=["export", "mentions", "gkg"],
        help=(
            "GDELT file type to include. Can be repeated, e.g. "
            "--file-type export --file-type gkg. "
            "Defaults to all file types if omitted."
        ),
    )
    add_relative_time_args(master_parser)

    fetch_exports_parser = subparsers.add_parser(
        "fetch-exports-from-masterlist",
        help="Download and extract GDELT export files from a master list for a relative time window.",
    )

    master_source_group = fetch_exports_parser.add_mutually_exclusive_group(
        required=True
    )
    master_source_group.add_argument(
        "--master-url",
        help="Remote URL of the GDELT master list",
    )
    master_source_group.add_argument(
        "--master-file",
        type=Path,
        help="Local file path of a previously downloaded GDELT master list",
    )

    fetch_exports_parser.add_argument(
        "--raw-data-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directory for downloaded and extracted GDELT export files",
    )
    fetch_exports_parser.add_argument(
        "--delete-zip",
        action="store_true",
        help="Delete ZIP files after successful extraction",
    )
    fetch_exports_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing ZIP and extracted files",
    )
    add_relative_time_args(fetch_exports_parser)

    return parser


def main() -> None:
    parser = build_parser()
    args: Namespace = parser.parse_args()

    configure_logging()
    logger.info("Starting GDELT ingest CLI")
    logger.info("Selected command: {}", args.command)

    settings = load_settings()
    logger.debug("Settings loaded successfully: {}", settings)

    # ---- dispatch ----
    if args.command == "fetch-latest":
        csv_path = fetch_latest_gdelt(
            last_update_url=args.last_update_url,
            raw_data_dir=args.raw_data_dir,
        )
        logger.success("Latest GDELT file ready at: {}", csv_path)

    elif args.command == "fetch-lookups":
        paths = run_fetch_lookups(dest_dir=args.lookup_dir)
        logger.success("Downloaded {} lookup files to {}", len(paths), args.lookup_dir)

    elif args.command == "ingest-lookups":
        run_ingest_lookups(
            lookup_dir=args.lookup_dir,
            pg_user=args.pg_user,
            pg_pass=args.pg_pass,
            pg_host=args.pg_host,
            pg_port=args.pg_port,
            pg_db=args.pg_db,
            schema=args.schema,
        )
        logger.success("Lookup files ingested into PostgreSQL")

    elif args.command == "sync-lookups":
        paths = run_fetch_lookups(dest_dir=args.lookup_dir)
        logger.info("Downloaded {} lookup files to {}", len(paths), args.lookup_dir)

        run_ingest_lookups(
            lookup_dir=args.lookup_dir,
            pg_user=args.pg_user,
            pg_pass=args.pg_pass,
            pg_host=args.pg_host,
            pg_port=args.pg_port,
            pg_db=args.pg_db,
            schema=args.schema,
        )
        logger.success("Lookup files downloaded and ingested successfully")

    elif args.command == "list-master-urls":
        start_ts, end_ts = resolve_relative_window(
            years=args.years,
            months=args.months,
            days=args.days,
        )

        logger.info(
            "Resolved relative time window to {} .. {} (UTC)",
            start_ts,
            end_ts,
        )

        if args.master_url:
            urls = iter_urls_in_range_from_url(
                master_url=args.master_url,
                start_ts=start_ts,
                end_ts=end_ts,
                file_types=args.file_type,
            )
        else:
            urls = iter_urls_in_range_from_file(
                master_file=args.master_file,
                start_ts=start_ts,
                end_ts=end_ts,
                file_types=args.file_type,
            )

        count = 0
        for url in urls:
            print(url)
            count += 1

        logger.success("Found {} matching GDELT file URL(s)", count)

    elif args.command == "fetch-exports-from-masterlist":
        start_ts, end_ts = resolve_relative_window(
            years=args.years,
            months=args.months,
            days=args.days,
        )

        paths = run_fetch_exports_from_masterlist(
            start_ts=start_ts,
            end_ts=end_ts,
            raw_data_dir=args.raw_data_dir,
            master_url=args.master_url,
            master_file=args.master_file,
            delete_zip=args.delete_zip,
            overwrite=args.overwrite,
        )

        logger.success("Fetched {} export file(s)", len(paths))

    else:
        parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
