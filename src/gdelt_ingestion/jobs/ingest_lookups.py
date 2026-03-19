from __future__ import annotations

from pathlib import Path

from loguru import logger

from ..gdelt.lookup import LOOKUP_SPECS, read_lookup_file
from ..storage.lookups import replace_lookup_table
from ..storage.postgres import create_pg_engine, ensure_schema


def run(
    *,
    lookup_dir: Path,
    pg_user: str,
    pg_pass: str,
    pg_host: str,
    pg_port: int,
    pg_db: str,
    schema: str = "lookup",
) -> None:
    """Read downloaded GDELT lookup files from disk and load them into Postgres."""
    engine = create_pg_engine(
        user=pg_user,
        password=pg_pass,
        host=pg_host,
        port=pg_port,
        db=pg_db,
    )

    processed = 0
    skipped = 0

    try:
        ensure_schema(engine, schema=schema)

        for spec in LOOKUP_SPECS.values():
            path = lookup_dir / spec.filename

            if not path.exists():
                logger.warning("Lookup file not found, skipping: {}", path)
                skipped += 1
                continue

            logger.info("Reading lookup file: {}", path.name)
            df = read_lookup_file(path, spec)

            if df.empty:
                logger.warning("Lookup file is empty after parsing, skipping: {}", path.name)
                skipped += 1
                continue

            logger.info(
                "Writing {} rows to {}.{}",
                len(df),
                schema,
                spec.table_name,
            )

            replace_lookup_table(
                df=df,
                engine=engine,
                schema=schema,
                table_name=spec.table_name,
            )

            processed += 1
            logger.success(
                "Loaded lookup file {} into {}.{}",
                path.name,
                schema,
                spec.table_name,
            )

        logger.success(
            "Finished ingesting lookup files. processed={}, skipped={}",
            processed,
            skipped,
        )
    finally:
        engine.dispose()