from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

VALID_LOG_LEVELS = {"TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"}


def configure_logging() -> None:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    if log_level not in VALID_LOG_LEVELS:
        raise ValueError(
            f"Invalid LOG_LEVEL={log_level!r}. "
            f"Expected one of: {', '.join(sorted(VALID_LOG_LEVELS))}"
        )

    logger.remove()

    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level:<8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    )

    logger.add(
        sys.stdout,
        level=log_level,
        colorize=True,
        format=log_format,
        filter=lambda record: record["level"].no < logger.level("ERROR").no,
    )

    logger.add(
        sys.stderr,
        level="ERROR",
        colorize=True,
        format=log_format,
    )
