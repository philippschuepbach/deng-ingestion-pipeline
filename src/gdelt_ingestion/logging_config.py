from __future__ import annotations
import os
import sys
from loguru import logger

from dataclasses import dataclass
from pathlib import Path


def configure_logging(level: str | None = None) -> None:
    """Configure Loguru to log to stderr (container-friendly).

    Idempotent: safe to call multiple times (replaces existing handlers).
    """
    log_level = level or os.getenv("LOG_LEVEL", "INFO")

    # Ensure we don't end up with duplicate log handlers if called again.
    logger.remove()

    logger.add(
        sys.stderr,
        level=log_level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
    )

@dataclass
class Settings:
    raw_data_dir: Path = Path("data/raw")
    lookup_dir: Path = Path("data/lookups")


def load_settings() -> Settings:
    """Load application settings."""
    return Settings()