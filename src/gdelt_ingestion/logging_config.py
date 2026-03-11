import os
import sys
from loguru import logger


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
