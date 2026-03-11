# tests/test_logging_config.py
from gdelt_ingestion import configure_logging
from loguru import logger


def test_configure_logging_is_idempotent():
    configure_logging("INFO")
    n1 = len(logger._core.handlers)  # internal, but stable enough for a smoke test

    configure_logging("INFO")
    n2 = len(logger._core.handlers)

    assert n1 == n2 == 1
