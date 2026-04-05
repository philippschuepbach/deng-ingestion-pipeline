from __future__ import annotations

from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent
SRC_ROOT = PACKAGE_ROOT.parent
PROJECT_ROOT = SRC_ROOT.parent

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
RAW_ARCHIVES_DIR = RAW_DIR / "archives"
LOOKUPS_DIR = DATA_DIR / "lookups"
SQL_DIR = PROJECT_ROOT / "sql"
