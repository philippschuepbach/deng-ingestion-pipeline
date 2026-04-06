from deng_ingestion.core.paths import SQL_DIR


def load_sql(*parts: str) -> str:
    return (SQL_DIR.joinpath(*parts)).read_text(encoding="utf-8")
