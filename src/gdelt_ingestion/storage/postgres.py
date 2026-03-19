from sqlalchemy import create_engine, text

def create_pg_engine(*, user: str, password: str, host: str, port: int, db: str):
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    )

def ensure_schema(engine, schema: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

