FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ./
COPY README.md ./
COPY src ./src
COPY sql ./sql

RUN uv sync --locked --no-dev

CMD ["uv", "run", "--no-dev", "python", "-m", "deng_ingestion.cli.main", "--help"]
