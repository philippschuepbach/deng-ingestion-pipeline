FROM python:3.12-slim@sha256:5072b08ad74609c5329ab4085a96dfa873de565fb4751a4cfcd7dcc427661df0
COPY --from=ghcr.io/astral-sh/uv:latest@sha256:90bbb3c16635e9627f49eec6539f956d70746c409209041800a0280b93152823 /uv /bin/


WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ./
COPY README.md ./
COPY src ./src
COPY sql ./sql

RUN uv sync --locked --no-dev

CMD ["uv", "run", "--no-dev", "python", "-m", "deng_ingestion.cli.main", "--help"]
