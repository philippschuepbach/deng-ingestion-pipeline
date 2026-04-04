FROM python:3.12-slim

WORKDIR /app

RUN pip install uv

COPY pyproject.toml ./
COPY README.md ./
COPY src ./src
COPY sql ./sql

RUN uv sync --frozen || uv sync

CMD ["tail", "-f", "/dev/null"]
