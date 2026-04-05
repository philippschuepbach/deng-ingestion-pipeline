#!/bin/sh
set -e

cd /workspace
uv sync --no-dev

exec /app/kestra server standalone
