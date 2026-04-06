.PHONY: format lint typecheck test check fix precommit

format:
	uv run black src

lint:
	uv run ruff check src

typecheck:
	uv run mypy src

test:
	uv run pytest

check: lint typecheck test

fix:
	uv run ruff check src --fix
	uv run black src

precommit:
	uv run pre-commit run --all-files
