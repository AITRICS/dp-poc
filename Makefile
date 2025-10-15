# Makefile

.PHONY: dev install lint format type-check pre-commit test run test-benchmark

dev:
	@if [ ! -d ".venv" ]; then \
		echo "Creating virtual environment..."; \
		uv venv --python 3.12; \
	fi;
	@echo "Installing dependencies...";
	@uv sync --extra dev;
	@echo "Development environment ready.";

install:
	@echo "Installing dependencies...";
	@uv sync --extra dev;

lint:
	@echo "Running ruff linter...";
	@uv run ruff check . --fix;

format:
	@echo "Running ruff formatter...";
	@uv run ruff format .;

type-check:
	@echo "Running mypy type checker...";
	@uv run mypy .;

pre-commit:
	@echo "Running pre-commit hooks...";
	@uv run pre-commit run --all-files;

pre-commit-install:
	@echo "Installing pre-commit hooks...";
	@uv run pre-commit install;

test:
	@echo "Running tests...";
	@uv run pytest -v;

test-cov:
	@echo "Running tests with coverage...";
	@uv run pytest -v --cov --cov-report=term-missing

test-unit:
	@echo "Running unit tests...";
	@uv run pytest -v -m unit;

test-integration:
	@echo "Running integration tests...";
	@uv run pytest -v -m integration;

test-benchmark:
	@echo "Running benchmark tests...";
	@uv run pytest -v -m benchmark;

run:
	@echo "Running main application...";
	@uv run python -m app.main;
