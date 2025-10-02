# Makefile

.PHONY: dev

dev:
	@if [ ! -d ".venv" ]; then \
		echo "Creating virtual environment..."; \
		uv venv --python 3.12; \
	fi;
	@echo "Installing dependencies...";
	@uv pip sync pyproject.toml;
	@echo "Development environment ready.";