.PHONY: help
help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  help   Show this help message"
	@echo "  lint   Run linter and format checker (ruff check + ruff format --check)"
	@echo "  fix    Auto-fix lint issues and format code (ruff check --fix + ruff format)"
	@echo "  lock   Update uv.lock lockfile"
	@echo "  build  Install the project with dev and docs dependencies"
	@echo "  test   Run pytest with verbose output"

.PHONY: lint
lint: lock
	uv run ruff check
	uv run ruff format --check .

.PHONY: fix
fix: lock
	uv run ruff check --fix --unsafe-fixes
	uv run ruff format .

.PHONY: lock
lock:
	uv lock

.PHONY: build
build: lock
	uv pip install ".[dev,docs]"

.PHONY: test
test: lock
	uv run pytest -vv -s