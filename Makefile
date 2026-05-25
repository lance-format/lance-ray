.PHONY: help
help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  help   Show this help message"
	@echo "  lint   Run linter and format checker (ruff check + ruff format --check)"
	@echo "  fix    Auto-fix lint issues and format code (ruff check --fix + ruff format)"
	@echo "  lock   Update uv.lock lockfile"
	@echo "  build       Install the project with dev and docs dependencies"
	@echo "  build-whl   Build wheel package into dist/ (for dev & test)"
	@echo "  clean       Remove build artifacts and caches"
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

.PHONY: build-whl
build-whl:
	uv build --wheel --no-cache

.PHONY: clean
clean:
	rm -rf build/ dist/ *.egg-info/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
	find . -type d -name .ruff_cache -exec rm -rf {} +
	rm -f .coverage

.PHONY: test
test: lock
	uv run pytest -vv -s