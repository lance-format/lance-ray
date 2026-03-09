.PHONY: lint
lint: lock
	uv run --with ruff ruff check
	uv run --with ruff ruff format --check .

.PHONY: fix
fix: lock
	uv run --with ruff ruff check --fix --unsafe-fixes
	uv run --with ruff ruff format .

.PHONY: lock
lock:
	uv lock

.PHONY: build
build: lock
	uv pip install ".[dev,docs]"

.PHONY: test
test: lock
	uv run pytest -vv -s
