.PHONY: lint
lint: lock
	uv run --extra dev ruff check
	uv run --extra dev ruff format --check .

.PHONY: fix
fix: lock
	uv run --extra dev ruff check --fix --unsafe-fixes
	uv run --extra dev ruff format .

.PHONY: lock
lock:
	uv lock

.PHONY: build
build: lock
	uv pip install ".[dev,docs]"

.PHONY: test
test: lock
	uv run pytest -vv -s
