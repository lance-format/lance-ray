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