.PHONY: lint
lint: lock
	uv run --no-project --with 'ruff==0.12.1' ruff check
	uv run --no-project --with 'ruff==0.12.1' ruff format --check .

.PHONY: fix
fix: lock
	uv run --no-project --with 'ruff==0.12.1' ruff check --fix --unsafe-fixes
	uv run --no-project --with 'ruff==0.12.1' ruff format .

.PHONY: lock
lock:
	uv lock

.PHONY: build
build: lock
	uv pip install ".[dev,docs]"

.PHONY: test
test: lock
	uv run pytest -vv -s
