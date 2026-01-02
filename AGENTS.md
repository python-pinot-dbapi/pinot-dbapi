# AGENTS.md

This repository contains **`pinotdb`**, a Python DB-API 2.0 client and **SQLAlchemy dialect** for querying Apache Pinot via its SQL API.

## Project layout

- **DB-API implementation**: `pinotdb/db.py`
  - Entry points: `pinotdb.connect(...)`, `pinotdb.connect_async(...)`
  - Uses **`httpx`** to POST SQL queries to the broker (default path `/query/sql`).
- **SQLAlchemy dialect**: `pinotdb/sqlalchemy.py`
  - Dialects: `PinotHTTPDialect`, `PinotHTTPSDialect`, plus multi-stage variants.
  - Uses **`requests`** to fetch metadata from the Pinot **controller** (e.g. `/tables`, `/tables/<table>/schema`).
- **Tests**:
  - Unit tests: `tests/unit/`
  - Integration tests: `tests/integration/` (typically require a running Pinot quickstart)
- **Examples**: `examples/`

## Supported Python versions

Declared in `pyproject.toml`: **Python `>=3.10,<4`**.

## Local development (recommended)

This repo uses **Poetry** for dependency management and **tox** for running tests across multiple Python versions.

- **Install deps (dev + extras)**:

```bash
make init
```

- **Run tests**:

```bash
make test
```

- **Run unit tests only (fast)**:

```bash
make test-unit
```

- **Run integration tests only**:

```bash
make test-integration
```

- **Lint**:

```bash
make lint
```

## Integration testing with Pinot quickstart (Docker)

The Make target `run-pinot` starts a Pinot quickstart container:

```bash
make run-pinot
```

Notes:
- The container is named **`pinot-quickstart`** and exposes ports **2123**, **9000** (controller), **8000** (broker).
- Run `make run-pinot` in one terminal, then run `make test-integration` in another. `make test-integration` will fail fast if Pinot isn't reachable on `PINOT_HOST:PINOT_BROKER_PORT` and `PINOT_HOST:PINOT_CONTROLLER_PORT` (defaults: `localhost:8000` and `localhost:9000`).
- If you need to restart cleanly, you may have to stop/remove the container (e.g. `docker rm -f pinot-quickstart`).

## tox

To run the test suite under tox (multi-Python):

```bash
tox
```

`tox.ini` installs Poetry and runs `poetry install --all-extras` followed by `poetry run pytest`.

## Contribution guidance (for coding agents)

- **Prefer small, backwards-compatible changes**: this package is a DB-API surface + SQLAlchemy dialect, so parameter names and behavior are hard to change without breaking users.
- **Add/adjust tests**:
  - Pure logic → `tests/unit/`
  - End-to-end behavior against Pinot → `tests/integration/` (requires quickstart)
- **Keep style consistent**: run `make lint` and keep new code `flake8`-clean.
- **Be explicit about broker/controller behavior**:
  - Broker queries go through `httpx` in `pinotdb/db.py`
  - Controller metadata goes through `requests` in `pinotdb/sqlalchemy.py`

## Release (maintainers)

From `README.md`:

- **Configure PyPI token** (one-time):

```bash
poetry config pypi-token.pypi <token>
```

- **Bump version**:

```bash
poetry version patch
```

- **Build and publish**:

```bash
poetry build
poetry publish
```

There is also a GitHub Actions workflow for publishing (see `README.md`).
