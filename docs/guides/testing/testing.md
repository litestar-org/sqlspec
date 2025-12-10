---
orphan: true
---

# Testing Guide for SQLSpec

This document describes how we execute and structure tests across SQLSpec. It reflects our current strategy of using **pytest + pytest-databases** (with Docker services) and **pytest-xdist** for parallel execution. Follow these instructions whenever you add, update, or run tests locally or in CI.

---

## 1. Test Command Reference

We invoke pytest directly through `uv` so we share the same virtual environment management in local dev and CI. The most common commands are:

- **Full suite (unit + integration + lint gates configured elsewhere):**

  ```bash
  uv run pytest
  ```

- **Single file:**

  ```bash
  uv run pytest tests/integration/test_asyncpg_driver.py
  ```

- **Single test (node id):**

  ```bash
  uv run pytest tests/unit/adapters/test_asyncpg_config.py::test_validation
  ```

- **Parallel run (recommended by default):**

  ```bash
  uv run pytest -n auto
  ```

  We rely on `pytest-xdist` (`-n auto`) to scale across CPU cores. Keep thread-safety in mind; most of our fixtures are session-scoped, so they are safe to share across workers. If you add fixtures that cannot be shared, mark them `scope="function"` or use `xdist_worker_id` to isolate resources.

---

## 2. pytest-databases Overview

We depend on [`pytest-databases`](https://github.com/litestar-org/pytest-databases) to launch containerized databases on demand. **Never call `make infra-up` or `make infra-down` in tests**—pytest-databases handles lifecycle automatically.

### 2.1 Enable Plugins

To make services available, list the plugins at module import time (usually in `tests/conftest.py`):

```python
pytest_plugins = [
    "pytest_databases.docker.postgres",
    "pytest_databases.docker.mysql",
    "pytest_databases.docker.oracle",
    # add others as needed (redis, elasticsearch, etc.)
]
```

Each plugin contributes fixtures ending with `_service`. **Always use the service fixtures** for consistent access to database containers.

### 2.2 Service Fixtures

- `postgres_service`, `mysql_service`, `oracle_service`, etc. provide access to host, port, credentials, and utility helpers.
- All drivers that target the same database flavour share the same underlying container. For example, `asyncpg`, `psycopg`, and `psqlpy` all point at the PostgreSQL service spun up by `postgres_service`.
- Fixtures are usually **session-scoped** so databases start once per pytest session, which keeps setup fast and reduces container churn.

Example usage:

```python
from pytest_databases.docker.postgres import PostgresService

def test_round_trip(postgres_service: PostgresService) -> None:
    url = postgres_service.connection_url()
    # Use url in SQLSpec config, run migrations once, run assertions…
```

Avoid calling `postgres_service.connection` or `postgres_connection` directly unless there is no alternative; we prefer to construct SQLSpec configs and drivers using the service metadata and reuse our own fixture helpers (see below).

---

## 3. SQLSpec Testing Fixtures

We provide shared fixtures in `tests/fixtures/` to keep tests DRY and ensure consistent setup across drivers. When adding tests:

- Reuse existing config/driver fixtures (e.g., `asyncpg_driver`, `psycopg_sync_driver`) instead of creating new ones.
- Keep fixtures at `session` or `module` scope whenever possible. This allows us to deploy DDL once per service instead of per test.
- Store DDL scripts under `tests/fixtures/postgres/`, `tests/fixtures/mysql/`, etc. Use helper utilities from `tests/fixtures/sql_utils.py` to apply them.
- If you need pre-populated data, prefer fixture-level setup functions that insert data during `setup` once.

### Example Pattern

```python
import pytest
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from tests.fixtures.sql_utils import apply_ddl

@pytest.fixture(scope="session")
def asyncpg_config(postgres_service):
    return AsyncpgConfig(
        connection_config={"dsn": postgres_service.connection_url()},
        extension_config={"litestar": {"session_table": "litestar_sessions"}},
    )

@pytest.fixture(scope="session")
def asyncpg_driver(asyncpg_config):
    spec = SQLSpec()
    config = spec.add_config(asyncpg_config)
    apply_ddl(config, "tests/fixtures/postgres/basic_schema.sql")
    with spec.provide_driver(config) as driver:
        yield driver
```

The combination ensures:

1. The container is started via `postgres_service`.
2. Configuration is stored once per session.
3. DDL executes once per session (fast) rather than per test.

---

## 4. Database Isolation & Data Management

- Because we share containers across tests, **start each test in a known state**. Prefer truncating tables or using transaction rollbacks within the test when data isolation is required.
- For write-heavy suites, consider wrapping tests in `BEGIN … ROLLBACK` using driver transaction fixtures. If the driver does not support nested transactions, reset tables via fixture-level finalizers.
- Use fixture-scoped DB objects (schemas, tables) with unique names to avoid cross-test collisions.

---

## 5. Organising Tests

Directory structure:

```
tests/
├── unit/          # no external services
├── integration/   # requires services (postgres, mysql, etc.)
├── fixtures/      # shared fixtures, ddl scripts, utilities
└── conftest.py    # pytest plugins, global fixtures
```

Guidelines:

- Unit tests should never depend on pytest-databases.
- Integration tests must declare their service dependencies via fixtures and should be marked with meaningful pytest markers (`@pytest.mark.postgres`, `@pytest.mark.oracle`) to enable targeted runs (e.g., `uv run pytest -m postgres`).
- When you add a new adapter or driver, create or reuse fixture modules under `tests/fixtures/<adapter>/`.

---

## 6. Parallel Execution with pytest-xdist

We run with `-n auto` by default. Keep in mind:

- Each worker receives its own copy of fixtures respecting scope. Session-scoped fixtures run once—`pytest-databases` is compatible with xdist because services are globally shared via Docker.
- If you need worker-specific isolation, inspect `request.node` or `xdist_worker_id` in your fixture and adjust resource names accordingly (e.g., create per-worker schemas).
- Ensure DDL scripts are idempotent (`CREATE TABLE IF NOT EXISTS…`) when using session-scoped fixtures.

### 6.1 Test Isolation for Pooled SQLite Connections

When testing framework extensions or code that uses connection pooling with SQLite, avoid using `:memory:` databases as they cause test failures in parallel execution.

**The Problem**:

SQLite's `:memory:` databases with connection pooling share a single database instance across all connections in the pool. When pytest-xdist runs tests in parallel, multiple tests reuse the same connection from the pool, causing "table already exists" errors.

**Why it happens**:

AioSQLite's config automatically converts `:memory:` to `file::memory:?cache=shared` for pooling support, creating a single shared database instance.

**The Solution**:

Use unique temporary database files per test instead of `:memory:`:

```python
import tempfile
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.extensions.starlette import SQLSpecPlugin

def test_starlette_integration() -> None:
    """Test with isolated temporary database."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"starlette": {"commit_mode": "autocommit"}}
        )
        # Each test gets its own isolated database file
        # No state shared between parallel tests
```

**Benefits**:

- Complete test isolation
- Safe parallel execution with `pytest -n auto`
- Files automatically cleaned up on test completion
- Reflects production pooling behavior

**When to use**:

- Framework extension tests (Starlette, FastAPI, Flask, etc.)
- Any test using connection pooling with SQLite
- Integration tests that must run in parallel

**Alternatives to avoid**:

- ❌ `CREATE TABLE IF NOT EXISTS` - Masks isolation issues
- ❌ Disabling pooling - Doesn't test production config
- ❌ Running serially - Slows down CI significantly

---

## 7. Markers & CI Filtering

We register markers in `pyproject.toml` (e.g., `postgres`, `mysql`, `oracle`, `integration`). When writing tests:

```python
import pytest

@pytest.mark.integration
@pytest.mark.postgres
async def test_asyncpg_insert(asyncpg_driver):
    ...
```

This lets us run `uv run pytest -m postgres` locally and use the marker selectors in CI pipelines.

---

## 8. Linting & Static Checks

Tests follow the same linting and type checking rules as production code. Use:

- `uv run ruff check tests/`
- `uv run mypy tests/`

Keep fixtures typed—the more explicit the types, the easier it is to detect broken signatures when pytest-databases updates.

---

## 9. Common Pitfalls & Best Practices

- Don’t start/stop Docker containers manually. Rely on pytest-databases services.
- Prefer service fixtures over connection fixtures to stay future-proof.
- Reuse global fixtures from `tests/fixtures/` rather than duplicating DDL/application setup in individual test modules.
- Ensure teardown logic runs: use `yield` fixtures and finalizers to close drivers, truncate tables, or drop schemas if necessary.
- Document new fixtures and add them to the markdown guides so other contributors know they exist.

---

By following these guidelines, the SQLSpec suite remains fast, reliable, and consistent across local development and CI environments. When in doubt, check existing integration tests for patterns you can extend.*** End Patch
