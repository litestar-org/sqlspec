# Testing

SQLSpec's thin session contract makes database code straightforward to test at two complementary levels: sub-millisecond unit and domain tests against isolated SQLite databases, and dialect-verified integration tests against containerized engines managed by `pytest-databases`.

## Fast Isolated SQLite Test Fixtures

For domain logic, parameter binding, `SQLFileLoader` validation, and service-layer workflows that use portable SQL, provision a fresh SQLite database per test:

```{ .python title="docs/examples/patterns/integration_testing.py" }
--8<-- "docs/examples/patterns/integration_testing.py:example"
```

In `pytest`, wrap `create_test_spec` inside a fixture using `tmp_path` (or `":memory:"` for single-connection tests) and close the pool in `finally:`:

```python
from collections.abc import Iterator
from pathlib import Path
import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig


@pytest.fixture
def db_config(tmp_path: Path) -> Iterator[SqliteConfig]:
    spec = SQLSpec()
    config = spec.add_config(
        SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    )
    try:
        yield config
    finally:
        config.close_pool()
```

When testing `:memory:` SQLite across multiple pooled connections or threads, use a shared-cache URI (`"file:test_mem?mode=memory&cache=shared"` with `uri=True`) so every pooled checkout sees the same in-memory schema.

## Containerized Dialect Tests with pytest-databases

Dialect-specific features—such as PostgreSQL `JSONB` / `pgvector` / `LISTEN/NOTIFY`, Oracle 26ai `VECTOR`, Spanner `INTERLEAVE`, or DuckDB Parquet pipelines—should be verified against real database containers using [`pytest-databases`](https://github.com/litestar-org/pytest-databases):

=== "uv"

    ```bash
    uv add --dev pytest pytest-asyncio pytest-databases
    ```

=== "pip"

    ```bash
    pip install pytest pytest-asyncio pytest-databases
    ```

=== "pdm"

    ```bash
    pdm add -d pytest pytest-asyncio pytest-databases
    ```

=== "poetry"

    ```bash
    poetry add --group dev pytest pytest-asyncio pytest-databases
    ```

```python
import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg import AsyncpgConfig


@pytest.mark.asyncio
async def test_postgres_users(postgres_service: PostgresService) -> None:
    config = AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        }
    )
    try:
        async with config.provide_session() as session:
            version = await session.select_value(
                "SELECT current_setting('server_version_num')::int"
            )
            assert version >= 140000
    finally:
        await config.close_pool()
```

`pytest-databases` provides ready-to-use Docker/Podman service fixtures for PostgreSQL (`postgres_service`, `pgvector_service`, `paradedb_service`, `alloydb_omni_service`), MySQL, MariaDB, CockroachDB, Oracle (`oracle_23ai_service`), SQL Server, Db2, Spanner emulator, BigQuery emulator, MinIO/RustFS, and Floci cloud storage emulators.

## Seeding Deterministic Table Fixtures

Instead of maintaining brittle `INSERT` setup helpers in test files, store canonical seed rows in `tests/fixtures/` (`.jsonl.gz`, `.json.gz`, `.jsonl`, or `.json`, including numbered shards `<table>.001.jsonl.gz`) and use the 8 helpers in `sqlspec.utils.fixtures`:

| Function | Mode | Purpose & Signature Highlights |
| --- | --- | --- |
| `load_table_fixtures_sync(driver, fixtures_path, ...)` | Sync | Introspects column types (including `VECTOR` embeddings, `UUID`, `Decimal`, `JSONB`), loads per-table or sharded fixtures, and optionally resyncs sequences. |
| `load_table_fixtures_async(driver, fixtures_path, ...)` | Async | Async counterpart to `load_table_fixtures_sync` for `AsyncDriverAdapterBase` sessions. |
| `export_table_fixtures_sync(driver, fixtures_path, tables, ...)` | Sync | Exports live tables to `.json` / `.jsonl` (optionally gzipped or sharded via `max_shard_bytes` / `max_rows_per_shard`). |
| `export_table_fixtures_async(driver, fixtures_path, tables, ...)` | Async | Async counterpart to `export_table_fixtures_sync`. |
| `open_fixture_sync(fixtures_path, fixture_name)` | Sync | Reads and parses a `.json`, `.json.gz`, or `.json.zip` fixture payload from local disk or `storage_registry`. |
| `open_fixture_async(fixtures_path, fixture_name)` | Async | Async counterpart to `open_fixture_sync`. |
| `write_fixture_sync(fixtures_path, table_name, data, ...)` | Sync | Serializes Python records or `msgspec` / Pydantic models to a `.json` or `.json.gz` fixture file. |
| `write_fixture_async(fixtures_path, table_name, data, ...)` | Async | Async counterpart to `write_fixture_sync`. |

### Key Table Fixture Options

| Parameter | Applies To | Behavior |
| --- | --- | --- |
| `tables` / `table_order` | `load_table_fixtures_*` | Restricts which tables load and specifies explicit foreign-key dependency order. |
| `conflict_keys` | `load_table_fixtures_*` | Mapping of `{table: key_or_keys}` enabling idempotent upsert loads across test runs. |
| `exclude_update_columns` | `load_table_fixtures_*` | Columns preserved unchanged when an upsert conflict updates an existing row. |
| `ignore_unknown_columns` | `load_table_fixtures_*` | Drops fixture fields not present in the target table so shared fixtures survive schema branches. |
| `resync_sequences` | `load_table_fixtures_*` | Advances identity and sequence counters past explicit fixture IDs (`True` by default). On MySQL, MariaDB, and Oracle, sequence resync executes `ALTER` DDL that implicitly commits the active transaction; set `resync_sequences=False` inside rollback-only test transactions. |
| `max_shard_bytes` / `max_rows_per_shard` | `export_table_fixtures_*` | Splits exported tables into numbered shards (`<table>.001.jsonl.gz`, ...) and cleans up stale shard files atomically. |

```python
from pathlib import Path
from sqlspec.utils.fixtures import load_table_fixtures_sync

with spec.provide_session(config) as session:
    load_table_fixtures_sync(
        session,
        fixtures_path=Path("tests/fixtures"),
        tables=["teams", "members"],
        conflict_keys={"teams": "name", "members": "email"},
    )
```

## Recap

- Test portable query and service behavior against isolated per-test `SqliteConfig` or `AiosqliteConfig` instances for instant feedback.
- Verify engine-specific SQL, pooler modes, and vector/JSON codecs against real containers with `pytest-databases`.
- Always call `config.close_pool()` / `await config.close_pool()` in fixture teardown so background pool tasks exit cleanly.
