# SQLite and Aiosqlite

SQLSpec ships `mypyc`-compiled synchronous (`sqlspec.adapters.sqlite`) and asynchronous (`sqlspec.adapters.aiosqlite`) SQLite adapters with thread-safe FIFO connection pools, automatic pragma application, shared-memory `:memory:` URI support, and custom Python SQL functions.

## Install and When to Choose

=== "uv"

    ```bash
    # Sync (sqlite3 — built-in)
    uv add sqlspec

    # Async (aiosqlite)
    uv add "sqlspec[aiosqlite]"
    ```

=== "pip"

    ```bash
    # Sync (sqlite3 — built-in)
    pip install sqlspec

    # Async (aiosqlite)
    pip install "sqlspec[aiosqlite]"
    ```

=== "pdm"

    ```bash
    # Sync (sqlite3 — built-in)
    pdm add sqlspec

    # Async (aiosqlite)
    pdm add "sqlspec[aiosqlite]"
    ```

=== "poetry"

    ```bash
    # Sync (sqlite3 — built-in)
    poetry add sqlspec

    # Async (aiosqlite)
    poetry add "sqlspec[aiosqlite]"
    ```

Choose **`SqliteConfig`** for CLI tools, synchronous web apps, embedded local storage, and sub-millisecond unit tests. Choose **`AiosqliteConfig`** for `asyncio` frameworks (Litestar, FastAPI, Starlette, Google ADK) where SQLite I/O runs on a dedicated worker thread without blocking the event loop.

## Minimal Connection and Pool Setup

```{ .python title="docs/examples/drivers/sqlite_connection.py" }
--8<-- "docs/examples/drivers/sqlite_connection.py:example"
```

For production file-backed SQLite databases, enable Write-Ahead Logging (`WAL`), foreign key enforcement, and a bounded `busy_timeout` via `driver_features={"pragmas": ...}`:

```python
from sqlspec.adapters.aiosqlite import AiosqliteConfig

config = AiosqliteConfig(
    connection_config={
        "database": "var/app.db",
        "timeout": 30.0,
        "pool_min_size": 1,
        "pool_max_size": 4,
    },
    driver_features={
        "pragmas": {
            "journal_mode": "WAL",
            "synchronous": "NORMAL",
            "foreign_keys": "ON",
            "busy_timeout": 5000,
            "cache_size": -64000,
        }
    },
)
```

## Key Driver Features and Pool Tuning

| Setting | Location | Description |
| --- | --- | --- |
| `database` | `connection_config` | File path, `":memory:"`, or SQLite URI (`"file:memdb?mode=memory&cache=shared"` with `uri=True`). |
| `timeout` | `connection_config` | Lock wait timeout in seconds passed to `sqlite3.connect`. |
| `uri` | `connection_config` | Interprets `database` as a SQLite URI string when `True`. |
| `pool_min_size` / `pool_max_size` | `connection_config` | Bounds for `SqliteConnectionPool` / `AiosqliteConnectionPool`. |
| `pragmas` | `driver_features` | Mapping of `PRAGMA` settings applied automatically whenever a pooled connection is created. |
| `default_transaction_mode` | `driver_features` | `"DEFERRED"` (default on `sqlite`), `"IMMEDIATE"` (default on `aiosqlite` to prevent `SQLITE_BUSY` upgrade deadlocks), or `"EXCLUSIVE"`. |
| `custom_functions` / `custom_aggregates` | `driver_features` | Registers deterministic Python scalar or aggregate functions on every connection. |
| `custom_window_functions` / `custom_collations` | `driver_features` | Registers Python 3.11+ window functions and custom `ORDER BY` collations. |
| `extensions` | `driver_features` | Sequence of loadable SQLite C extension paths (such as `sqlite-vec` or `spatialite`). |

See [SQLite Reference](/reference/adapters/sqlite/) and [Aiosqlite Reference](/reference/adapters/aiosqlite/) for full `TypedDict` parameter tables.

## Type Mapping and Dialect Notes

- **Pooled `:memory:` Databases**: A plain `":memory:"` path creates an isolated database per connection. When `pool_max_size > 1` in tests, pass `"database": "file:test_shared?mode=memory&cache=shared"` with `"uri": True` so all pooled checkouts share one schema.
- **`BEGIN IMMEDIATE` Concurrency on `aiosqlite`**: Under concurrent `asyncio` writes, starting with `DEFERRED` and upgrading to a write lock mid-transaction causes immediate `SQLITE_BUSY` errors. `AiosqliteConfig` defaults to `BEGIN IMMEDIATE` so writers wait cleanly up to `busy_timeout`.
- **Python 3.12–3.14 Thread Safety**: `AiosqliteConnectionPool` performs all `in_transaction` and `autocommit` inspection on the connection's dedicated worker thread, satisfying CPython 3.14's strict thread-affinity checks while skipping worker hops on clean connection releases.
- **JSON and FTS5**: Modern SQLite builds include `JSONB` / `json_extract` functions and `FTS5` virtual tables out of the box, used automatically by `SqliteADKMemoryStore` and `AiosqliteADKMemoryStore` when full-text search is enabled.
