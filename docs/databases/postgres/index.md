# PostgreSQL and AlloyDB Overview

SQLSpec provides three dedicated PostgreSQL adapters—[`asyncpg`](asyncpg.md), [`psycopg`](psycopg.md), and [`psqlpy`](psqlpy.md)—plus columnar [`adbc`](../adbc.md) and [`arrow_odbc`](../arrow-odbc.md) bridges. For distributed CockroachDB clusters, see the standalone [CockroachDB](../cockroachdb.md) guide. All three PostgreSQL adapters share catalog reflection, `LISTEN/NOTIFY` event hubs, and vector/BM25 search extensions.

## Install and When to Choose

| Adapter | Install Command | Best Fit |
| --- | --- | --- |
| [`asyncpg`](asyncpg.md) | `uv add "sqlspec[asyncpg]"` | Lowest-latency pure `asyncio` OLTP services using PostgreSQL's binary wire protocol. |
| [`psycopg`](psycopg.md) | `uv add "sqlspec[psycopg]"` | Applications needing both synchronous and `asyncio` pools, libpq parity, `NullConnectionPool`, or streaming `COPY`. |
| [`psqlpy`](psqlpy.md) | `uv add "sqlspec[psqlpy]"` | High-concurrency `asyncio` services using a Rust/Tokio connection pool, native `PgVector` binding, and wire pipelining. |

For Google Cloud Managed Databases, add `sqlspec[cloud-sql]` (`cloud-sql-python-connector`) or `sqlspec[alloydb]` (`google-cloud-alloydb-connector`).

## Minimal Connection and Pool Setup

=== "asyncpg (Async)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    spec = SQLSpec()
    config = spec.add_config(
        AsyncpgConfig(
            connection_config={"dsn": "postgresql://app:secret@localhost:5432/prod", "min_size": 2, "max_size": 20}
        )
    )
    ```

=== "psycopg (Sync or Async)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig

    spec = SQLSpec()
    config = spec.add_config(
        PsycopgAsyncConfig(
            connection_config={"conninfo": "postgresql://app:secret@localhost:5432/prod", "min_size": 2, "max_size": 20}
        )
    )
    ```

=== "psqlpy (Async Rust)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.psqlpy import PsqlpyConfig

    spec = SQLSpec()
    config = spec.add_config(
        PsqlpyConfig(connection_config={"dsn": "postgresql://app:secret@localhost:5432/prod", "max_db_pool_size": 20})
    )
    ```

## Key Driver Features and Pool Tuning

`asyncpg`, `psycopg`, and `psqlpy` share unified `driver_features` keys for external connection poolers, AlloyDB, and search extensions:

| `driver_features` Key | Type | Description |
| --- | --- | --- |
| `pooler_mode` | `"session" \| "transaction" \| "statement"` | Disables protocol-level prepared statement caching automatically in `"transaction"` and `"statement"` modes (PgBouncer, Supavisor, Odyssey). |
| `direct_dsn` | `str` | Unpooled PostgreSQL DSN used by `config.derive_direct_config()` so DDL migrations and `ListenerHub` (`LISTEN/NOTIFY`) bypass transaction poolers. |
| `enable_alloydb_columnar` | `bool` | Enables `google_columnar_engine.enable_columnar_scan = 'on'` on Google Cloud AlloyDB connections or transaction-locally via `SET LOCAL`. |
| `enable_pgvector` | `bool` | Registers `pgvector` distance operators (`<->`, `<=>`, `<#>`) and vector codecs when available. |
| `enable_paradedb` | `bool` | Enables ParadeDB `pg_search` BM25 (`@@@`) operators and hybrid search SQL generation. |
| `enable_pg_textsearch` | `bool` | Enables PostgreSQL `pg_textsearch` full-text search extensions in ADK memory stores. |
| `json_serializer` / `json_deserializer` | `Callable` | Customizes `json` and `jsonb` wire encoding (defaults to SQLSpec's centralized `msgspec` serializers). |

### Running Behind PgBouncer (`pooler_mode` + `direct_dsn`)

Transaction-mode poolers multiplex backend server connections across clients after each transaction commits, breaking session-scoped prepared statements, advisory locks, and `LISTEN/NOTIFY` channels. Configure `pooler_mode="transaction"` alongside `direct_dsn`:

```python
from sqlspec.adapters.asyncpg import AsyncpgConfig

config = AsyncpgConfig(
    connection_config={"dsn": "postgresql://app:secret@pgbouncer:6432/prod", "min_size": 5, "max_size": 25},
    driver_features={
        "pooler_mode": "transaction",
        "direct_dsn": ("postgresql://app:secret@postgres-primary:5432/prod"),
    },
)
```

With both settings active, HTTP traffic uses PgBouncer safely without prepared-statement collisions while `sqlspec upgrade` and `EventChannel` `LISTEN/NOTIFY` route through `config.derive_direct_config()`.

## Type Mapping and Dialect Notes

- **Parameter Translation**: Write `:named` parameters in SQL or `.sql` files; SQLSpec compiles them to `$1, $2` for `asyncpg`/`psqlpy` and `%s` / `%(name)s` for `psycopg` while preserving PostgreSQL `::type` casts.
- **JSONB `?` Operator**: Prefer `jsonb_exists(payload, :key)` or named `:key` placeholders when checking key existence so `?` is never ambiguous with qmark parameter styles.
- **Exception Hierarchy**: SQLSTATE codes (`23505`, `23503`, `23502`, `23514`, `40P01`, `40001`, `57014`, `42501`) map consistently to `UniqueViolationError`, `ForeignKeyViolationError`, `NotNullViolationError`, `CheckViolationError`, `DeadlockError`, `SerializationConflictError`, `QueryTimeoutError`, and `PermissionDeniedError`.
