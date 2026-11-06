# PostgreSQL and AlloyDB Overview

SQLSpec provides three dedicated PostgreSQL adapters—[`asyncpg`](asyncpg.md), [`psycopg`](psycopg.md), and [`psqlpy`](psqlpy.md)—plus columnar [`adbc`](../adbc.md) and [`arrow_odbc`](../arrow-odbc.md) bridges. For distributed CockroachDB clusters, see the standalone [CockroachDB](../cockroachdb.md) guide. All three PostgreSQL adapters share catalog reflection, `LISTEN/NOTIFY` event hubs, and vector/BM25 search extensions.

## Overview & Capabilities

| Adapter Config | Driver Extra | Execution | Pool Architecture | Best For |
| --- | --- | --- | --- | --- |
| [`AsyncpgConfig`](asyncpg.md) | `sqlspec[asyncpg]` | Async | `asyncpg.Pool` (binary protocol) | Lowest-latency pure `asyncio` OLTP services using PostgreSQL's binary wire protocol. |
| [`PsycopgAsyncConfig` / `PsycopgSyncConfig`](psycopg.md) | `sqlspec[psycopg]` | Sync & Async | `psycopg_pool` (`ConnectionPool` / `AsyncConnectionPool` / `NullConnectionPool`) | Services mixing sync and `asyncio` pools, libpq parity, serverless `NullConnectionPool`, or streaming `COPY`. |
| [`PsqlpyConfig`](psqlpy.md) | `sqlspec[psqlpy]` | Async | Rust Tokio `ConnectionPool` | High-concurrency `asyncio` workloads that benefit from a Rust connection pool and native `PgVector` bindings. |

For Google Cloud Managed Databases, add `sqlspec[cloud-sql]` (`cloud-sql-python-connector`) or `sqlspec[alloydb]` (`google-cloud-alloydb-connector`).

## Installation & Quickstart

=== "uv"

    ```bash
    uv add "sqlspec[asyncpg]"
    # or: uv add "sqlspec[psycopg]"
    # or: uv add "sqlspec[psqlpy]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[asyncpg]"
    # or: pip install "sqlspec[psycopg]"
    # or: pip install "sqlspec[psqlpy]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[asyncpg]"
    # or: pdm add "sqlspec[psycopg]"
    # or: pdm add "sqlspec[psqlpy]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[asyncpg]"
    # or: poetry add "sqlspec[psycopg]"
    # or: poetry add "sqlspec[psqlpy]"
    ```

=== "asyncpg (Async)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    spec = SQLSpec()
    config = spec.add_config(
        AsyncpgConfig(
            connection_config={
                "dsn": "postgresql://app:secret@localhost:5432/prod",
                "min_size": 2,
                "max_size": 20,
            }
        )
    )
    ```

=== "psycopg (Async or Sync)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig

    spec = SQLSpec()
    config = spec.add_config(
        PsycopgAsyncConfig(
            connection_config={
                "conninfo": "postgresql://app:secret@localhost:5432/prod",
                "min_size": 2,
                "max_size": 20,
            }
        )
    )
    ```

=== "psqlpy (Async Rust)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.psqlpy import PsqlpyConfig

    spec = SQLSpec()
    config = spec.add_config(
        PsqlpyConfig(
            connection_config={
                "dsn": "postgresql://app:secret@localhost:5432/prod",
                "max_db_pool_size": 20,
            }
        )
    )
    ```

## Connection & Pool Configuration

All three PostgreSQL adapters separate connection/pool parameters (`connection_config`) from SQLSpec runtime capabilities (`driver_features`):

| Setting | Location | Type | Description |
| --- | --- | --- | --- |
| `auth_token_provider` | `connection_config` | `Callable[[], str or Awaitable[str]]` | Sync or async callable invoked on connection creation to mint short-lived IAM authentication tokens (Google Cloud SQL IAM, AlloyDB IAM, AWS RDS IAM, Azure AD) without restarting pools. |
| `pooler_mode` | `driver_features` | `"session" or "transaction" or "statement"` | Disables protocol-level prepared statement caching automatically in `"transaction"` and `"statement"` modes (PgBouncer, Supavisor, Odyssey). |
| `direct_dsn` | `driver_features` | `str` | Unpooled PostgreSQL DSN used by `config.derive_direct_config()` so DDL migrations and `ListenerHub` (`LISTEN/NOTIFY`) bypass transaction poolers. |
| `enable_alloydb_columnar` | `driver_features` | `bool` | Enables `google_columnar_engine.enable_columnar_scan = 'on'` on Google Cloud AlloyDB connections or transaction-locally via `SET LOCAL`. |
| `enable_pgvector` | `driver_features` | `bool` | Registers `pgvector` distance operators (`<->`, `<=>`, `<#>`) and vector codecs when available. |
| `enable_paradedb` | `driver_features` | `bool` | Enables ParadeDB `pg_search` BM25 (`@@@`) operators and hybrid search SQL generation. |
| `enable_pg_textsearch` | `driver_features` | `bool` | Enables PostgreSQL `pg_textsearch` full-text search extensions in ADK memory stores. |
| `json_serializer` / `json_deserializer` | `driver_features` | `Callable` | Customizes `json` and `jsonb` wire encoding (defaults to SQLSpec's centralized `msgspec` serializers). |

### Cloud IAM Authentication with auth_token_provider

Pass `auth_token_provider` inside `connection_config` on `AsyncpgConfig`, `PsycopgSyncConfig`, `PsycopgAsyncConfig`, or `PsqlpyConfig` to inject rotating IAM tokens on every new physical connection:

```python
from sqlspec.adapters.asyncpg import AsyncpgConfig


async def mint_iam_token() -> str:
    return "short-lived-iam-token"


config = AsyncpgConfig(
    connection_config={
        "host": "10.0.0.5",
        "port": 5432,
        "user": "app-sa@project.iam",
        "database": "prod",
        "auth_token_provider": mint_iam_token,
    }
)
```

### Running Behind PgBouncer (pooler_mode and direct_dsn)

Transaction-mode poolers multiplex backend server connections across clients after each transaction commits, breaking session-scoped prepared statements, advisory locks, and `LISTEN/NOTIFY` channels. Configure `pooler_mode="transaction"` alongside `direct_dsn`:

```python
from sqlspec.adapters.asyncpg import AsyncpgConfig

config = AsyncpgConfig(
    connection_config={
        "dsn": "postgresql://app:secret@pgbouncer:6432/prod",
        "min_size": 5,
        "max_size": 25,
    },
    driver_features={
        "pooler_mode": "transaction",
        "direct_dsn": "postgresql://app:secret@postgres-primary:5432/prod",
    },
)
```

With both settings active, HTTP traffic uses PgBouncer safely without prepared-statement collisions while `sqlspec upgrade` and `EventChannel` `LISTEN/NOTIFY` route through `config.derive_direct_config()`.

## Type Mappings & Dialect Notes

| Database Type | Python Type | Notes / Feature Flag |
| --- | --- | --- |
| `JSON` / `JSONB` | `dict` / `list` | Encoded and decoded automatically via `json_serializer` and `json_deserializer` (`msgspec` by default). |
| `UUID` | `uuid.UUID` / `str` | Native binary/text UUID coercion across all three PostgreSQL drivers. |
| `VECTOR` (`pgvector`) | `list[float]` / `numpy.ndarray` | Enabled automatically when `enable_pgvector=True` and `pgvector` is installed (`PgVector` native in `psqlpy`). |
| `ARRAY` | `list[T]` | Native PostgreSQL multi-dimensional and typed array binding. |
| `TSRANGE` / `TSTZRANGE` / `DATERANGE` | `Range` / `tuple` | Native range codecs on `asyncpg` and `psycopg`. |
| `BYTEA` | `bytes` / `bytearray` | Binary wire transport without hex-encoding overhead. |
| `NUMERIC` / `DECIMAL` | `decimal.Decimal` | Exact arbitrary-precision decimal arithmetic. |

- **Parameter Translation**: Write `:named` parameters in SQL or `.sql` files; SQLSpec compiles them to `$1, $2` for `asyncpg`/`psqlpy` and `%s` / `%(name)s` for `psycopg` while preserving PostgreSQL `::type` casts.
- **JSONB Key Existence Operator**: Prefer `jsonb_exists(payload, :key)` or named `:key` placeholders when checking key existence so `?` is never ambiguous with positional qmark parameter styles.
- **Exception Hierarchy**: SQLSTATE codes `23505`, `23503`, `23502`, `23514`, `40P01`, `40001`, and `42501` map consistently to `UniqueViolationError`, `ForeignKeyViolationError`, `NotNullViolationError`, `CheckViolationError`, `DeadlockError`, `SerializationConflictError`, and `PermissionDeniedError`. `57014` (query canceled) maps to `QueryTimeoutError` when the server reports a statement timeout and to `OperationCancelledError` otherwise.

## See Also

- [PostgreSQL — asyncpg Guide](asyncpg.md)
- [PostgreSQL — psycopg Guide](psycopg.md)
- [PostgreSQL — psqlpy Guide](psqlpy.md)
- [AsyncPG API Reference](/reference/adapters/asyncpg/)
- [Psycopg API Reference](/reference/adapters/psycopg/)
- [Psqlpy API Reference](/reference/adapters/psqlpy/)
- [PostgreSQL & ParadeDB Dialect Reference](/reference/data-dictionary/dialects/postgres/)
