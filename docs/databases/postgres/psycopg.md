# PostgreSQL — psycopg

`sqlspec.adapters.psycopg` wraps Psycopg 3 (`psycopg[binary,pool]`), providing symmetric synchronous (`PsycopgSyncConfig`) and asynchronous (`PsycopgAsyncConfig`) connection pools, `NullConnectionPool` support for serverless runtimes, streaming `COPY`, and full libpq compatibility.

## Overview & Capabilities

Choose `psycopg` when your service mixes synchronous CLI/worker processes with `asyncio` HTTP servers, relies on standard `libpq` environment variables and `.pgpass` / GSSAPI / Kerberos authentication, or runs on scale-to-zero serverless platforms where `enable_pooling=False` avoids idle connections.

| Adapter Config | Execution | Pool Class | Best For |
| --- | --- | --- | --- |
| `PsycopgAsyncConfig` | Async (`asyncio`) | `psycopg_pool.AsyncConnectionPool` (or `AsyncNullConnectionPool`) | Async HTTP APIs, streaming `COPY`, and `LISTEN/NOTIFY` event listeners. |
| `PsycopgSyncConfig` | Sync | `psycopg_pool.ConnectionPool` (or `NullConnectionPool`) | Synchronous workers, CLI tooling, and shared sync/async PostgreSQL deployments. |

## Installation & Quickstart

=== "uv"

    ```bash
    uv add "sqlspec[psycopg]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[psycopg]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[psycopg]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[psycopg]"
    ```

=== "Async (PsycopgAsyncConfig)"

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

=== "Sync (PsycopgSyncConfig)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.psycopg import PsycopgSyncConfig

    spec = SQLSpec()
    config = spec.add_config(
        PsycopgSyncConfig(
            connection_config={
                "conninfo": "postgresql://app:secret@localhost:5432/prod",
                "min_size": 2,
                "max_size": 10,
            }
        )
    )
    ```

## Connection & Pool Configuration

### Connection and Pool Parameters

Pass `PsycopgConnectionParams` and `PsycopgPoolParams` keys inside `connection_config={...}` on `PsycopgSyncConfig` or `PsycopgAsyncConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.psycopg.config.PsycopgConnectionParams,sqlspec.adapters.psycopg.config.PsycopgPoolParams -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `conninfo` | `str` | - | PostgreSQL libpq connection URI or keyword/value conninfo string. |
| `host` | `str` | - | Database server hostname or Unix socket directory. |
| `port` | `int` | `5432` | Database server TCP port (defaults to `5432`). |
| `user` | `str` | - | PostgreSQL role name used for authentication. |
| `password` | `str` | - | Password or token for `user`. |
| `auth_token_provider` | `Callable[[], str] or Callable[[], Awaitable[str]]` | - | Callable returning a fresh authentication password or IAM token on each connection attempt. |
| `dbname` | `str` | - | Target PostgreSQL database name. |
| `connect_timeout` | `int` | - | Connection establishment timeout in seconds. |
| `options` | `str` | - | Command-line startup GUC options sent to the PostgreSQL backend. |
| `application_name` | `str` | - | Application identifier reported in `pg_stat_activity`. |
| `sslmode` | `PsycopgSSLMode` | - | libpq TLS verification mode (`disable`, `allow`, `prefer`, `require`, `verify-ca`, or `verify-full`). |
| `sslcert` | `str` | - | Path to the client TLS certificate file. |
| `sslkey` | `str` | - | Path to the client TLS private key file. |
| `sslrootcert` | `str` | - | Path to the trusted root CA certificate file. |
| `autocommit` | `bool` | - | Enable Psycopg autocommit mode outside explicit transactions. |
| `prepare_threshold` | `int or None` | - | Executions before server-side statement preparation, or `None` to disable prepared statements. |
| `context` | `AdaptContext or None` | - | Custom Psycopg `AdaptContext` used for type adaptation. |
| `row_factory` | `RowFactory[Any] or AsyncRowFactory[Any] or None` | - | Row factory applied to raw Psycopg connections or cursors. |
| `cursor_factory` | `type[Cursor[Any] or AsyncCursor[Any]] or None` | - | Custom Psycopg `Cursor` or `AsyncCursor` class. |
| `extra` | `dict[str, Any]` | - | Additional libpq keyword parameters forwarded to `connect`. |
| `min_size` | `int` | - | Minimum number of connections kept open in `ConnectionPool` / `AsyncConnectionPool`. |
| `max_size` | `int` | - | Maximum number of connections allowed in the pool. |
| `connection_class` | `type[Connection[Any] or AsyncConnection[Any]]` | - | Custom Psycopg `Connection` or `AsyncConnection` subclass. |
| `name` | `str` | - | Pool identifier used in logs and worker thread names. |
| `timeout` | `float` | - | Maximum wait time in seconds when acquiring a connection from the pool. |
| `max_waiting` | `int` | - | Maximum number of queued callers waiting for a connection (`0` means no limit). |
| `max_lifetime` | `float` | - | Maximum connection age in seconds before recycling. |
| `max_idle` | `float` | - | Maximum idle duration in seconds before shrinking connections above `min_size`. |
| `reconnect_timeout` | `float` | - | Seconds to retry failed connection attempts before invoking `reconnect_failed`. |
| `num_workers` | `int` | - | Number of background maintenance workers run by `psycopg_pool`. |
| `open` | `bool or None` | - | Whether to open the pool immediately upon instantiation. |
| `configure` | `ConnectionCB[PsycopgSyncConnection] or AsyncConnectionCB[PsycopgAsyncConnection] or None` | - | Callback run once on each new physical connection before entering the pool. |
| `check` | `ConnectionCB[PsycopgSyncConnection] or AsyncConnectionCB[PsycopgAsyncConnection] or None` | - | Health-check callback run before lending out a pooled connection. |
| `reset` | `ConnectionCB[PsycopgSyncConnection] or AsyncConnectionCB[PsycopgAsyncConnection] or None` | - | Reset callback run when returning a connection to the pool. |
| `close_returns` | `bool` | - | Make `conn.close()` return the connection to the pool instead of closing the socket. |
| `reconnect_failed` | `ConnectFailedCB or AsyncConnectFailedCB or None` | - | Callback invoked when pool reconnection attempts exceed `reconnect_timeout`. |
| `kwargs` | `dict[str, Any]` | - | Explicit connection keyword mapping passed to `psycopg.connect`. |
<!-- GENERATED:END config-table:sqlspec.adapters.psycopg.config.PsycopgConnectionParams,sqlspec.adapters.psycopg.config.PsycopgPoolParams -->

### Driver Features

Pass `PsycopgDriverFeatures` keys inside `driver_features={...}` on `PsycopgSyncConfig` or `PsycopgAsyncConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.psycopg.config.PsycopgDriverFeatures -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `enable_pgvector` | `bool` | - | Register `pgvector` adapters and promote the dialect when `pgvector` is installed. |
| `enable_paradedb` | `bool` | `True` | Detect `pg_search` and promote the SQL dialect to `paradedb` (defaults to `True`). |
| `enable_pg_textsearch` | `bool` | `True` | Detect `pg_textsearch` and promote the dialect for BM25 `<@>` ranking (defaults to `True`). |
| `tenant_strategy` | `SyncTenantStrategy or AsyncTenantStrategy or TenantStrategyShorthand or None` | - | Opt-in tenant strategy or `rls` shorthand for every pooled lease. |
| `json_serializer` | `Callable[[Any], str]` | - | Custom JSON serializer applied to statement parameters and Psycopg JSON dumpers. |
| `json_deserializer` | `Callable[[str], Any]` | - | Custom JSON deserializer applied to Psycopg JSON/JSONB loaders. |
| `on_connection_create` | `Callable[..., Any]` | - | Sync or async callback invoked with each raw Psycopg connection after internal setup. |
| `enable_events` | `bool` | `True` | Enable SQLSpec event channel pub/sub support (defaults to `True` when `events` is configured). |
| `events_backend` | `Literal['notify', 'notify_queue', 'poll_queue']` | - | Event transport backend (`notify` for LISTEN/NOTIFY, `notify_queue`, or `poll_queue`). |
| `enable_alloydb` | `bool` | `False` | Route sync connections through `google-cloud-alloydb-connector` (defaults to `False`). |
| `alloydb_instance_uri` | `str` | - | AlloyDB instance resource URI (`projects/.../locations/.../clusters/.../instances/...`). |
| `enable_alloydb_iam_auth` | `bool` | - | Use IAM database authentication with the AlloyDB Python Connector. |
| `alloydb_ip_type` | `str` | `PRIVATE` | AlloyDB network path (`PUBLIC`, `PRIVATE`, or `PSC`; defaults to `PRIVATE`). |
| `enable_alloydb_columnar` | `bool` | - | Enable `google_columnar_engine.enable_columnar_scan=on` via startup `options` or per-transaction `SET LOCAL`. |
| `enable_pgcopy_binary` | `bool` | - | Enable binary streaming format for Arrow ingestion via PGCOPY binary protocol. |
| `pooler_mode` | `Literal['session', 'transaction', 'statement', 'auto']` | - | External pooler compatibility mode; sets `prepare_threshold=None` in `transaction` or `statement` mode. |
| `direct_dsn` | `str or Mapping[str, Any] or None` | - | Direct PostgreSQL DSN or connection mapping bypassing the pooler for migrations and `LISTEN/NOTIFY`. |
| `enable_pooling` | `bool` | `True` | Enable connection pooling (defaults to `True`; `False` uses `NullConnectionPool` / `AsyncNullConnectionPool`). |
<!-- GENERATED:END config-table:sqlspec.adapters.psycopg.config.PsycopgDriverFeatures -->

## Type Mappings & Dialect Notes

| Database Type | Python Type | Notes / Feature Flag |
| --- | --- | --- |
| `JSON` / `JSONB` | `dict` / `list` | Configured via `psycopg.types.json.set_json_dumps` / `set_json_loads` using `json_serializer` and `json_deserializer`. |
| `UUID` | `uuid.UUID` / `str` | Native Psycopg 3 UUID loader and dumper. |
| `VECTOR` (`pgvector`) | `list[float]` / `numpy.ndarray` | Registered when `enable_pgvector=True` and `pgvector` is installed. |
| `ARRAY` | `list[T]` | Native PostgreSQL array adaptation. |
| `TSRANGE` / `TSTZRANGE` / `DATERANGE` | `psycopg.types.range.Range` | Native PostgreSQL range adaptation with typed bounds. |
| `BYTEA` | `bytes` / `bytearray` / `memoryview` | Binary parameter and result column transport. |
| `NUMERIC` / `DECIMAL` | `decimal.Decimal` | Exact decimal arithmetic. |

- **Pooler Mode and Direct DSN**: When `pooler_mode` is set to `"transaction"` or `"statement"`, SQLSpec automatically sets `prepare_threshold=None` to prevent prepared statement collisions on PgBouncer and routes migrations and `LISTEN/NOTIFY` through `direct_dsn` via `config.derive_direct_config()`.
- **Placeholder Compilation**: Named `:param` and positional `?` placeholders compile to Psycopg's `%s` / `%(param)s` styles automatically while escaping literal `%` characters in SQL strings.
- **Built-In executemany Pipelining**: Psycopg 3's `cursor.executemany` enters pipeline mode internally, so `session.execute_many(...)` batches multi-row writes over a single network pipeline without extra configuration.
- **Bounded COPY Streaming and LISTEN/NOTIFY**: `session.load_from_arrow(...)` and `COPY` operations stream data in bounded memory chunks rather than materializing entire tables into Python strings, and `EventChannel` supports native `LISTEN/NOTIFY` wakeup notifications.

## See Also

- [PostgreSQL and AlloyDB Overview](index.md)
- [Psycopg API Reference](/reference/adapters/psycopg/)
- [PostgreSQL Dialect Reference](/reference/data-dictionary/dialects/postgres/)
