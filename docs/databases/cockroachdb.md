# CockroachDB

SQLSpec provides two dedicated CockroachDB adapters—`sqlspec.adapters.cockroach_asyncpg` (`CockroachAsyncpgConfig`) and `sqlspec.adapters.cockroach_psycopg` (`CockroachPsycopgSyncConfig`, `CockroachPsycopgAsyncConfig`)—layering distributed `40001` transaction retry policies, bounded exponential jitter, and `AS OF SYSTEM TIME` follower reads onto PostgreSQL wire drivers.

## Overview & Capabilities

| Adapter Config | Driver Extra | Execution | Pool Architecture | Best For |
| --- | --- | --- | --- | --- |
| `CockroachAsyncpgConfig` | `sqlspec[cockroachdb]` or `sqlspec[asyncpg]` | Async (`asyncio`) | `asyncpg.Pool` (`CockroachAsyncpgPool`) | Pure-`asyncio` distributed OLTP services prioritizing binary-protocol performance. |
| `CockroachPsycopgAsyncConfig` | `sqlspec[cockroachdb]` or `sqlspec[psycopg]` | Async (`asyncio`) | `psycopg_pool.AsyncConnectionPool` (or `AsyncNullConnectionPool`) | Async services requiring libpq TLS/GSSAPI certificate options or serverless `enable_pooling=False`. |
| `CockroachPsycopgSyncConfig` | `sqlspec[cockroachdb]` or `sqlspec[psycopg]` | Sync | `psycopg_pool.ConnectionPool` (or `NullConnectionPool`) | Synchronous services, CLI tools, and background workers targeting CockroachDB clusters. |

## Installation & Quickstart

=== "uv"

    ```bash
    uv add "sqlspec[cockroachdb]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[cockroachdb]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[cockroachdb]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[cockroachdb]"
    ```

=== "CockroachAsyncpg (Async)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig

    spec = SQLSpec()
    config = spec.add_config(
        CockroachAsyncpgConfig(
            connection_config={
                "dsn": "postgresql://root@localhost:26257/defaultdb",
                "min_size": 2,
                "max_size": 20,
            },
            driver_features={
                "enable_auto_retry": True,
                "max_retries": 5,
                "enable_follower_reads": False,
            },
        )
    )
    ```

=== "CockroachPsycopg (Async)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncConfig

    spec = SQLSpec()
    config = spec.add_config(
        CockroachPsycopgAsyncConfig(
            connection_config={
                "conninfo": "postgresql://root@localhost:26257/defaultdb",
                "min_size": 2,
                "max_size": 20,
            },
            driver_features={"enable_auto_retry": True, "max_retries": 5},
        )
    )
    ```

=== "CockroachPsycopg (Sync)"

    ```{ .python title="docs/examples/drivers/cockroach_psycopg_connection.py" }
    --8<-- "docs/examples/drivers/cockroach_psycopg_connection.py:example"
    ```

## Connection & Pool Configuration

=== "cockroach_asyncpg"

    **Connection and Pool Parameters (`CockroachAsyncpgConnectionConfig`, `CockroachAsyncpgPoolConfig`)**

    <!-- GENERATED:START config-table:sqlspec.adapters.cockroach_asyncpg.config.CockroachAsyncpgConnectionConfig,sqlspec.adapters.cockroach_asyncpg.config.CockroachAsyncpgPoolConfig -->
    | Key | Type | Default | Description |
    | --- | --- | --- | --- |
    | `dsn` | `str` | - | CockroachDB PostgreSQL-compatible connection URI. |
    | `host` | `str` | - | CockroachDB node or load-balancer hostname. |
    | `port` | `int` | `26257` | CockroachDB SQL TCP port (defaults to `26257`). |
    | `user` | `str` | - | CockroachDB SQL username. |
    | `password` | `str` | - | Password or JWT token for `user`. |
    | `service` | `str` | - | Service profile name in `pg_service.conf`. |
    | `servicefile` | `str` | - | Path to an alternate `pg_service.conf` file. |
    | `database` | `str` | - | Target CockroachDB database name. |
    | `ssl` | `Any` | - | TLS mode string, boolean flag, or `ssl.SSLContext` instance. |
    | `passfile` | `str` | - | Path to a `pgpass` password file. |
    | `direct_tls` | `bool` | - | Initiate TLS handshake directly without `SSLRequest` negotiation. |
    | `timeout` | `float` | - | Connection establishment timeout in seconds. |
    | `connect_timeout` | `float` | - | Pool connection establishment timeout in seconds. |
    | `command_timeout` | `float` | - | Default per-command timeout in seconds. |
    | `application_name` | `str` | - | Application identifier forwarded via `server_settings`. |
    | `cluster` | `str` | - | CockroachDB Serverless / Cloud routing cluster identifier. |
    | `default_transaction_use_follower_reads` | `bool` | - | Session setting enabling bounded-staleness follower reads on read-only transactions. |
    | `results_buffer_size` | `int` | - | CockroachDB server result streaming buffer size in bytes. |
    | `statement_cache_size` | `int` | - | Maximum prepared statements cached per connection (`0` disables caching). |
    | `max_cached_statement_lifetime` | `int` | - | Maximum age in seconds before cached prepared statements are evicted. |
    | `max_cacheable_statement_size` | `int` | - | Maximum SQL byte length eligible for prepared statement caching. |
    | `server_settings` | `dict[str, str]` | - | Startup CockroachDB session variables passed at connection time. |
    | `target_session_attrs` | `CockroachAsyncpgTargetSessionAttrs` | - | Required node session attribute when connecting across multiple hosts. |
    | `statement_timeout` | `float or int or None` | - | Statement execution timeout in seconds forwarded via server_settings. |
    | `idle_in_transaction_session_timeout` | `float or int or None` | - | Idle in transaction timeout in seconds forwarded via server_settings. |
    | `krbsrvname` | `str` | - | Kerberos service principal name for GSSAPI authentication. |
    | `gsslib` | `CockroachAsyncpgGSSLib` | - | GSSAPI library implementation (`gssapi` or `sspi`). |
    | `min_size` | `int` | - | Minimum number of open connections maintained in the pool. |
    | `max_size` | `int` | - | Maximum number of concurrent connections allowed in the pool. |
    | `max_queries` | `int` | - | Number of queries after which a pooled connection is recycled. |
    | `max_inactive_connection_lifetime` | `float` | - | Idle timeout in seconds before closing unused connections above `min_size`. |
    | `connect` | `Callable[..., Awaitable[CockroachAsyncpgConnection]]` | - | Custom async connection factory passed to `asyncpg.create_pool`. |
    | `setup` | `Callable[[CockroachAsyncpgConnection], Awaitable[None]]` | - | Async hook run before each connection checkout from the pool. |
    | `init` | `Callable[[CockroachAsyncpgConnection], Awaitable[None]]` | - | Async hook run once when a new physical connection is opened. |
    | `reset` | `Callable[[CockroachAsyncpgConnection], Awaitable[None]]` | - | Async hook run when returning a connection to the pool. |
    | `loop` | `AbstractEventLoop` | - | Explicit `asyncio` event loop for pool tasks. |
    | `connection_class` | `type[CockroachAsyncpgConnection]` | - | Custom `asyncpg.Connection` subclass used by the pool. |
    | `record_class` | `type[Record]` | - | Custom `asyncpg.Record` subclass returned by raw queries. |
    | `extra` | `dict[str, Any]` | - | Additional keyword arguments forwarded to `asyncpg.create_pool`. |
    <!-- GENERATED:END config-table:sqlspec.adapters.cockroach_asyncpg.config.CockroachAsyncpgConnectionConfig,sqlspec.adapters.cockroach_asyncpg.config.CockroachAsyncpgPoolConfig -->

    **Driver Features (`CockroachAsyncpgDriverFeatures`)**

    <!-- GENERATED:START config-table:sqlspec.adapters.cockroach_asyncpg.config.CockroachAsyncpgDriverFeatures -->
    | Key | Type | Default | Description |
    | --- | --- | --- | --- |
    | `tenant_strategy` | `AsyncTenantStrategy or TenantStrategyShorthand or None` | - | Opt-in tenant strategy, or the `"rls"` shorthand, applied to every session and connection lease. |
    | `tenant_session_baseline` | `Mapping[str, str]` | - | Declared session settings replayed before each tenant checkout. |
    | `pooler_mode` | `Literal['session', 'transaction', 'statement', 'auto'] or None` | - | Tenant connections require session affinity. |
    | `enable_native_storage` | `bool` | - | Opt in to server-side CockroachDB `EXPORT` and `IMPORT INTO` storage operations. |
    | `native_storage_csv_options` | `_NativeStorageCSVOptions` | - | Explicit CSV markers mapping supporting `nullas` (export NULL string), `nullif` (import NULL string), and `skip` (header line count to skip on import). |
    | `enable_auto_retry` | `bool` | - | Automatically retry CockroachDB `40001` serialization failure transactions with backoff. |
    | `max_retries` | `int` | - | Maximum retry attempts on `40001` transaction serialization conflicts. |
    | `retry_delay_base_ms` | `float` | - | Initial exponential backoff delay in milliseconds between serialization retries. |
    | `retry_delay_max_ms` | `float` | - | Upper bound in milliseconds on exponential backoff delay between retries. |
    | `enable_retry_logging` | `bool` | - | Emit structured debug/warning logs when serialization retries occur. |
    | `enable_follower_reads` | `bool` | - | Allow `AS OF SYSTEM TIME` follower-read clauses on read queries. |
    | `default_staleness` | `str` | - | Default `AS OF SYSTEM TIME` staleness expression (such as `'-5s'` or `follower_read_timestamp()`). |
    | `json_serializer` | `Callable[[Any], str]` | `to_json` | Custom JSON serializer registered on `json`/`jsonb` codecs (defaults to `to_json`). |
    | `json_deserializer` | `Callable[[str], Any]` | `from_json` | Custom JSON deserializer registered on `json`/`jsonb` codecs (defaults to `from_json`). |
    | `enable_json_codecs` | `bool` | `True` | Register automatic `json` and `jsonb` codecs on new connections (defaults to `True`). |
    | `enable_pgvector` | `bool` | - | Register `pgvector` codecs for CockroachDB `VECTOR` columns when `pgvector` is installed. |
    | `on_connection_create` | `Callable[[CockroachAsyncpgConnection], Awaitable[None]]` | - | Async callback invoked with each raw `asyncpg` connection after codec initialization. |
    | `enable_events` | `bool` | - | Enable durable queue-backed SQLSpec event channel support. |
    | `events_backend` | `Literal['poll_queue']` | - | Event channel backend (`poll_queue` for CockroachDB table-backed polling). |
    <!-- GENERATED:END config-table:sqlspec.adapters.cockroach_asyncpg.config.CockroachAsyncpgDriverFeatures -->

=== "cockroach_psycopg"

    **Connection and Pool Parameters (`CockroachPsycopgConnectionConfig`, `CockroachPsycopgPoolConfig`)**

    <!-- GENERATED:START config-table:sqlspec.adapters.cockroach_psycopg.config.CockroachPsycopgConnectionConfig,sqlspec.adapters.cockroach_psycopg.config.CockroachPsycopgPoolConfig -->
    | Key | Type | Default | Description |
    | --- | --- | --- | --- |
    | `conninfo` | `str` | - | CockroachDB libpq connection URI or keyword/value conninfo string. |
    | `host` | `str` | - | CockroachDB node or load-balancer hostname. |
    | `port` | `int` | `26257` | CockroachDB SQL TCP port (defaults to `26257`). |
    | `user` | `str` | - | CockroachDB SQL username. |
    | `password` | `str` | - | Password or JWT token for `user`. |
    | `dbname` | `str` | - | Target CockroachDB database name. |
    | `connect_timeout` | `int` | - | Connection establishment timeout in seconds. |
    | `options` | `str` | - | Command-line startup session options sent to CockroachDB. |
    | `default_transaction_use_follower_reads` | `bool` | - | Enable bounded-staleness follower reads for read-only transactions. |
    | `results_buffer_size` | `int` | - | CockroachDB server result streaming buffer size in bytes. |
    | `statement_timeout` | `int` | - | Per-statement execution timeout in milliseconds. |
    | `idle_in_transaction_session_timeout` | `int` | - | Idle-in-transaction timeout in milliseconds before aborting open transactions. |
    | `application_name` | `str` | - | Application identifier visible in CockroachDB session diagnostics. |
    | `sslmode` | `str` | - | libpq TLS verification mode (such as `require` or `verify-full`). |
    | `sslcert` | `str` | - | Path to the client TLS certificate file. |
    | `sslkey` | `str` | - | Path to the client TLS private key file. |
    | `sslrootcert` | `str` | - | Path to the root CA certificate bundle. |
    | `autocommit` | `bool` | - | Enable Psycopg autocommit mode outside explicit transactions. |
    | `prepare_threshold` | `int or None` | - | Executions before Psycopg prepares a statement server-side, or `None` to disable. |
    | `row_factory` | `Callable[..., Any]` | - | Custom Psycopg row factory applied to raw connections. |
    | `cursor_factory` | `type[Any]` | - | Custom Psycopg cursor class used by raw connections. |
    | `context` | `Any` | - | Psycopg `AdaptContext` providing custom type adapters. |
    | `cluster` | `str` | - | CockroachDB Serverless / Cloud routing cluster identifier. |
    | `extra` | `dict[str, Any]` | - | Additional libpq keyword parameters merged into `connect`. |
    | `min_size` | `int` | - | Minimum number of connections maintained in `ConnectionPool` / `AsyncConnectionPool`. |
    | `max_size` | `int` | - | Maximum number of connections allowed in the pool. |
    | `name` | `str` | - | Diagnostic pool name used in logs and worker threads. |
    | `timeout` | `float` | - | Maximum wait time in seconds when acquiring a connection from the pool. |
    | `max_waiting` | `int` | - | Maximum queued callers waiting for an available connection (`0` means unlimited). |
    | `max_lifetime` | `float` | - | Maximum lifetime in seconds before a pooled connection is recycled. |
    | `max_idle` | `float` | - | Maximum idle time in seconds before shrinking connections above `min_size`. |
    | `reconnect_timeout` | `float` | - | Duration in seconds the pool retries failed connection attempts before invoking `reconnect_failed`. |
    | `reconnect_failed` | `Callable[..., Any]` | - | Callback invoked when pool reconnection attempts exceed `reconnect_timeout`. |
    | `num_workers` | `int` | - | Number of background maintenance worker threads/tasks managed by the pool. |
    | `configure` | `Callable[..., Any]` | - | Callback invoked once on each newly created connection before entering the pool. |
    | `check` | `Callable[..., Any]` | - | Health-check callback invoked before handing out a pooled connection. |
    | `reset` | `Callable[..., Any]` | - | Callback invoked to clean connection state when returned to the pool. |
    | `open` | `bool or None` | - | Whether to open the pool immediately during construction. |
    | `close_returns` | `bool` | - | Make `conn.close()` return the connection to the pool instead of closing the socket. |
    | `kwargs` | `dict[str, Any]` | - | Explicit connection keyword dictionary passed to `psycopg.connect`. |
    <!-- GENERATED:END config-table:sqlspec.adapters.cockroach_psycopg.config.CockroachPsycopgConnectionConfig,sqlspec.adapters.cockroach_psycopg.config.CockroachPsycopgPoolConfig -->

    **Driver Features (`CockroachPsycopgDriverFeatures`)**

    <!-- GENERATED:START config-table:sqlspec.adapters.cockroach_psycopg.config.CockroachPsycopgDriverFeatures -->
    | Key | Type | Default | Description |
    | --- | --- | --- | --- |
    | `tenant_strategy` | `SyncTenantStrategy or AsyncTenantStrategy or TenantStrategyShorthand or None` | - | Opt-in tenant strategy, or the `"rls"` shorthand, applied to every session and connection lease. |
    | `pooler_mode` | `Literal['session', 'transaction', 'statement', 'auto'] or None` | - | Tenant connections require session affinity. |
    | `enable_native_storage` | `bool` | - | Opt in to server-side CockroachDB `EXPORT` and `IMPORT INTO` storage operations. |
    | `native_storage_csv_options` | `_NativeStorageCSVOptions` | - | Explicit CSV markers mapping supporting `nullas` (export NULL string), `nullif` (import NULL string), and `skip` (header line count to skip on import). |
    | `enable_auto_retry` | `bool` | - | Automatically retry CockroachDB `40001` serialization failure transactions with backoff. |
    | `max_retries` | `int` | - | Maximum retry attempts on `40001` transaction serialization conflicts. |
    | `retry_delay_base_ms` | `float` | - | Initial exponential backoff delay in milliseconds between serialization retries. |
    | `retry_delay_max_ms` | `float` | - | Upper bound in milliseconds on exponential backoff delay between retries. |
    | `enable_retry_logging` | `bool` | - | Emit structured debug/warning logs when serialization retries occur. |
    | `enable_follower_reads` | `bool` | - | Allow `AS OF SYSTEM TIME` follower-read clauses on read queries. |
    | `statement_timeout_ms` | `int` | - | Statement execution timeout forwarded to the managed libpq session option. |
    | `default_staleness` | `str` | - | Default `AS OF SYSTEM TIME` staleness expression (such as `'-5s'` or `follower_read_timestamp()`). |
    | `json_serializer` | `Callable[[Any], str]` | `to_json` | Custom JSON serializer applied to statement parameters (defaults to `to_json`). |
    | `json_deserializer` | `Callable[[str], Any]` | `from_json` | Custom JSON deserializer retained alongside `json_serializer` (defaults to `from_json`). |
    | `on_connection_create` | `Callable[..., Any]` | - | Sync or async callback invoked with each raw Psycopg connection after internal setup. |
    | `enable_events` | `bool` | - | Enable durable queue-backed SQLSpec event channel support. |
    | `events_backend` | `Literal['poll_queue']` | - | Event channel backend (`poll_queue` for CockroachDB table-backed polling). |
    <!-- GENERATED:END config-table:sqlspec.adapters.cockroach_psycopg.config.CockroachPsycopgDriverFeatures -->

## Type Mappings & Dialect Notes

| Database Type | Python Type | Notes / Feature Flag |
| --- | --- | --- |
| `JSONB` / `JSON` | `dict` / `list` | Encoded with `json_serializer` and decoded with `json_deserializer`. `cockroach_asyncpg` registers binary codecs when `enable_json_codecs=True` (default); `cockroach_psycopg` installs the serializer through Psycopg's JSON dumpers. |
| `UUID` | `uuid.UUID` / `str` | Recommended distributed primary key type (`UUID DEFAULT gen_random_uuid()`). |
| `INT8` / `INT64` | `int` | CockroachDB's default 64-bit integer type (`unique_rowid()`). |
| `DECIMAL` / `NUMERIC` | `decimal.Decimal` | Exact arbitrary-precision decimal arithmetic. |
| `TIMESTAMPTZ` / `TIMESTAMP` | `datetime.datetime` | Timezone-aware and naive timestamp mapping. |
| `BYTES` / `BYTEA` | `bytes` / `bytearray` | Binary wire transport. |

- **Automatic 40001 Serialization Retry**: CockroachDB runs transactions at `SERIALIZABLE` isolation by default. Contended transactions raise `SQLSTATE 40001` (`SerializationConflictError`); when `enable_auto_retry=True` (default), SQLSpec retries operations using `CockroachAsyncpgRetryConfig` / `CockroachPsycopgRetryConfig` (`max_retries`, `retry_delay_base_ms`, `retry_delay_max_ms`, `enable_retry_logging`) with jittered exponential backoff.
- **Follower Reads (`AS OF SYSTEM TIME`)**: When `enable_follower_reads=True`, read-only queries execute inside an explicit read-only transaction with `SET TRANSACTION AS OF SYSTEM TIME <default_staleness>` (such as `"-10s"` or `"follower_read_timestamp()"`) and commit cleanly afterward so pooled connections never leak stale-read state.
- **Distributed Primary Keys**: Prefer `UUID DEFAULT gen_random_uuid()` over monotonic `SERIAL` sequences on write-heavy tables to avoid single-range hotspotting across CockroachDB nodes.
- **Transactional Event Queue**: Because CockroachDB does not implement PostgreSQL `LISTEN/NOTIFY`, `EventChannel` uses SQLSpec's transactional table-backed queue store (`CockroachAsyncpgEventQueueStore`, `CockroachPsycopgSyncEventQueueStore`, `CockroachPsycopgAsyncEventQueueStore`) automatically.

## Follower Reads and Read-Only Sessions

`CockroachAsyncpgConfig`, `CockroachPsycopgSyncConfig`, and `CockroachPsycopgAsyncConfig` can serve follower reads from one shared pool. `provide_read_session()` defaults to `staleness="follower_read_timestamp()"` and applies a native `AS OF SYSTEM TIME` read. A supported bounded-staleness interval can choose an older timestamp explicitly.

Follower reads can be stale; they are not read-after-write primary reads. Their driver rejects writes with `ReadOnlyTransactionError`. A primary routing scope bypasses the follower path. Use separate endpoint pools only when your topology needs them. See [Replica Operations](../learn/recipes/read-replicas-operations.md).

## See Also

- [Cockroach AsyncPG API Reference](/reference/adapters/cockroach-asyncpg/)
- [Cockroach Psycopg API Reference](/reference/adapters/cockroach-psycopg/)
- [CockroachDB Dialect Reference](/reference/data-dictionary/dialects/cockroachdb/)
