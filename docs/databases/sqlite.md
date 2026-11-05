# SQLite and Aiosqlite

SQLSpec ships `mypyc`-compiled synchronous (`sqlspec.adapters.sqlite`) and asynchronous (`sqlspec.adapters.aiosqlite`) SQLite adapters with managed connection pools, automatic `PRAGMA` initialization, shared-memory `:memory:` URI support, and custom Python SQL functions, collations, aggregates, and window functions.

## Overview & Capabilities

| Adapter Config | Driver Extra | Execution | Pool Architecture | Best For |
| --- | --- | --- | --- | --- |
| `SqliteConfig` | `sqlspec` (stdlib `sqlite3`) | Sync | `SqliteConnectionPool` (thread-local connection manager with `pool_recycle_seconds`, `health_check_interval`) | CLI tools, synchronous services, embedded local storage, and sub-millisecond unit tests. |
| `AiosqliteConfig` | `sqlspec[aiosqlite]` | Async (`asyncio`) | `AiosqliteConnectionPool` (bounded async queue pool with `pool_size`, `min_size`, `idle_timeout`) | `asyncio` frameworks (Litestar, FastAPI, Starlette, Google ADK) running SQLite on worker threads. |

## Installation & Quickstart

=== "uv"

    ```bash
    uv add sqlspec
    # or async: uv add "sqlspec[aiosqlite]"
    ```

=== "pip"

    ```bash
    pip install sqlspec
    # or async: pip install "sqlspec[aiosqlite]"
    ```

=== "pdm"

    ```bash
    pdm add sqlspec
    # or async: pdm add "sqlspec[aiosqlite]"
    ```

=== "poetry"

    ```bash
    poetry add sqlspec
    # or async: poetry add "sqlspec[aiosqlite]"
    ```

=== "SQLite (Sync)"

    ```{ .python title="docs/examples/drivers/sqlite_connection.py" }
    --8<-- "docs/examples/drivers/sqlite_connection.py:example"
    ```

=== "Aiosqlite (Async)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    spec = SQLSpec()
    config = spec.add_config(
        AiosqliteConfig(
            connection_config={
                "database": "var/app.db",
                "timeout": 30.0,
                "pool_size": 5,
                "min_size": 1,
            },
            driver_features={
                "pragmas": {
                    "journal_mode": "WAL",
                    "synchronous": "NORMAL",
                    "foreign_keys": "ON",
                    "busy_timeout": 5000,
                }
            },
        )
    )
    ```

## Connection & Pool Configuration

=== "SQLite (Sync)"

    > [!NOTE]
    > `SqliteConnectionPool` is a thread-local connection manager that recycles connections via `pool_recycle_seconds` and `health_check_interval` directly on `SqliteConnectionParams`. When `database=":memory:"` is configured, `SqliteConfig` automatically normalizes it to a unique shared-cache URI (`file:sqlspec_memory_<id>?mode=memory&cache=shared`, `uri=True`) so tables persist across thread-local checkouts.

    **Connection and Pool Parameters (`SqliteConnectionParams`)**

    <!-- GENERATED:START config-table:sqlspec.adapters.sqlite.config.SqliteConnectionParams -->
    | Key | Type | Default | Description |
    | --- | --- | --- | --- |
    | `database` | `str or PathLike[str]` | - | Filesystem path, `':memory:'`, or `file:...` URI for the SQLite database. |
    | `timeout` | `float` | - | Busy lock wait timeout in seconds passed to `sqlite3.connect`. |
    | `detect_types` | `int` | - | Bitmask of `sqlite3.PARSE_DECLTYPES` and `sqlite3.PARSE_COLNAMES` flags. |
    | `isolation_level` | `Literal['DEFERRED', 'IMMEDIATE', 'EXCLUSIVE'] or None` | - | Implicit transaction begin mode, or `None` for manual/autocommit control. |
    | `check_same_thread` | `bool` | - | Restrict raw `sqlite3.Connection` usage to the creating OS thread. |
    | `factory` | `SqliteConnectionFactory or None` | - | Custom `sqlite3.Connection` subclass passed to `sqlite3.connect`. |
    | `cached_statements` | `int` | - | Number of compiled SQLite VM statements cached per connection by `sqlite3`. |
    | `uri` | `bool` | - | Interpret `database` as a SQLite `file:` URI supporting query parameters. |
    | `autocommit` | `SQLiteAutocommitMode` | - | PEP 249 / Python 3.12+ autocommit control flag or `sqlite3.LEGACY_TRANSACTION_CONTROL` (`-1`). |
    | `pool_recycle_seconds` | `int` | - | Maximum connection age in seconds before recycling pooled SQLite connections. |
    | `health_check_interval` | `float` | - | Minimum interval in seconds between connection liveness probes in `SqliteConnectionPool`. |
    | `enable_optimizations` | `bool` | - | Apply recommended SQLite `PRAGMA` settings (WAL journal mode, synchronous=NORMAL, busy_timeout) on connect. |
    | `enable_foreign_keys` | `bool` | `False` | Execute `PRAGMA foreign_keys = ON` when initializing each connection (defaults to `False`). |
    | `extra` | `dict[str, Any]` | - | Additional keyword arguments forwarded to `sqlite3.connect`. |
    <!-- GENERATED:END config-table:sqlspec.adapters.sqlite.config.SqliteConnectionParams -->

    **Driver Features (`SqliteDriverFeatures`)**

    <!-- GENERATED:START config-table:sqlspec.adapters.sqlite.config.SqliteDriverFeatures -->
    | Key | Type | Default | Description |
    | --- | --- | --- | --- |
    | `enable_custom_adapters` | `bool` | `False` | Register global `sqlite3` module type adapters/converters (defaults to `False`). |
    | `json_serializer` | `Callable[[Any], str]` | `to_json` | Custom JSON serializer used for bound parameters (defaults to `to_json`). |
    | `json_deserializer` | `Callable[[str], Any]` | `from_json` | Custom JSON deserializer used for JSON column decoding (defaults to `from_json`). |
    | `on_connection_create` | `Callable[[SqliteConnection], None]` | - | Synchronous hook called with each raw `sqlite3.Connection` after PRAGMA setup. |
    | `enable_events` | `bool` | - | Enable durable table-backed SQLSpec event queue support. |
    | `events_backend` | `Literal['poll_queue']` | - | Event channel backend (`poll_queue` is the sole SQLite backend). |
    | `custom_functions` | `Sequence[SqliteFunctionConfig]` | - | Scalar SQL functions registered via `conn.create_function` on each connection. |
    | `custom_collations` | `Sequence[SqliteCollationConfig]` | - | Custom string collations registered via `conn.create_collation` on each connection. |
    | `custom_aggregates` | `Sequence[SqliteAggregateConfig]` | - | Custom SQL aggregate classes registered via `conn.create_aggregate`. |
    | `custom_window_functions` | `Sequence[SqliteWindowFunctionConfig]` | - | Custom SQL window function classes registered via `conn.create_window_function`. |
    | `default_transaction_mode` | `Literal['DEFERRED', 'IMMEDIATE', 'EXCLUSIVE']` | - | Default `BEGIN` locking mode used by `begin()` (`DEFERRED`, `IMMEDIATE`, or `EXCLUSIVE`). |
    | `authorizer_callback` | `Callable[[int, str or None, str or None, str or None, str or None], int]` | - | SQLite compile-time access control hook registered via `conn.set_authorizer`. |
    | `trace_callback` | `Callable[[str], None]` | - | Statement trace callback registered via `conn.set_trace_callback`. |
    | `progress_handler` | `Callable[[], int or None]` | - | SQLite VM progress hook invoked every `progress_handler_interval` opcodes. |
    | `progress_handler_interval` | `int` | - | Positive VM opcode count between `progress_handler` invocations. |
    | `row_factory` | `Literal['row', 'dict', 'tuple'] or Callable[..., Any]` | - | Raw connection row factory selector (`'row'`, `'dict'`, `'tuple'`) or custom callable. |
    | `text_factory` | `Callable[[bytes], Any]` | - | Decoder callable assigned to `conn.text_factory` for `TEXT` columns. |
    | `pragmas` | `Mapping[str, str or int or bool]` | - | Additional `PRAGMA` key/value overrides applied after built-in connection optimizations. |
    | `extensions` | `Sequence[str]` | - | Shared-library SQLite extension paths loaded via `conn.load_extension`. |
    <!-- GENERATED:END config-table:sqlspec.adapters.sqlite.config.SqliteDriverFeatures -->

=== "Aiosqlite (Async)"

    > [!NOTE]
    > `AiosqliteConnectionPool` is a bounded async queue pool configured via `pool_size` (default `5`), `min_size` (default `0`), `connect_timeout`, `idle_timeout`, `operation_timeout`, and `health_check_interval`.

    **Connection and Pool Parameters (`AiosqliteConnectionParams`, `AiosqlitePoolParams`)**

    <!-- GENERATED:START config-table:sqlspec.adapters.aiosqlite.config.AiosqliteConnectionParams,sqlspec.adapters.aiosqlite.config.AiosqlitePoolParams -->
    | Key | Type | Default | Description |
    | --- | --- | --- | --- |
    | `database` | `str or PathLike[str]` | - | Filesystem path, `':memory:'`, or `file:...` URI for the SQLite database. |
    | `timeout` | `float` | - | Busy lock wait timeout in seconds passed to `aiosqlite.connect`. |
    | `detect_types` | `int` | - | Bitmask of `sqlite3.PARSE_DECLTYPES` and `sqlite3.PARSE_COLNAMES` flags. |
    | `isolation_level` | `SQLiteIsolationLevel` | - | Implicit transaction begin mode, or `None` for manual transaction management. |
    | `check_same_thread` | `bool` | - | Restrict underlying `sqlite3.Connection` access to the `aiosqlite` worker thread. |
    | `factory` | `AiosqliteConnectionFactory or None` | - | Custom `sqlite3.Connection` subclass instantiated on the worker thread. |
    | `cached_statements` | `int` | - | Number of compiled SQLite VM statements cached per connection. |
    | `uri` | `bool` | - | Interpret `database` as a SQLite `file:` URI supporting query parameters. |
    | `iter_chunk_size` | `int` | - | Row batch size fetched per worker-thread hop when iterating `aiosqlite` cursors. |
    | `autocommit` | `SQLiteAutocommitMode` | - | PEP 249 / Python 3.12+ autocommit flag or `sqlite3.LEGACY_TRANSACTION_CONTROL` (`-1`). |
    | `pool_size` | `int` | - | Maximum number of worker-backed SQLite connections managed by `AiosqliteConnectionPool`. |
    | `min_size` | `int` | - | Minimum number of warm connections pre-opened in the pool. |
    | `connect_timeout` | `float` | - | Maximum wait time in seconds when acquiring a connection from the pool. |
    | `idle_timeout` | `float` | - | Idle duration in seconds before recycling unused pooled connections. |
    | `operation_timeout` | `float` | - | Maximum duration in seconds allowed for pool lifecycle operations. |
    | `health_check_interval` | `float` | - | Interval in seconds between background or checkout connection health checks. |
    | `enable_optimizations` | `bool` | - | Apply WAL journal mode, `synchronous=NORMAL`, and busy-timeout PRAGMAs on connect. |
    | `enable_foreign_keys` | `bool` | `False` | Execute `PRAGMA foreign_keys = ON` on every pooled connection (defaults to `False`). |
    | `extra` | `dict[str, Any]` | - | Additional keyword arguments forwarded directly to `aiosqlite.connect`. |
    <!-- GENERATED:END config-table:sqlspec.adapters.aiosqlite.config.AiosqliteConnectionParams,sqlspec.adapters.aiosqlite.config.AiosqlitePoolParams -->

    **Driver Features (`AiosqliteDriverFeatures`)**

    <!-- GENERATED:START config-table:sqlspec.adapters.aiosqlite.config.AiosqliteDriverFeatures -->
    | Key | Type | Default | Description |
    | --- | --- | --- | --- |
    | `enable_custom_adapters` | `bool` | `False` | Register global `sqlite3` module type adapters/converters (defaults to `False`). |
    | `json_serializer` | `Callable[[Any], str]` | `to_json` | Custom JSON serializer used for bound parameters (defaults to `to_json`). |
    | `json_deserializer` | `Callable[[str], Any]` | `from_json` | Custom JSON deserializer used for JSON column decoding (defaults to `from_json`). |
    | `on_connection_create` | `Callable[[AiosqliteConnection], Awaitable[None]]` | - | Async hook invoked with each `aiosqlite.Connection` after PRAGMA initialization. |
    | `enable_events` | `bool` | - | Enable durable table-backed SQLSpec event queue support. |
    | `events_backend` | `Literal['poll_queue']` | - | Event channel backend (`poll_queue` is the sole SQLite backend). |
    | `custom_functions` | `Sequence[AiosqliteFunctionConfig]` | - | Scalar SQL functions registered on the `aiosqlite` worker thread. |
    | `custom_collations` | `Sequence[AiosqliteCollationConfig]` | - | Custom SQL collations registered on the `aiosqlite` worker thread. |
    | `custom_aggregates` | `Sequence[AiosqliteAggregateConfig]` | - | Custom SQL aggregate classes registered on each connection. |
    | `custom_window_functions` | `Sequence[AiosqliteWindowFunctionConfig]` | - | Custom SQL window function classes registered on each connection. |
    | `default_transaction_mode` | `Literal['DEFERRED', 'IMMEDIATE', 'EXCLUSIVE']` | - | Default `BEGIN` locking mode used by `begin()` (`DEFERRED`, `IMMEDIATE`, or `EXCLUSIVE`). |
    | `authorizer_callback` | `Callable[[int, str or None, str or None, str or None, str or None], int]` | - | SQLite compile-time authorizer callback executed on the worker thread. |
    | `trace_callback` | `Callable[[str], None]` | - | Statement trace callback executed on the worker thread. |
    | `progress_handler` | `Callable[[], int or None]` | - | SQLite VM progress hook invoked every `progress_handler_interval` opcodes. |
    | `progress_handler_interval` | `int` | - | Positive VM opcode count between `progress_handler` invocations. |
    | `row_factory` | `Literal['row', 'dict', 'tuple'] or Callable[..., Any]` | - | Raw connection row factory selector (`'row'`, `'dict'`, `'tuple'`) or custom callable. |
    | `text_factory` | `Callable[[bytes], Any]` | - | Decoder callable assigned to `conn.text_factory` for `TEXT` columns. |
    | `pragmas` | `Mapping[str, str or int or bool]` | - | Additional `PRAGMA` key/value overrides applied after built-in optimizations. |
    | `extensions` | `Sequence[str]` | - | Shared-library SQLite extension paths loaded on each connection. |
    <!-- GENERATED:END config-table:sqlspec.adapters.aiosqlite.config.AiosqliteDriverFeatures -->

## Type Mappings & Dialect Notes

| Database Type | Python Type | Notes / Feature Flag |
| --- | --- | --- |
| `TEXT` (ISO-8601) | `str` / `datetime.datetime` / `datetime.date` | ISO-8601 date and timestamp strings convert via SQLSpec's type coercion pipeline. |
| `INTEGER` | `int` / `bool` | Booleans store as `0` and `1` in SQLite `INTEGER` affinity columns. |
| `REAL` / `NUMERIC` | `float` / `decimal.Decimal` | Floating-point and exact decimal parameter conversion. |
| `JSON` / `TEXT` | `dict` / `list` | Serialized via `json_serializer` and `json_deserializer`; compatible with SQLite `json_extract` and `JSONB`. |
| `BLOB` | `bytes` / `bytearray` | Raw binary column binding. |
| `TEXT` / `BLOB` (`UUID`) | `uuid.UUID` / `str` | UUID parameters bind cleanly as string or bytes representations. |

- **WAL Mode and PRAGMA Tuning**: Pass `driver_features={"pragmas": {"journal_mode": "WAL", "synchronous": "NORMAL", "foreign_keys": "ON", "busy_timeout": 5000}}` (or an `on_connection_create` callback) so every pooled connection applies production concurrency settings on creation.
- **BEGIN IMMEDIATE Concurrency on Aiosqlite**: Under concurrent `asyncio` writes, starting with `DEFERRED` and upgrading to a write lock mid-transaction causes immediate `SQLITE_BUSY` errors. `AiosqliteConfig` defaults `default_transaction_mode` to `"IMMEDIATE"` so writers wait cleanly up to `busy_timeout`.
- **Custom Functions, Collations, Aggregates, and Window Functions**: Register Python callables across every pooled connection using `custom_functions` (`SqliteFunctionConfig` / `AiosqliteFunctionConfig`), `custom_collations` (`SqliteCollationConfig` / `AiosqliteCollationConfig`), `custom_aggregates` (`SqliteAggregateConfig` / `AiosqliteAggregateConfig`), and `custom_window_functions` (`SqliteWindowFunctionConfig` / `AiosqliteWindowFunctionConfig`).

## See Also

- [SQLite API Reference](/reference/adapters/sqlite/)
- [Aiosqlite API Reference](/reference/adapters/aiosqlite/)
- [SQLite Dialect Reference](/reference/data-dictionary/dialects/sqlite/)
