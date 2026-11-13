# IBM Db2

`sqlspec.adapters.db2` supports IBM Db2 LUW 11.5+ via `ibm_db` (`>= 3.3.0`), offering both synchronous (`Db2SyncConfig`) and `asyncio` (`Db2AsyncConfig`) managed connection pools, custom SQLGlot `db2` dialect transpilation, automatic lowercase column normalization, and Litestar, Events, and Google ADK session/memory store extensions.

## Overview & Capabilities

| Adapter Config | Driver Extra | Execution | Pool Architecture | Best For |
| --- | --- | --- | --- | --- |
| `Db2AsyncConfig` | `sqlspec[db2]` | Async (`asyncio`) | `Db2AsyncConnectionPool` (`max_size`, `acquire_timeout`, `pool_recycle_seconds`, `health_check_interval`) | Async HTTP services and ADK agents communicating with IBM Db2 over DRDA. |
| `Db2SyncConfig` | `sqlspec[db2]` | Sync | `Db2SyncConnectionPool` (thread-local manager with `pool_recycle_seconds`, `health_check_interval`) | Synchronous applications, batch jobs, and CLI migrations targeting IBM Db2 LUW. |

> [!NOTE]
> IBM publishes prebuilt `clidriver` wheels for Linux `x86_64`, macOS `arm64`, and Windows. On Linux `aarch64` hosts, connect to Db2 via [`adbc`](adbc.md) (`adbc-driver-db2` pure-Go DRDA wheel) or pair [`arrow_odbc`](arrow-odbc.md) (`dialect="db2"` over 64-bit `libdb2o.so`).

## Installation & Quickstart

=== "uv"

    ```bash
    uv add "sqlspec[db2]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[db2]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[db2]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[db2]"
    ```

=== "Async (Db2AsyncConfig)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.db2 import Db2AsyncConfig

    spec = SQLSpec()
    config = spec.add_config(
        Db2AsyncConfig(
            connection_config={
                "database": "appdb",
                "hostname": "localhost",
                "port": 50000,
                "user": "db2inst1",
                "password": "secret",
                "current_schema": "APP",
                "max_size": 10,
                "acquire_timeout": 30.0,
            }
        )
    )
    ```

=== "Sync (Db2SyncConfig)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.db2 import Db2SyncConfig

    spec = SQLSpec()
    config = spec.add_config(
        Db2SyncConfig(
            connection_config={
                "database": "appdb",
                "hostname": "localhost",
                "port": 50000,
                "user": "db2inst1",
                "password": "secret",
                "current_schema": "APP",
            }
        )
    )
    ```

## Connection & Pool Configuration

### Connection and Pool Parameters

Pass `Db2ConnectionParams`, `Db2PoolParams`, and `Db2AsyncPoolParams` keys inside `connection_config={...}` on `Db2SyncConfig` or `Db2AsyncConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.db2.config.Db2ConnectionParams,sqlspec.adapters.db2.config.Db2PoolParams,sqlspec.adapters.db2.config.Db2AsyncPoolParams -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `database` | `str` | - | Database name or cataloged alias mapped to the `DATABASE` CLI keyword. |
| `hostname` | `str` | - | Db2 server hostname mapped to `HOSTNAME` (omit for cataloged local aliases). |
| `port` | `int` | `50000` | Db2 TCP listener port mapped to `PORT` (defaults to `50000` when `hostname` is set). |
| `protocol` | `str` | `'TCPIP'` | Communications protocol mapped to `PROTOCOL` (defaults to `'TCPIP'` when `hostname` is set). |
| `user` | `str` | - | Authentication username mapped to the `UID` CLI keyword. |
| `password` | `str` | - | Authentication password mapped to the `PWD` CLI keyword. |
| `current_schema` | `str` | - | Default SQL schema mapped to the `CURRENTSCHEMA` CLI keyword. |
| `security` | `str` | - | Transport security mode mapped to `SECURITY` (for example `'SSL'`). |
| `ssl_server_certificate` | `str` | - | Server certificate file path mapped to the `SSLSERVERCERTIFICATE` CLI keyword. |
| `authentication` | `str` | - | Db2 authentication mechanism mapped to the `AUTHENTICATION` CLI keyword. |
| `connect_timeout` | `int` | - | Connection establishment timeout in seconds mapped to `CONNECTTIMEOUT`. |
| `autocommit` | `bool` | - | Initial connection autocommit state applied via `ibm_db_dbi` outside the DSN. |
| `dsn` | `str` | - | Base `KEY=VALUE;...` string or `db2://` URL overridden by explicit keyword parameters. |
| `extra` | `dict[str, str or int or bool]` | - | Additional CLI keywords appended verbatim to the compiled DSN string. |
| `pool_recycle_seconds` | `int` | `86400` | Maximum connection age in seconds before replacement (defaults to `86400`). |
| `health_check_interval` | `float` | `30.0` | Idle seconds elapsed before a pooled connection is pinged on reuse (defaults to `30.0`). |
| `max_size` | `int` | `10` | Maximum number of connections checked out or opening concurrently (defaults to `10`). |
| `acquire_timeout` | `float` | `30.0` | Maximum seconds to wait for a free connection before raising `ConnectionTimeoutError` (defaults to `30.0`). |
<!-- GENERATED:END config-table:sqlspec.adapters.db2.config.Db2ConnectionParams,sqlspec.adapters.db2.config.Db2PoolParams,sqlspec.adapters.db2.config.Db2AsyncPoolParams -->

### Driver Features

Pass `Db2DriverFeatures` keys inside `driver_features={...}` on `Db2SyncConfig` or `Db2AsyncConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.db2.config.Db2DriverFeatures -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `tenant_strategy` | `SyncTenantStrategy or AsyncTenantStrategy or TenantStrategyShorthand or None` | - | Opt-in tenant strategy, or the `"rcac"` shorthand, applied to every session and connection lease. |
| `json_serializer` | `Callable[[Any], str]` | `sqlspec.utils.serializers.to_json` | Serializer converting dict/list parameters to JSON text (defaults to `sqlspec.utils.serializers.to_json`). |
| `json_deserializer` | `Callable[[str], Any]` | `sqlspec.utils.serializers.from_json` | Deserializer parsing JSON text payloads (defaults to `sqlspec.utils.serializers.from_json`). |
| `on_connection_create` | `Callable[[Any], Any]` | - | Callback invoked with the raw `ibm_db_dbi` sync or async connection after open. |
| `enable_events` | `bool` | - | Enable SQLSpec table-backed event channel support. |
| `events_backend` | `Literal['poll_queue']` | - | Event queue backend name (`'poll_queue'`). |
| `enable_lowercase_column_names` | `bool` | `True` | Normalize unquoted uppercase Db2 column names to lowercase (defaults to `True`). |
<!-- GENERATED:END config-table:sqlspec.adapters.db2.config.Db2DriverFeatures -->

## Type Mappings & Dialect Notes

| Database Type | Python Type | Notes / Feature Flag |
| --- | --- | --- |
| `CLOB` / `VARCHAR` (JSON) | `dict` / `list` | Serialized and deserialized via `json_serializer` and `json_deserializer`. |
| `DECIMAL` / `DECFLOAT` | `decimal.Decimal` | Exact arbitrary-precision decimal arithmetic. |
| `TIMESTAMP` / `DATE` | `datetime.datetime` / `datetime.date` | Native Db2 timestamp and date conversion. |
| `BLOB` / `VARBINARY` | `bytes` / `bytearray` | Binary payload storage. |
| `BOOLEAN` / `SMALLINT` | `bool` / `int` | Native Db2 `BOOLEAN` and `0`/`1` integer flags. |
| `VARCHAR(36)` / `CHAR(16) FOR BIT DATA` | `uuid.UUID` / `str` | UUID string and binary parameter coercion. |

- **Data-Change-Table Returning Syntax (`FINAL TABLE`)**: Db2 compiles `INSERT ... RETURNING` projections using `SELECT ... FROM FINAL TABLE (INSERT INTO ...)` syntax via SQLSpec's custom `db2` SQLGlot dialect, and supports standard `MERGE INTO` upserts.
- **Strict Connection Key Validation**: `Db2SyncConfig` and `Db2AsyncConfig` validate `connection_config` keys eagerly, raising `ImproperConfigurationError` if unknown aliases such as `host`, `username`, or `min_size` are passed instead of `hostname`, `user`, and `max_size`.
- **Savepoint Cursor Retention**: Db2 closes open cursors on savepoint rollback unless `ON ROLLBACK RETAIN CURSORS` is specified; SQLSpec emits `SAVEPOINT <name> ON ROLLBACK RETAIN CURSORS` and `RELEASE SAVEPOINT <name>` automatically.
- **Event Queue and ADK Stores**: `Db2SyncEventQueueStore` and `Db2AsyncEventQueueStore` claim rows with `FETCH FIRST n ROWS ONLY` and `WITH RS USE AND KEEP UPDATE LOCKS SKIP LOCKED DATA` so concurrent queue workers skip rows that are already claimed. `Db2SyncADKMemoryStore` and `Db2AsyncADKMemoryStore` match text with `LOCATE(LOWER(?), LOWER(content_text)) > 0` for case-insensitive search.

## HADR Reader Pools

Pass explicit HADR reader endpoints through `replicas=` with `read_only=True`. The adapter sets `SQL_ATTR_ACCESS_MODE=SQL_MODE_READ_ONLY` before it yields the socket. Set up the reader database, role, and HADR rules on the server first.

SQLCODE `-817`, `-1773`, and `-1776` keep their mapped read-only or unavailable meaning. `derive_pool()` keeps tenant hooks and explicit Db2 options. See [Read Replicas](../learn/recipes/read-replicas.md) for group choice and primary fallback.

## See Also

- [Db2 API Reference](/reference/adapters/db2/)
- [IBM Db2 Dialect Reference](/reference/data-dictionary/dialects/db2/)
- [Apache Arrow ADBC Guide](adbc.md)
- [Arrow ODBC Guide](arrow-odbc.md)
