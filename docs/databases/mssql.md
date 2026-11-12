# Microsoft SQL Server

SQLSpec provides two synchronous Microsoft SQL Server and Azure SQL adapters—`sqlspec.adapters.mssql_python` (`MssqlPythonConfig`) and `sqlspec.adapters.pymssql` (`PymssqlConfig`)—plus zero-copy columnar ODBC streaming via [`arrow_odbc`](arrow-odbc.md).

## Overview & Capabilities

| Adapter Config | Driver Extra | Execution | Pool Architecture | Best For |
| --- | --- | --- | --- | --- |
| `MssqlPythonConfig` | `sqlspec[mssql-python]` | Sync | `MssqlPythonConnectionPool` (`pool_size`, `pool_idle_timeout`, `pool_enabled`) | Official Microsoft driver with Azure Entra ID (`ActiveDirectoryDefault`) auth and native Arrow `BulkCopy` ingestion. |
| `PymssqlConfig` | `sqlspec[pymssql]` | Sync | `PymssqlConnectionPool` (thread-local manager with `pool_recycle_seconds`, `health_check_interval`) | Lightweight FreeTDS-based synchronous driver for classic SQL authentication and portable Linux containers. |

## Installation & Quickstart

=== "uv"

    ```bash
    uv add "sqlspec[mssql-python]"
    # or: uv add "sqlspec[pymssql]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[mssql-python]"
    # or: pip install "sqlspec[pymssql]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[mssql-python]"
    # or: pdm add "sqlspec[pymssql]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[mssql-python]"
    # or: poetry add "sqlspec[pymssql]"
    ```

=== "mssql-python (Sync)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.mssql_python import MssqlPythonConfig

    spec = SQLSpec()
    config = spec.add_config(
        MssqlPythonConfig(
            connection_config={
                "server": "localhost",
                "port": 1433,
                "database": "app_db",
                "user": "sa",
                "password": "StrongPassword123",
                "encrypt": "mandatory",
                "trust_server_certificate": True,
                "pool_size": 10,
            }
        )
    )
    ```

=== "pymssql (Sync)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.pymssql import PymssqlConfig

    spec = SQLSpec()
    config = spec.add_config(
        PymssqlConfig(
            connection_config={
                "server": "localhost",
                "port": 1433,
                "user": "sa",
                "password": "StrongPassword123",
                "database": "app_db",
                "encryption": "request",
                "pool_recycle_seconds": 3600,
                "health_check_interval": 30,
            }
        )
    )
    ```

## Connection & Pool Configuration

=== "mssql-python (Sync)"

    **Connection and Pool Parameters (`MssqlPythonConnectionParams`, `MssqlPythonPoolParams`)**

    <!-- GENERATED:START config-table:sqlspec.adapters.mssql_python.config.MssqlPythonConnectionParams,sqlspec.adapters.mssql_python.config.MssqlPythonPoolParams -->
    | Key | Type | Default | Description |
    | --- | --- | --- | --- |
    | `connection_string` | `str` | - | Full ODBC/TDS semicolon-delimited connection string passed to `mssql_python.connect`. |
    | `server` | `str` | - | SQL Server hostname, `host,port`, or `host\instance` target. |
    | `address` | `str` | - | ODBC `Address` alias for `server`. |
    | `addr` | `str` | - | ODBC `Addr` alias for `server`. |
    | `port` | `int` | `1433` | SQL Server TCP port (defaults to `1433` when omitted). |
    | `database` | `str` | - | Initial SQL Server or Azure SQL database name. |
    | `db` | `str` | - | Alias normalized to `Database` in the connection string. |
    | `uid` | `str` | - | SQL authentication login ID. |
    | `user` | `str` | - | Alias normalized to `UID` in the connection string. |
    | `username` | `str` | - | Alias normalized to `UID` in the connection string. |
    | `pwd` | `str` | - | SQL authentication password. |
    | `password` | `str` | - | Alias normalized to `PWD` in the connection string. |
    | `authentication` | `str` | - | ODBC `Authentication` mode (such as `ActiveDirectoryDefault` or `SqlPassword`). |
    | `trusted_connection` | `bool or str` | - | Use integrated Windows / Kerberos authentication (`yes`/`no`). |
    | `trust_server_certificate` | `bool or str` | - | Bypass TLS server certificate chain validation (`yes`/`no`). |
    | `trust` | `bool or str` | - | Short alias for `trust_server_certificate`. |
    | `encrypt` | `bool or str` | - | TLS encryption policy (`yes`, `no`, `strict`, `mandatory`, or `optional`). |
    | `hostname_in_certificate` | `str` | - | Expected DNS subject name validated in the server TLS certificate. |
    | `hostnameincertificate` | `str` | - | ODBC keyword alias for `hostname_in_certificate`. |
    | `server_certificate` | `str` | - | Filesystem path to an expected server certificate file for pinning. |
    | `servercertificate` | `str` | - | ODBC keyword alias for `server_certificate`. |
    | `server_spn` | `str` | - | Kerberos Service Principal Name for the SQL Server target. |
    | `serverspn` | `str` | - | ODBC keyword alias for `server_spn`. |
    | `multi_subnet_failover` | `bool or str` | - | Enable parallel IP connection attempts for Always On availability groups. |
    | `multisubnetfailover` | `bool or str` | - | ODBC keyword alias for `multi_subnet_failover`. |
    | `application_intent` | `str` | - | Availability group workload routing intent (`ReadWrite` or `ReadOnly`). |
    | `applicationintent` | `str` | - | ODBC keyword alias for `application_intent`. |
    | `connect_retry_count` | `int` | - | Number of automatic connection retry attempts on broken idle connections. |
    | `connectretrycount` | `int` | - | ODBC keyword alias for `connect_retry_count`. |
    | `connect_retry_interval` | `int` | - | Delay in seconds between automatic connection recovery attempts. |
    | `connectretryinterval` | `int` | - | ODBC keyword alias for `connect_retry_interval`. |
    | `keep_alive` | `int` | - | TCP keepalive idle interval in milliseconds. |
    | `keepalive` | `int` | - | ODBC keyword alias for `keep_alive`. |
    | `keep_alive_interval` | `int` | - | Interval in milliseconds between TCP keepalive probes. |
    | `keepaliveinterval` | `int` | - | ODBC keyword alias for `keep_alive_interval`. |
    | `ip_address_preference` | `str` | - | Address family preference during DNS resolution (`IPv4First`, `IPv6First`, or `UsePlatformDefault`). |
    | `ipaddresspreference` | `str` | - | ODBC keyword alias for `ip_address_preference`. |
    | `packet_size` | `int` | - | TDS network packet size in bytes. |
    | `packetsize` | `int` | - | ODBC keyword alias for `packet_size`. |
    | `connection_timeout` | `int` | - | Connection establishment timeout in seconds. |
    | `login_timeout` | `int` | - | Login handshake timeout in seconds. |
    | `command_timeout` | `int` | - | Default SQL statement execution timeout in seconds. |
    | `autocommit` | `bool` | - | Enable autocommit mode on new `mssql-python` connections. |
    | `attrs_before` | `dict[int, int or str or bytes]` | - | Pre-connect SQL_ATTR_* ODBC connection attributes dictionary (such as access tokens). |
    | `timeout` | `int` | - | Query execution timeout in seconds assigned on the connection. |
    | `native_uuid` | `bool` | - | Return `UNIQUEIDENTIFIER` columns as `uuid.UUID` objects instead of strings. |
    | `token_provider` | `TokenProvider` | - | Callable or Entra ID credential provider supplying OAuth access tokens. |
    | `extra` | `dict[str, Any]` | - | Additional connection attributes forwarded to `mssql_python.connect`. |
    | `pool_size` | `int` | - | Maximum number of connections retained in `MssqlPythonConnectionPool`. |
    | `pool_idle_timeout` | `int` | - | Idle duration in seconds before pooled SQL Server connections are closed. |
    | `pool_enabled` | `bool` | - | Enable `mssql-python` driver-level connection pooling. |
    <!-- GENERATED:END config-table:sqlspec.adapters.mssql_python.config.MssqlPythonConnectionParams,sqlspec.adapters.mssql_python.config.MssqlPythonPoolParams -->

    **Driver Features (`MssqlPythonDriverFeatures`)**

    <!-- GENERATED:START config-table:sqlspec.adapters.mssql_python.config.MssqlPythonDriverFeatures -->
    | Key | Type | Default | Description |
    | --- | --- | --- | --- |
    | `tenant_strategy` | `SyncTenantStrategy or TenantStrategyShorthand or None` | - | Opt-in tenant strategy, or the `"session_context"` shorthand, applied to every session and connection lease. |
    | `enable_pooling` | `bool` | `True` | Enable SQLSpec connection pooling around `mssql-python` connections (defaults to `True`). |
    | `json_serializer` | `Callable[[Any], str]` | `to_json` | Custom JSON serializer applied to bound JSON parameters (defaults to `to_json`). |
    | `json_deserializer` | `Callable[[str], Any]` | `from_json` | Custom JSON deserializer applied to JSON result columns (defaults to `from_json`). |
    | `on_connection_create` | `Callable[[MssqlPythonConnection], None]` | - | Synchronous hook invoked with each raw `mssql-python` connection upon creation. |
    | `enable_events` | `bool` | - | Enable durable table-backed SQLSpec event queue support. |
    | `events_backend` | `Literal['poll_queue']` | - | Event channel backend (`poll_queue` is the sole SQL Server backend). |
    <!-- GENERATED:END config-table:sqlspec.adapters.mssql_python.config.MssqlPythonDriverFeatures -->

=== "pymssql (Sync)"

    > [!NOTE]
    > `PymssqlConnectionPool` is a thread-local connection manager that recycles connections via `pool_recycle_seconds` and `health_check_interval` on `PymssqlConnectionParams`. Do not pass `pool_min_size` or `pool_max_size` to `PymssqlConfig`, as unknown keys are forwarded to `pymssql.connect()`.

    **Connection and Pool Parameters (`PymssqlConnectionParams`)**

    <!-- GENERATED:START config-table:sqlspec.adapters.pymssql.config.PymssqlConnectionParams -->
    | Key | Type | Default | Description |
    | --- | --- | --- | --- |
    | `server` | `str` | - | SQL Server hostname, `host:port`, or `host\instance` target. |
    | `host` | `str` | - | Alias for `server` accepted by `pymssql.connect`. |
    | `user` | `str` | - | SQL Server or Windows domain login (`DOMAIN\user`). |
    | `password` | `str` | - | Password for `user`. |
    | `database` | `str` | - | Initial SQL Server database selected on connect. |
    | `port` | `int or str` | `1433` | SQL Server TCP port (defaults to `1433`). |
    | `timeout` | `PymssqlTimeout` | - | Query response timeout in seconds (`0` means wait indefinitely). |
    | `login_timeout` | `PymssqlTimeout` | `60` | Connection and authentication timeout in seconds (defaults to `60`). |
    | `charset` | `str` | `'UTF-8'` | Character encoding communicated over FreeTDS (defaults to `'UTF-8'`). |
    | `as_dict` | `bool` | - | Return rows from raw `pymssql` cursors as dictionaries instead of tuples. |
    | `appname` | `str` | - | Application name visible in `sys.dm_exec_sessions`. |
    | `conn_properties` | `str` | - | Semicolon-separated `SET` session statements run immediately upon connect. |
    | `autocommit` | `bool` | - | Enable autocommit mode on new `pymssql` connections. |
    | `tds_version` | `str` | - | Explicit FreeTDS protocol version (such as `'7.4'`). |
    | `encryption` | `Literal['off', 'request', 'require']` | - | FreeTDS TLS transport encryption requirement (`off`, `request`, or `require`). |
    | `use_datetime2` | `bool` | - | Bind Python `datetime` values using `DATETIME2` precision instead of legacy `DATETIME`. |
    | `arraysize` | `int` | - | Default row batch count returned by `cursor.fetchmany()`. |
    | `conv` | `Mapping[int or type[Any], Callable[..., Any]]` | - | Custom FreeTDS type serialization/deserialization mapping. |
    | `read_only` | `bool` | - | Set Always On `ApplicationIntent=ReadOnly` during login. |
    | `pool_recycle_seconds` | `int` | - | Maximum connection age in seconds before `PymssqlConnectionPool` recycles connections. |
    | `health_check_interval` | `float` | - | Minimum interval in seconds between connection health checks in `PymssqlConnectionPool`. |
    | `extra` | `dict[str, Any]` | - | Additional keyword arguments forwarded to `pymssql.connect`. |
    <!-- GENERATED:END config-table:sqlspec.adapters.pymssql.config.PymssqlConnectionParams -->

    **Driver Features (`PymssqlDriverFeatures`)**

    <!-- GENERATED:START config-table:sqlspec.adapters.pymssql.config.PymssqlDriverFeatures -->
    | Key | Type | Default | Description |
    | --- | --- | --- | --- |
    | `tenant_strategy` | `SyncTenantStrategy or TenantStrategyShorthand or None` | - | Opt-in tenant strategy, or the `"session_context"` shorthand, applied to every session and connection lease. |
    | `json_serializer` | `Callable[[Any], str]` | `to_json` | Custom JSON serializer applied to bound JSON parameters (defaults to `to_json`). |
    | `json_deserializer` | `Callable[[str], Any]` | `from_json` | Custom JSON deserializer applied to JSON result columns (defaults to `from_json`). |
    | `on_connection_create` | `Callable[[PymssqlConnection], None]` | - | Synchronous hook invoked with each raw `pymssql.Connection` upon creation. |
    | `enable_events` | `bool` | - | Enable durable table-backed SQLSpec event queue support. |
    | `events_backend` | `Literal['poll_queue']` | - | Event channel backend (`poll_queue` is the sole SQL Server backend). |
    | `enable_pooling` | `bool` | `True` | Enable driver-level connection pooling (defaults to `True`). |
    | `enable_uuid_conversion` | `bool` | - | Enable automatic UUID string-to-object conversions. |
    <!-- GENERATED:END config-table:sqlspec.adapters.pymssql.config.PymssqlDriverFeatures -->

## Type Mappings & Dialect Notes

| Database Type | Python Type | Notes / Feature Flag |
| --- | --- | --- |
| `NVARCHAR(MAX)` (JSON) | `dict` / `list` | Serialized and deserialized via `json_serializer` and `json_deserializer`; works with SQL Server `JSON_VALUE` and `OPENJSON`. |
| `UNIQUEIDENTIFIER` | `uuid.UUID` / `str` | Native T-SQL GUID column type (`NEWID()` / `NEWSEQUENTIALID()`). |
| `DATETIME2` / `DATETIMEOFFSET` | `datetime.datetime` | High-precision 100ns timestamp and timezone-aware offset storage. |
| `DECIMAL` / `MONEY` | `decimal.Decimal` | Exact arbitrary-precision decimal and currency values. |
| `VARBINARY(MAX)` | `bytes` / `bytearray` | Binary payload storage. |
| `BIT` | `bool` / `int` | T-SQL boolean flag (`0` / `1`). |

- **`.returning()` Renders `OUTPUT`**: Builders qualify unqualified columns, columns named with the target table, and `*`. Insert and update read from `INSERTED`, so an update returns post-update values. Delete reads from `DELETED`. Write `DELETED.col` on an update to return the previous value, and use a `FROM` alias such as `s.col` to return a source column. Subqueries raise `SQLBuilderError`. Raw `SQL` statements run as written, so include the `INSERTED` or `DELETED` prefix yourself.
- **`OUTPUT` and Triggers**: SQL Server rejects `OUTPUT` without `INTO` when the target table has an enabled trigger (Msg 334), and builders do not render `INTO`. For those tables, run raw `SQL` with `OUTPUT ... INTO @table_variable` followed by a `SELECT`.
- **MERGE Upserts**: Build atomic SQL Server upserts with `sql.merge(dialect="tsql").into(...)` (`WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...`).
- **Streaming Arrow BulkCopy on mssql-python**: `session.load_from_arrow(table, source)` streams `pyarrow.Table`, `RecordBatch`, or `RecordBatchReader` batches directly through `mssql_python`'s native TDS `bulkcopy` engine without materializing intermediate Python dicts.
- **T-SQL Pagination Requires ORDER BY**: SQL Server's `OFFSET ... ROWS FETCH NEXT ... ROWS ONLY` syntax requires an explicit `ORDER BY` clause. Always attach `.order_by(...)` or `OrderByFilter` when using `LimitOffsetFilter` on `tsql`.

## See Also

- [MSSQL Python API Reference](/reference/adapters/mssql-python/)
- [PyMSSQL API Reference](/reference/adapters/pymssql/)
- [SQL Server Dialect Reference](/reference/data-dictionary/dialects/mssql/)
- [Arrow ODBC Guide](arrow-odbc.md)
