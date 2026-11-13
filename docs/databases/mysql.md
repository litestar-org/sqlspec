# MySQL and MariaDB

SQLSpec provides four adapters for MySQL 8.0+ and MariaDB 10.5+ sharing unified error translation (`ER_DUP_ENTRY`, `ER_LOCK_DEADLOCK`, `ER_NO_REFERENCED_ROW_2`), catalog introspection, and bulk `LOAD DATA LOCAL INFILE` Arrow ingestion: `asyncmy`, `aiomysql`, `pymysql`, and `mysqlconnector`.

## Overview & Capabilities

| Adapter Config | Driver Extra | Execution | Pool Architecture | Best For |
| --- | --- | --- | --- | --- |
| `AsyncmyConfig` | `sqlspec[asyncmy]` | Async (`asyncio`) | `asyncmy.Pool` (`minsize`, `maxsize`, `pool_recycle`) | High-throughput compiled `asyncio` services targeting MySQL 8.0+ or MariaDB 10.5+. |
| `AiomysqlConfig` | `sqlspec[aiomysql]` | Async (`asyncio`) | `aiomysql.Pool` (`minsize`, `maxsize`, `pool_recycle`) | Pure-Python `asyncio` services built over PyMySQL for PyPy or pure-Python runtimes. |
| `PyMysqlConfig` | `sqlspec[pymysql]` | Sync | `PyMysqlConnectionPool` (thread-local manager with `pool_recycle_seconds`, `health_check_interval`) | Synchronous applications and Google Cloud SQL for MySQL (`enable_cloud_sql=True`). |
| `MysqlConnectorSyncConfig` / `MysqlConnectorAsyncConfig` | `sqlspec[mysql-connector]` | Sync & Async | `MySQLConnectionPool` (`pool_name`, `pool_size`, `pool_reset_session`) / native async sessions | Official Oracle MySQL Connector/Python supporting both sync pools and native async connections. |

## Installation & Quickstart

=== "uv"

    ```bash
    uv add "sqlspec[asyncmy]"
    # or: uv add "sqlspec[aiomysql]"
    # or: uv add "sqlspec[pymysql]"
    # or: uv add "sqlspec[mysql-connector]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[asyncmy]"
    # or: pip install "sqlspec[aiomysql]"
    # or: pip install "sqlspec[pymysql]"
    # or: pip install "sqlspec[mysql-connector]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[asyncmy]"
    # or: pdm add "sqlspec[aiomysql]"
    # or: pdm add "sqlspec[pymysql]"
    # or: pdm add "sqlspec[mysql-connector]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[asyncmy]"
    # or: poetry add "sqlspec[aiomysql]"
    # or: poetry add "sqlspec[pymysql]"
    # or: poetry add "sqlspec[mysql-connector]"
    ```

=== "Asyncmy (Async)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncmy import AsyncmyConfig

    spec = SQLSpec()
    config = spec.add_config(
        AsyncmyConfig(
            connection_config={
                "host": "localhost",
                "port": 3306,
                "user": "app",
                "password": "secret",
                "db": "app_db",
                "minsize": 2,
                "maxsize": 20,
            }
        )
    )
    ```

=== "Aiomysql (Async)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.aiomysql import AiomysqlConfig

    spec = SQLSpec()
    config = spec.add_config(
        AiomysqlConfig(
            connection_config={
                "host": "localhost",
                "port": 3306,
                "user": "app",
                "password": "secret",
                "db": "app_db",
                "minsize": 2,
                "maxsize": 20,
            }
        )
    )
    ```

=== "PyMySQL (Sync)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.pymysql import PyMysqlConfig

    spec = SQLSpec()
    config = spec.add_config(
        PyMysqlConfig(
            connection_config={
                "host": "localhost",
                "port": 3306,
                "user": "app",
                "password": "secret",
                "database": "app_db",
                "charset": "utf8mb4",
                "pool_recycle_seconds": 3600,
                "health_check_interval": 30,
            }
        )
    )
    ```

=== "MySQL Connector (Sync & Async)"

    ```{ .python title="docs/examples/drivers/mysqlconnector_connection.py" }
    --8<-- "docs/examples/drivers/mysqlconnector_connection.py:example"
    ```

## Connection & Pool Configuration

=== "Asyncmy"

    **Connection and Pool Parameters (`AsyncmyConnectionParams`, `AsyncmyPoolParams`)**

    <!-- GENERATED:START config-table:sqlspec.adapters.asyncmy.config.AsyncmyConnectionParams,sqlspec.adapters.asyncmy.config.AsyncmyPoolParams -->
    | Key | Type | Default | Description |
    | --- | --- | --- | --- |
    | `dsn` | `str` | - | MySQL/MariaDB connection URI (`mysql://user:pass@host:port/db`). |
    | `url` | `str` | - | Connection URL alias parsed identically to `dsn`. |
    | `connection_string` | `str` | - | Connection string alias parsed identically to `dsn`. |
    | `host` | `str` | `'localhost'` | MySQL or MariaDB server hostname (defaults to `'localhost'`). |
    | `user` | `str` | - | Database account username. |
    | `username` | `str` | - | Alias normalized to `user` when `user` is omitted. |
    | `password` | `str` | - | Password for the database user. |
    | `database` | `str` | - | Default schema/database selected upon connecting. |
    | `db` | `str` | - | Alias for `database` accepted by `asyncmy`. |
    | `port` | `int` | `3306` | MySQL/MariaDB TCP port (defaults to `3306`). |
    | `unix_socket` | `str` | - | Filesystem path to a local MySQL Unix domain socket. |
    | `charset` | `str` | `'utf8mb4'` | Connection character set (defaults to `'utf8mb4'`). |
    | `connect_timeout` | `int or float` | - | TCP handshake and authentication timeout in seconds. |
    | `read_default_file` | `str` | - | Path to a MySQL `my.cnf` option file. |
    | `read_default_group` | `str` | - | Option group name read from `read_default_file`. |
    | `autocommit` | `bool` | - | Enable MySQL autocommit mode on new connections. |
    | `allow_local_infile` | `bool` | - | Permit client-side `LOAD DATA LOCAL INFILE` transfers. |
    | `local_infile` | `bool` | - | Alias enabling `LOAD DATA LOCAL INFILE` support. |
    | `ssl` | `AsyncmySSLParams or ssl.SSLContext or dict[str, Any]` | - | TLS configuration mapping or `ssl.SSLContext` instance. |
    | `sql_mode` | `str` | - | Session `sql_mode` string applied when the connection opens. |
    | `init_command` | `str` | - | SQL statement executed immediately after authentication. |
    | `auth_plugin_map` | `dict[str or bytes, type[Any]]` | - | Mapping from MySQL auth plugin names to custom handler classes. |
    | `binary_prefix` | `bool` | - | Emit `_binary` introducer prefixes when escaping `bytes` literals. |
    | `client_flag` | `int` | - | Bitmask of MySQL `CLIENT_*` protocol capability flags. |
    | `conv` | `dict[Any, Any]` | - | Custom MySQL field-type conversion table. |
    | `cursor_class` | `type[AsyncmyRawCursor] or type[AsyncmyDictCursor]` | - | Cursor class alias normalized for `asyncmy` connections. |
    | `cursor_cls` | `type[AsyncmyRawCursor] or type[AsyncmyDictCursor]` | - | Default `asyncmy` cursor class used by raw connections. |
    | `max_allowed_packet` | `int` | `16 MiB` | Maximum client packet size in bytes (defaults to `16 MiB` in `asyncmy`). |
    | `program_name` | `str` | - | Client program name reported in `performance_schema.session_connect_attrs`. |
    | `read_timeout` | `int or float` | - | Socket read timeout in seconds for query responses. |
    | `server_public_key` | `str or bytes` | - | RSA public key PEM used for `caching_sha2_password` / `sha256_password` exchange. |
    | `stmt_cache_size` | `int` | - | Maximum prepared statements cached per `asyncmy` connection. |
    | `use_unicode` | `bool` | `True` | Decode string columns into Python `str` values (defaults to `True`). |
    | `write_timeout` | `int or float` | - | Socket write timeout in seconds for outgoing packets. |
    | `extra` | `dict[str, Any]` | - | Additional keyword arguments forwarded to `asyncmy.connect` / `create_pool`. |
    | `minsize` | `int` | - | Minimum number of open connections maintained in `asyncmy.Pool`. |
    | `maxsize` | `int` | - | Maximum number of concurrent connections allowed in `asyncmy.Pool`. |
    | `echo` | `bool` | - | Log executed SQL statements on `asyncmy` connections and pools. |
    | `pool_recycle` | `int` | - | Maximum connection age in seconds before `asyncmy` recycles pooled connections. |
    <!-- GENERATED:END config-table:sqlspec.adapters.asyncmy.config.AsyncmyConnectionParams,sqlspec.adapters.asyncmy.config.AsyncmyPoolParams -->

    **Driver Features (`AsyncmyDriverFeatures`)**

    <!-- GENERATED:START config-table:sqlspec.adapters.asyncmy.config.AsyncmyDriverFeatures -->
    | Key | Type | Default | Description |
    | --- | --- | --- | --- |
    | `enable_local_infile_bulk_load` | `bool` | - | Use `LOAD DATA LOCAL INFILE` for `load_from_arrow` when local infile is enabled on the connection. |
    | `json_serializer` | `Callable[[Any], str]` | `to_json` | Custom JSON serializer applied to bound JSON parameters (defaults to `to_json`). |
    | `json_deserializer` | `Callable[[str], Any]` | `from_json` | Custom JSON deserializer applied to JSON result columns (defaults to `from_json`). |
    | `on_connection_create` | `Callable[[AsyncmyConnection], Awaitable[None]]` | - | Async callback invoked once per physical `asyncmy` connection acquired from the pool. |
    | `enable_events` | `bool` | - | Enable durable table-backed SQLSpec event queue support. |
    | `events_backend` | `Literal['poll_queue']` | - | Event channel backend (`poll_queue` is the sole MySQL/MariaDB backend). |
    <!-- GENERATED:END config-table:sqlspec.adapters.asyncmy.config.AsyncmyDriverFeatures -->

=== "Aiomysql"

    **Connection and Pool Parameters (`AiomysqlConnectionParams`, `AiomysqlPoolParams`)**

    <!-- GENERATED:START config-table:sqlspec.adapters.aiomysql.config.AiomysqlConnectionParams,sqlspec.adapters.aiomysql.config.AiomysqlPoolParams -->
    | Key | Type | Default | Description |
    | --- | --- | --- | --- |
    | `dsn` | `str` | - | MySQL/MariaDB connection URI (`mysql://user:pass@host:port/db`). |
    | `url` | `str` | - | Connection URL alias parsed identically to `dsn`. |
    | `connection_string` | `str` | - | Connection string alias parsed identically to `dsn`. |
    | `host` | `str` | `'localhost'` | MySQL or MariaDB server hostname (defaults to `'localhost'`). |
    | `user` | `str` | - | Database account username. |
    | `username` | `str` | - | Alias normalized to `user` when `user` is omitted. |
    | `password` | `str` | - | Password for the database user. |
    | `passwd` | `str` | - | Legacy PyMySQL/aiomysql alias for `password`. |
    | `db` | `str` | - | Default schema/database selected upon connecting. |
    | `database` | `str` | - | Alias normalized to `db` when `db` is omitted. |
    | `port` | `int` | `3306` | MySQL/MariaDB TCP port (defaults to `3306`). |
    | `unix_socket` | `str` | - | Filesystem path to a local MySQL Unix domain socket. |
    | `charset` | `str` | `'utf8mb4'` | Connection character set (defaults to `'utf8mb4'`). |
    | `connect_timeout` | `int or float or None` | - | TCP connection establishment timeout in seconds. |
    | `read_default_file` | `str` | - | Path to a MySQL `my.cnf` option file. |
    | `read_default_group` | `str` | - | Option group name read from `read_default_file`. |
    | `autocommit` | `bool or None` | - | Enable MySQL autocommit mode on new connections. |
    | `allow_local_infile` | `bool` | - | Alias enabling client-side `LOAD DATA LOCAL INFILE` transfers. |
    | `echo` | `bool` | - | Log executed SQL statements through `aiomysql`. |
    | `local_infile` | `bool` | - | Permit client-side `LOAD DATA LOCAL INFILE` transfers. |
    | `ssl` | `SSLContext` | - | Configured `ssl.SSLContext` used for TLS connections. |
    | `sql_mode` | `str` | - | Session `sql_mode` string applied at connection startup. |
    | `init_command` | `str` | - | SQL statement executed immediately after authentication. |
    | `conv` | `dict[int, Callable[[bytes], Any]] or dict[int, type[Any]] or Mapping[int, Any]` | - | Custom PyMySQL/aiomysql type decoder mapping. |
    | `use_unicode` | `bool or None` | `True` | Decode text columns into Python `str` values (defaults to `True`). |
    | `client_flag` | `int` | - | Bitmask of MySQL `CLIENT_*` capability flags. |
    | `cursorclass` | `type[AiomysqlRawCursor] or type[AiomysqlDictCursor]` | - | Default `aiomysql` cursor class instantiated by `conn.cursor()`. |
    | `cursor_class` | `type[AiomysqlRawCursor] or type[AiomysqlDictCursor]` | - | Alias normalized to `cursorclass`. |
    | `auth_plugin` | `str` | - | Explicit client authentication plugin name (such as `caching_sha2_password`). |
    | `program_name` | `str` | - | Application name reported in MySQL connection attributes. |
    | `server_public_key` | `str or bytes` | - | Server RSA public key PEM for SHA-256 password exchange. |
    | `loop` | `Any` | - | Explicit `asyncio` event loop passed to `aiomysql`. |
    | `minsize` | `int` | - | Minimum number of open connections maintained in `aiomysql.Pool`. |
    | `maxsize` | `int` | - | Maximum number of concurrent connections allowed in `aiomysql.Pool`. |
    | `pool_recycle` | `int` | - | Maximum connection age in seconds before recycling pooled connections. |
    <!-- GENERATED:END config-table:sqlspec.adapters.aiomysql.config.AiomysqlConnectionParams,sqlspec.adapters.aiomysql.config.AiomysqlPoolParams -->

    **Driver Features (`AiomysqlDriverFeatures`)**

    <!-- GENERATED:START config-table:sqlspec.adapters.aiomysql.config.AiomysqlDriverFeatures -->
    | Key | Type | Default | Description |
    | --- | --- | --- | --- |
    | `json_serializer` | `Callable[[Any], str]` | `to_json` | Custom JSON serializer applied to bound JSON parameters (defaults to `to_json`). |
    | `json_deserializer` | `Callable[[str], Any]` | `from_json` | Custom JSON deserializer applied to JSON result columns (defaults to `from_json`). |
    | `on_connection_create` | `Callable[[AiomysqlConnection], Awaitable[None]]` | - | Async callback invoked once per physical `aiomysql` connection acquired from the pool. |
    | `enable_events` | `bool` | - | Enable durable table-backed SQLSpec event queue support. |
    | `events_backend` | `Literal['poll_queue']` | - | Event channel backend (`poll_queue` is the sole MySQL/MariaDB backend). |
    | `enable_local_infile_bulk_load` | `bool` | - | Use `LOAD DATA LOCAL INFILE` for `load_from_arrow` when local infile is enabled. |
    <!-- GENERATED:END config-table:sqlspec.adapters.aiomysql.config.AiomysqlDriverFeatures -->

=== "PyMySQL"

    > [!NOTE]
    > `PyMysqlConnectionPool` is a thread-local connection manager that recycles connections via `pool_recycle_seconds` and `health_check_interval`. Do not pass `minsize` or `maxsize` to `PyMysqlConfig`, as unknown keys are forwarded to `pymysql.connect()`.

    **Connection and Pool Parameters (`PyMysqlConnectionParams`, `PyMysqlPoolParams`)**

    <!-- GENERATED:START config-table:sqlspec.adapters.pymysql.config.PyMysqlConnectionParams,sqlspec.adapters.pymysql.config.PyMysqlPoolParams -->
    | Key | Type | Default | Description |
    | --- | --- | --- | --- |
    | `dsn` | `str` | - | MySQL/MariaDB connection URI (`mysql://user:pass@host:port/db`). |
    | `url` | `str` | - | Connection URL alias parsed identically to `dsn`. |
    | `connection_string` | `str` | - | Connection string alias parsed identically to `dsn`. |
    | `host` | `str` | `'localhost'` | MySQL or MariaDB server hostname (defaults to `'localhost'`). |
    | `user` | `str` | - | Database account username. |
    | `username` | `str` | - | Alias normalized to `user` when `user` is omitted. |
    | `password` | `str` | - | Password for the database user. |
    | `database` | `str` | - | Default schema/database selected upon connecting. |
    | `db` | `str` | - | Alias for `database` accepted by `pymysql.connect`. |
    | `port` | `int` | `3306` | MySQL/MariaDB TCP port (defaults to `3306`). |
    | `unix_socket` | `str` | - | Filesystem path to a local MySQL Unix domain socket. |
    | `charset` | `str` | `'utf8mb4'` | Connection character set (defaults to `'utf8mb4'`). |
    | `collation` | `str` | - | Session collation paired with `charset`. |
    | `sql_mode` | `str` | - | Session `sql_mode` string applied when the connection opens. |
    | `read_default_file` | `str` | - | Path to a MySQL `my.cnf` option file. |
    | `read_default_group` | `str` | - | Option group name read from `read_default_file`. |
    | `conv` | `PyMysqlConverter` | - | Custom PyMySQL type encoder/decoder mapping. |
    | `use_unicode` | `bool` | `True` | Decode text columns into Python `str` values (defaults to `True`). |
    | `client_flag` | `int` | - | Bitmask of MySQL `CLIENT_*` protocol capability flags. |
    | `cursorclass` | `type[PyMysqlRawCursor]` | - | Default PyMySQL cursor class instantiated by `conn.cursor()`. |
    | `init_command` | `str` | - | SQL statement executed immediately after authentication. |
    | `connect_timeout` | `PyMysqlTimeout` | - | TCP handshake and authentication timeout in seconds. |
    | `read_timeout` | `PyMysqlTimeout` | - | Socket read timeout in seconds for server responses. |
    | `write_timeout` | `PyMysqlTimeout` | - | Socket write timeout in seconds for outgoing packets. |
    | `autocommit` | `bool` | - | Enable MySQL autocommit mode on new connections. |
    | `allow_local_infile` | `bool` | - | Alias enabling `LOAD DATA LOCAL INFILE` transfers. |
    | `local_infile` | `bool` | - | Permit client-side `LOAD DATA LOCAL INFILE` transfers. |
    | `max_allowed_packet` | `int` | - | Maximum client packet size in bytes. |
    | `defer_connect` | `bool` | - | Construct the `pymysql.Connection` instance without opening the socket immediately. |
    | `auth_plugin_map` | `Mapping[str, type[Any]]` | - | Mapping from MySQL auth plugin names to custom handler classes. |
    | `bind_address` | `str` | - | Local source IP address or hostname bound by the client socket. |
    | `binary_prefix` | `bool` | - | Emit `_binary` introducer prefixes when escaping `bytes` literals. |
    | `program_name` | `str` | - | Application name sent in MySQL connection attributes. |
    | `server_public_key` | `str or bytes` | - | Server RSA public key PEM used for SHA-256 password exchange. |
    | `ssl` | `PyMysqlSslConfig` | - | TLS `ssl.SSLContext` or `PyMysqlSslParams` mapping. |
    | `ssl_ca` | `str` | - | Path to the PEM CA bundle file for TLS verification. |
    | `ssl_cert` | `str` | - | Path to the client TLS certificate file. |
    | `ssl_disabled` | `bool` | - | Disable TLS negotiation completely when `True`. |
    | `ssl_key` | `str` | - | Path to the client TLS private key file. |
    | `ssl_key_password` | `str` | - | Passphrase unlocking an encrypted `ssl_key`. |
    | `ssl_verify_cert` | `bool` | - | Verify the server TLS certificate against `ssl_ca`. |
    | `ssl_verify_identity` | `bool` | - | Verify that the server TLS certificate matches the target hostname. |
    | `extra` | `dict[str, Any]` | - | Additional keyword arguments forwarded to `pymysql.connect`. |
    | `pool_recycle_seconds` | `int` | - | Maximum connection age in seconds before recycling pooled PyMySQL connections. |
    | `health_check_interval` | `float` | - | Minimum interval in seconds between `conn.ping()` liveness checks. |
    <!-- GENERATED:END config-table:sqlspec.adapters.pymysql.config.PyMysqlConnectionParams,sqlspec.adapters.pymysql.config.PyMysqlPoolParams -->

    **Driver Features (`PyMysqlDriverFeatures`)**

    <!-- GENERATED:START config-table:sqlspec.adapters.pymysql.config.PyMysqlDriverFeatures -->
    | Key | Type | Default | Description |
    | --- | --- | --- | --- |
    | `json_serializer` | `Callable[[Any], str]` | `to_json` | Custom JSON serializer applied to bound JSON parameters (defaults to `to_json`). |
    | `json_deserializer` | `Callable[[str], Any]` | `from_json` | Custom JSON deserializer applied to JSON result columns (defaults to `from_json`). |
    | `on_connection_create` | `Callable[[PyMysqlConnection], None]` | - | Synchronous hook invoked with each raw `pymysql.Connection` upon creation. |
    | `enable_events` | `bool` | - | Enable durable table-backed SQLSpec event queue support. |
    | `events_backend` | `Literal['poll_queue']` | - | Event channel backend (`poll_queue` is the sole MySQL/MariaDB backend). |
    | `enable_local_infile_bulk_load` | `bool` | - | Route `load_from_arrow` through `LOAD DATA LOCAL INFILE` when local infile is enabled. |
    | `enable_cloud_sql` | `bool` | `False` | Route connections through `cloud-sql-python-connector` (defaults to `False`). |
    | `cloud_sql_instance` | `str` | - | Cloud SQL instance connection string in `project:region:instance` format. |
    | `cloud_sql_enable_iam_auth` | `bool` | - | Use IAM database authentication with the Cloud SQL Python Connector. |
    | `cloud_sql_ip_type` | `str` | `PRIVATE` | Cloud SQL network path (`PUBLIC`, `PRIVATE`, or `PSC`; defaults to `PRIVATE`). |
    <!-- GENERATED:END config-table:sqlspec.adapters.pymysql.config.PyMysqlDriverFeatures -->

=== "MySQL Connector"

    **Connection and Pool Parameters (`MysqlConnectorSyncConnectionParams`, `MysqlConnectorPoolParams`)**

    <!-- GENERATED:START config-table:sqlspec.adapters.mysqlconnector.config.MysqlConnectorSyncConnectionParams,sqlspec.adapters.mysqlconnector.config.MysqlConnectorPoolParams -->
    | Key | Type | Default | Description |
    | --- | --- | --- | --- |
    | `host` | `str` | `'localhost'` | MySQL server hostname or IP address (defaults to `'localhost'`). |
    | `user` | `str` | - | Database account username. |
    | `username` | `str` | - | Alias normalized to `user` when `user` is omitted. |
    | `password` | `str` | - | Primary password for `user`. |
    | `passwd` | `str` | - | Legacy Connector/Python alias for `password`. |
    | `password1` | `str` | - | First authentication factor password for multi-factor MySQL accounts. |
    | `password2` | `str` | - | Second authentication factor password or token for MFA accounts. |
    | `password3` | `str` | - | Third authentication factor password or token for MFA accounts. |
    | `database` | `str` | - | Default schema/database selected upon connecting. |
    | `db` | `str` | - | Alias normalized to `database` when `database` is omitted. |
    | `port` | `int` | `3306` | MySQL server TCP port (defaults to `3306`). |
    | `unix_socket` | `str` | - | Filesystem path to a local MySQL Unix domain socket. |
    | `conn_attrs` | `dict[str, str]` | - | Custom key/value attributes exposed in `performance_schema.session_connect_attrs`. |
    | `init_command` | `str` | - | SQL statement executed immediately after establishing a connection. |
    | `auth_plugin` | `str` | - | Default client authentication plugin name (such as `caching_sha2_password`). |
    | `webauthn_callback` | `Callable[[], None] or str` | - | Callback invoked during FIDO/WebAuthn challenge-response authentication. |
    | `openid_token_file` | `str` | - | Path to a file containing an OpenID Connect identity token. |
    | `use_unicode` | `bool` | `True` | Decode character columns into Python `str` values (defaults to `True`). |
    | `charset` | `str` | `'utf8mb4'` | Connection character set (defaults to `'utf8mb4'`). |
    | `collation` | `str` | - | Connection collation paired with `charset`. |
    | `autocommit` | `bool` | - | Enable MySQL autocommit mode outside explicit transactions. |
    | `time_zone` | `str` | - | Session `time_zone` offset or named zone set on connect. |
    | `sql_mode` | `str` | - | Session `sql_mode` flags applied at connection startup. |
    | `get_warnings` | `bool` | - | Automatically fetch MySQL warnings after each statement. |
    | `raise_on_warnings` | `bool` | - | Raise a Python exception whenever the server reports a SQL warning. |
    | `connection_timeout` | `int or float` | - | Connection establishment timeout in seconds. |
    | `connect_timeout` | `int or float` | - | Alias for `connection_timeout` in seconds. |
    | `read_timeout` | `int or float` | - | Socket read timeout in seconds for server responses. |
    | `write_timeout` | `int or float` | - | Socket write timeout in seconds for outgoing packets. |
    | `client_flags` | `MysqlConnectorClientFlags` | - | MySQL `ClientFlag` bitmask or sequence of capability flags. |
    | `buffered` | `bool` | - | Default `buffered` cursor setting across statements on the connection. |
    | `raw` | `bool` | - | Default `raw` cursor setting returning unconverted wire bytes. |
    | `consume_results` | `bool` | - | Automatically drain unread cursor result sets before closing or reusing cursors. |
    | `tls_versions` | `MysqlConnectorStringSequence` | - | Permitted TLS protocol versions (such as `['TLSv1.2', 'TLSv1.3']`). |
    | `tls_ciphersuites` | `MysqlConnectorStringSequence` | - | Allowed TLS cipher suite names. |
    | `ssl_ca` | `str` | - | Path to the PEM CA bundle file used for server certificate verification. |
    | `ssl_cert` | `str` | - | Path to the client TLS certificate file. |
    | `ssl_key` | `str` | - | Path to the client TLS private key file. |
    | `ssl_cipher` | `str` | - | Deprecated TLS cipher specification string. |
    | `ssl_disabled` | `bool` | - | Disable TLS encryption completely when `True`. |
    | `ssl_verify_cert` | `bool` | - | Validate the server TLS certificate chain against `ssl_ca`. |
    | `ssl_verify_identity` | `bool` | - | Verify that the server TLS certificate subject/SAN matches `host`. |
    | `force_ipv6` | `bool` | - | Prefer IPv6 addresses when a hostname resolves to both IPv4 and IPv6. |
    | `dns_srv` | `bool` | - | Resolve `host` as a DNS SRV record for multi-endpoint discovery. |
    | `kerberos_auth_mode` | `Literal['SSPI', 'GSSAPI']` | - | Kerberos client library mode on Windows (`SSPI` or `GSSAPI`). |
    | `krb_service_principal` | `str` | - | Kerberos service principal name for `authentication_kerberos_client`. |
    | `oci_config_file` | `str` | - | Path to an Oracle Cloud Infrastructure SDK configuration file for IAM auth. |
    | `oci_config_profile` | `str` | - | Profile section within `oci_config_file`. |
    | `compress` | `bool` | - | Enable zlib packet compression on the MySQL wire protocol. |
    | `converter_class` | `type[Any]` | - | Custom `MySQLConverterBase` subclass converting Python and MySQL values. |
    | `converter_str_fallback` | `bool` | - | Serialize unsupported Python types via `str(value)` inside the converter. |
    | `failover` | `list[MysqlConnectorFailoverTarget] or tuple[MysqlConnectorFailoverTarget, ...]` | - | Ordered sequence of `MysqlConnectorFailoverTarget` server definitions. |
    | `option_files` | `MysqlConnectorPathSequence` | - | MySQL `my.cnf` / `my.ini` option file path or paths to load. |
    | `option_groups` | `MysqlConnectorStringSequence` | - | Option group names read from `option_files`. |
    | `allow_local_infile` | `bool` | - | Permit `LOAD DATA LOCAL INFILE` client file uploads. |
    | `local_infile` | `bool` | - | Alias enabling `LOAD DATA LOCAL INFILE` transfers. |
    | `allow_local_infile_in_path` | `str` | - | Restrict `LOAD DATA LOCAL INFILE` uploads to files beneath this directory. |
    | `use_pure` | `bool` | - | Force the pure-Python Connector/Python implementation instead of the C extension. |
    | `dsn` | `str` | - | MySQL connection URI parsed by SQLSpec into Connector/Python keyword arguments. |
    | `url` | `str` | - | Connection URL alias parsed identically to `dsn`. |
    | `connection_string` | `str` | - | Connection string alias parsed identically to `dsn`. |
    | `extra` | `dict[str, Any]` | - | Additional keyword arguments forwarded to `mysql.connector.connect`. |
    | `pool_name` | `str` | - | Identifier assigned to the native `MySQLConnectionPool`. |
    | `pool_size` | `int` | - | Number of connections maintained by `MySQLConnectionPool` (max `32` by default). |
    | `pool_reset_session` | `bool` | - | Reset session variables when returning a connection to `MySQLConnectionPool`. |
    <!-- GENERATED:END config-table:sqlspec.adapters.mysqlconnector.config.MysqlConnectorSyncConnectionParams,sqlspec.adapters.mysqlconnector.config.MysqlConnectorPoolParams -->

    **Driver Features (`MysqlConnectorDriverFeatures`)**

    <!-- GENERATED:START config-table:sqlspec.adapters.mysqlconnector.config.MysqlConnectorDriverFeatures -->
    | Key | Type | Default | Description |
    | --- | --- | --- | --- |
    | `json_serializer` | `Callable[[Any], str]` | `to_json` | Custom JSON serializer applied to bound JSON parameters (defaults to `to_json`). |
    | `json_deserializer` | `Callable[[str], Any]` | `from_json` | Custom JSON deserializer applied to JSON result columns (defaults to `from_json`). |
    | `on_connection_create` | `Callable[..., Any]` | - | Sync or async callback invoked when a physical or session-reset connection is acquired. |
    | `enable_events` | `bool` | - | Enable durable table-backed SQLSpec event queue support. |
    | `events_backend` | `Literal['poll_queue']` | - | Event channel backend (`poll_queue` is the sole MySQL backend). |
    | `cursor_options` | `MysqlConnectorCursorParams` | - | Default `MysqlConnectorCursorParams` forwarded to `connection.cursor()`. |
    | `enable_local_infile_bulk_load` | `bool` | - | Route `load_from_arrow` through `LOAD DATA LOCAL INFILE` when local infile is enabled. |
    <!-- GENERATED:END config-table:sqlspec.adapters.mysqlconnector.config.MysqlConnectorDriverFeatures -->

## Type Mappings & Dialect Notes

| Database Type | Python Type | Notes / Feature Flag |
| --- | --- | --- |
| `JSON` | `dict` / `list` | Serialized and deserialized automatically via `json_serializer` and `json_deserializer`. |
| `TINYINT(1)` / `BOOLEAN` | `int` / `bool` | Python `bool` parameters bind as `1` / `0`; result columns return `int`. |
| `CHAR(36)` / `BINARY(16)` | `uuid.UUID` / `str` / `bytes` | Coerces Python `uuid.UUID` parameters to string or binary storage formats. |
| `DECIMAL` / `NUMERIC` | `decimal.Decimal` | Exact arbitrary-precision decimal arithmetic. |
| `DATETIME` / `TIMESTAMP` | `datetime.datetime` | Microsecond-precision timestamp handling (`DATETIME(6)`). |
| `BLOB` / `LONGBLOB` | `bytes` / `bytearray` | Binary payload storage. |

- **Upserts with ON DUPLICATE KEY UPDATE**: Build conflict-tolerant writes with `sql.insert("users").values(...).on_duplicate_key_update(...)`, which compiles to MySQL and MariaDB `ON DUPLICATE KEY UPDATE` syntax.
- **LAST_INSERT_ID() vs MariaDB RETURNING**: MariaDB 10.5+ supports `INSERT ... RETURNING` and `DELETE ... RETURNING` directly via `session.select(...)`. Standard MySQL does not support `RETURNING`, and SQLSpec captures `LAST_INSERT_ID()` into `result.last_inserted_id` and `result.rows_affected` on the returned `SQLResult`.
- **Placeholder Compilation and utf8mb4**: Write `:named` or `?` parameters in SQLSpec; all MySQL adapters compile placeholders to `%s` automatically while preserving literal `%` signs in `LIKE '100%'` expressions. Always use `utf8mb4` so 4-byte UTF-8 characters never truncate.
- **Implicit DDL Commits**: MySQL and MariaDB issue an implicit `COMMIT` before and after `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, and `TRUNCATE TABLE`. Keep schema migrations separate from transactional application tests that rely on rollback isolation.

## Read Replicas and GTID Waits

All four MySQL/MariaDB adapters accept reader endpoints through `replicas=`. Point them at MySQL Router, ProxySQL, an Aurora/RDS reader endpoint, or a server you already replicate. `True` is rejected because it cannot identify that endpoint.

Derived reader configs append `SET SESSION TRANSACTION READ ONLY` to `init_command` once. Pass `read_only=False` explicitly to opt out. Alias overrides replace inherited values, and connection hooks remain attached. Sync mysql-connector readers get a distinct pool name and default `pool_reset_session=False`, so reset does not erase their read-only state.

`wait_for_gtid_set(gtid_set, timeout=2.0, mariadb=False)` returns whether the requested position is applied. It binds the GTID and timeout to `WAIT_FOR_EXECUTED_GTID_SET`; `mariadb=True` selects `MASTER_GTID_WAIT`. Await it on async drivers. Empty GTIDs, negative or nonfinite timeouts, and boolean timeout values fail before a query. Keep a positive timeout for a bounded MySQL wait: MySQL treats `0` as unlimited, while MariaDB treats `0` as a nonblocking check.

Errnos `1792`, `1836`, and SQLSTATE `25006` map to `ReadOnlyTransactionError`. Errno `1290` does so only if its message names read-only mode; another option error keeps its existing category. See [Replica Operations](../learn/recipes/read-replicas-operations.md).

## See Also

- [AsyncMy API Reference](/reference/adapters/asyncmy/)
- [AioMySQL API Reference](/reference/adapters/aiomysql/)
- [PyMySQL API Reference](/reference/adapters/pymysql/)
- [MySQL Connector API Reference](/reference/adapters/mysqlconnector/)
- [MySQL Dialect Reference](/reference/data-dictionary/dialects/mysql/)
- [MariaDB Dialect Reference](/reference/data-dictionary/dialects/mariadb/)
