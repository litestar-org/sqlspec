# SAP Adaptive Server Enterprise (Sybase ASE)

`sqlspec.adapters.sybase` provides synchronous support for SAP Adaptive Server Enterprise (SAP ASE) 16.0+ via `pymssql` (`>= 2.4.4`) over FreeTDS TDS 5.0 wire protocol, featuring thread-local connection pooling (`SybaseConfig`), custom SQLGlot `sybase` dialect compilation, system catalog introspection, and Litestar, Events, and Google ADK session/memory store extensions.

## Overview & Capabilities

| Capability | Details |
| --- | --- |
| **Adapter Config** | `SybaseConfig` (`sqlspec.adapters.sybase`) |
| **Driver Extra** | `sqlspec[sybase]` (`pymssql >= 2.4.4` with FreeTDS TDS 5.0) |
| **Execution Mode** | Synchronous |
| **Pool Architecture** | `SybaseConnectionPool` (thread-local manager with `pool_recycle_seconds`, `health_check_interval`) |
| **Dialect & Extensions** | Custom SQLGlot `sybase` dialect, Litestar store, Events queue store, Google ADK session & memory stores |

## Installation & Quickstart

=== "uv"

    ```bash
    uv add "sqlspec[sybase]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[sybase]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[sybase]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[sybase]"
    ```

### Connection Setup and Protocol Negotiation

Sybase ASE requires TDS 5.0 protocol negotiation. FreeTDS negotiates TDS 5.0 either when `TDSVER=5.0` is set in the environment before process start or when a FreeTDS server section alias is defined in `freetds.conf`:

=== "Environment Variable (TDSVER=5.0)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.sybase import SybaseConfig

    spec = SQLSpec()
    config = spec.add_config(
        SybaseConfig(
            connection_config={
                "server": "ase.example.internal",
                "port": 5000,
                "user": "sa",
                "password": "secret_password",
                "database": "app_db",
                "autocommit": True,
            }
        )
    )
    ```

=== "FreeTDS Server Alias (server_alias)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.sybase import SybaseConfig

    spec = SQLSpec()
    config = spec.add_config(
        SybaseConfig(
            connection_config={
                "server_alias": "SYBASE_PROD",
                "port": 5000,
                "user": "sa",
                "password": "secret_password",
                "database": "app_db",
            }
        )
    )
    ```

An alias supplies the host and TDS version; always set `port` explicitly as well, because `pymssql` overrides the port from `freetds.conf` and a missing or zero port raises `ImproperConfigurationError`.

## Connection & Pool Configuration

### Connection and Pool Parameters

Pass `SybaseConnectionParams` and `SybasePoolParams` keys inside `connection_config={...}` on `SybaseConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.sybase.config.SybaseConnectionParams,sqlspec.adapters.sybase.config.SybasePoolParams -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `server` | `str` | - | Database server host, IP, or FreeTDS server definition name. |
| `host` | `str` | - | Alias for `server`. |
| `freetds_server` | `str` | - | FreeTDS server configuration section name; requires an explicit port. |
| `server_alias` | `str` | - | FreeTDS server configuration section name; requires an explicit port. |
| `is_server_alias` | `bool` | - | Treat `server` as a FreeTDS configuration section name; requires an explicit port. |
| `user` | `str` | - | Login user name. |
| `username` | `str` | - | Alias for `user`. |
| `password` | `str` | - | Authentication password. |
| `database` | `str` | - | Initial database name to connect to. |
| `db` | `str` | - | Alias for `database`. |
| `port` | `int or str` | - | TCP port number; required for section aliases, otherwise defaults to 5000. |
| `timeout` | `SybaseTimeout` | - | Query execution timeout in seconds. |
| `login_timeout` | `SybaseTimeout` | - | Login and connection timeout in seconds. |
| `charset` | `str` | - | Client character set encoding. |
| `as_dict` | `bool` | - | Return query result rows as dictionaries. |
| `appname` | `str` | - | Application name sent to the server for connection identification. |
| `conn_properties` | `str` | - | Session initialization SQL; replaces the default connection properties when supplied. |
| `autocommit` | `bool` | - | Enable autocommit mode for transactions. |
| `tds_version` | `str` | - | TDS protocol version (such as `'5.0'` or `'7.4'`). |
| `encryption` | `Literal['off', 'request', 'require']` | - | TDS connection encryption level. |
| `arraysize` | `int` | - | Default cursor fetch array size. |
| `conv` | `Mapping[int or type[Any], Callable[..., Any]]` | - | Custom datatype conversion dictionary mapping TDS types to python types. |
| `read_only` | `bool` | - | Native pymssql login intent; does not enforce ASE read-only permissions. |
| `pool_recycle_seconds` | `int` | - | Maximum age in seconds before a pooled connection is recycled. |
| `health_check_interval` | `float` | - | Idle duration in seconds before a connection is checked on checkout. |
| `extra` | `dict[str, Any]` | - | Additional driver connection arguments. |
<!-- GENERATED:END config-table:sqlspec.adapters.sybase.config.SybaseConnectionParams,sqlspec.adapters.sybase.config.SybasePoolParams -->

### Driver Features

Pass `SybaseDriverFeatures` keys inside `driver_features={...}` on `SybaseConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.sybase.config.SybaseDriverFeatures -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `json_serializer` | `Callable[[Any], str]` | - | Custom callable for serializing objects to JSON strings. |
| `json_deserializer` | `Callable[[str], Any]` | - | Custom callable for deserializing JSON strings. |
| `on_connection_create` | `Callable[[SybaseConnection], None]` | - | Callback invoked immediately when a new connection is established. |
| `enable_events` | `bool` | - | Enable database event listening. |
| `events_backend` | `Literal['poll_queue']` | - | Backend mechanism used for dispatching events. |
<!-- GENERATED:END config-table:sqlspec.adapters.sybase.config.SybaseDriverFeatures -->

## Type Mappings & Dialect Notes

| Database Type | Python Type | Notes / Feature Flag |
| --- | --- | --- |
| `BIT` | `bool` / `int` | In SAP ASE, `BIT` columns are non-nullable by default (`BIT DEFAULT 0 NOT NULL`) unless explicitly altered. |
| `MONEY` / `SMALLMONEY` | `decimal.Decimal` | Fixed-scale currency types mapped to exact `Decimal` values. |
| `DATETIME` / `SMALLDATETIME` / `BIGDATETIME` | `datetime.datetime` | `CreateTable.from_arrow_schema()` maps Arrow timestamps to `BIGDATETIME` (ASE's `TIMESTAMP` keyword denotes a binary row-version column). Normalize timezone-aware values to UTC beforehand. |
| `IMAGE` / `TEXT` / `UNITEXT` | `bytes` / `str` | Large object (LOB) columns governed by `SET TEXTSIZE 2147483647` in default `conn_properties`. |
| `VARCHAR(36)` / `VARBINARY(16)` | `uuid.UUID` / `str` | UUID parameters serialize cleanly to character or binary representations. |
| `TEXT` / `VARCHAR` (`XML` / JSON) | `dict` / `list` / `str` | JSON payloads serialize via `json_serializer` and `json_deserializer`. |

- **Autocommit and DDL in Transactions**: In Sybase ASE, statements like `CREATE TABLE` and `CREATE INDEX` raise error `2762` inside multi-statement transactions unless database option `ddl in tran` is enabled. `SybaseConfig` defaults `autocommit=True` and begins explicit transactions on demand via `session.begin()`.
- **Pagination (`SELECT TOP n` vs `ROWS LIMIT`)**: Zero-offset bounded queries (including ADK session, event, and memory queries) emit `SELECT TOP n` without an `OFFSET` clause. Positive offsets emit `ROWS LIMIT n OFFSET m` on ASE 16.0+ modes that support it.
- **Arrow Schema Inference and Staged Storage Loads**: `CreateTable.from_arrow_schema()` maps string primary keys to `VARCHAR(255)` and raises `SQLBuilderError` for decimal precision above `38`. During staged storage loads (`load_from_arrow` / `load_from_storage`) with `conflict_keys`, SQLSpec updates matching rows first and inserts new rows in a second statement; coordinate concurrent loads that target the same keys.
- **System Catalog Introspection**: Schema discovery queries `dbo.sysobjects`, `dbo.syscolumns`, `dbo.sysindexes`, `dbo.sysreferences`, and `dbo.systypes`.
- **Migration Schemas**: `-- schema:` directives, `default_schema`, and `version_table_schema` raise `MigrationError`. Qualify objects in migration SQL, and set `version_table_name` to `"<schema>.ddl_migrations"` to place the tracking table.

## See Also

- [Sybase API Reference](/reference/adapters/sybase/)
- [Sybase ASE Dialect Reference](/reference/data-dictionary/dialects/sybase/)
- [Arrow and Storage Guide](../learn/arrow-and-storage.md)
