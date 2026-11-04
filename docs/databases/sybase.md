# SAP Adaptive Server Enterprise (Sybase ASE)

`sqlspec.adapters.sybase` provides first-class support for SAP Adaptive Server Enterprise (SAP ASE) 16.0+ via `pymssql` (`>= 2.4.4`) over FreeTDS TDS 5.0 wire protocol, featuring connection pooling (`SybaseConfig`), custom SQLGlot `sybase` dialect compilation, system catalog introspection, and Litestar, Events, and Google ADK session/memory store extensions.

## Install and When to Choose

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

- Choose **`SybaseConfig`** for transactional OLTP applications communicating with SAP ASE database instances.
- Prebuilt `pymssql` wheels ship statically linked FreeTDS with native TDS 5.0 wire support across Linux, macOS, and Windows.
- Note: Neither Apache Arrow ADBC nor GizmoData provides native ADBC drivers for SAP ASE, so SQLSpec communicates with ASE directly via FreeTDS TDS 5.0 DB-API connections.

## Connection Setup and Protocol Negotiation

Sybase ASE requires TDS 5.0 protocol negotiation. FreeTDS negotiates TDS 5.0 either when an environment variable `TDSVER=5.0` is set before process start, or when a FreeTDS server section alias is defined in `freetds.conf`:

=== "Environment Variable (`TDSVER=5.0`)"

    ```bash
    export TDSVER=5.0
    ```

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

=== "FreeTDS Server Alias (`server_alias`)"

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

An alias supplies the host and TDS version. Set `port` as well, even if it is in `freetds.conf`. `pymssql` replaces the port from that file with the value you pass. A missing, empty, or zero port raises `ImproperConfigurationError`.

## Key Configuration Parameters

| Setting | Location | Description |
| --- | --- | --- |
| `server` / `host` | `connection_config` | Database host name or IP address (default port `5000`). |
| `server_alias` / `freetds_server` | `connection_config` | Section name in `freetds.conf` configured with `tds version = 5.0`; requires an explicit `port`. |
| `is_server_alias` | `connection_config` | Set to `True` when `server` names a FreeTDS section. The same explicit-port requirement applies. |
| `database` / `db` | `connection_config` | Target database catalog name. |
| `user` / `username` | `connection_config` | Authentication login principal. |
| `password` | `connection_config` | Authentication password credentials. |
| `autocommit` | `connection_config` | Defaults to `True` to allow DDL execution outside implicit transactions. |
| `conn_properties` | `connection_config` | Defaults to `SET CHAINED OFF`, `SET QUOTED_IDENTIFIER ON`, `SET ANSINULL ON`, and `SET TEXTSIZE 2147483647`. A supplied value replaces these defaults. |
| `read_only` | `connection_config` | Passed as native pymssql login intent. It does not enforce read-only access in ASE; use database permissions. |
| `pool_recycle_seconds` | `connection_config` | Maximum connection age in seconds before recycling (defaults to 24 hours). |
| `health_check_interval` | `connection_config` | Minimum idle time in seconds before running liveness probes. |
| `json_serializer` / `json_deserializer` | `driver_features` | Custom JSON text serialization/deserialization callables. |

## Dialect and Catalog Notes

- **Autocommit and DDL in Transactions**: In Sybase ASE, statements like `CREATE TABLE` and `CREATE INDEX` raise error 2762 inside multi-statement transactions unless database option `ddl in tran` is enabled. `SybaseConfig` defaults `autocommit=True` and begins explicit transactions on demand via `driver.begin()`.
- **Pagination**: Bounded ADK session, event, and memory queries use `SELECT TOP n` with no offset. Positive offsets use `ROWS LIMIT n OFFSET m`; check that your ASE version and server mode support it.
- **System Catalogs**: Reflection queries introspect `dbo.sysobjects`, `dbo.syscolumns`, `dbo.sysindexes`, `dbo.sysreferences`, and `dbo.systypes`.

## Arrow and Staged Storage Loads

`CreateTable.from_arrow_schema()` maps Arrow timestamps to `BIGDATETIME`. ASE's `TIMESTAMP` type holds a row version. `BIGDATETIME` has no time-zone field, so normalize zone-aware values before loading them. String primary keys use `VARCHAR(255)`. Decimal precision above 38 raises `SQLBuilderError`.

With `conflict_keys`, staged loads update matching rows, then insert new rows in a separate statement. Coordinate concurrent loads that use the same keys. See [Arrow and Storage](../learn/arrow-and-storage.md#sap-column-types-and-staged-loads) for conflict options and SQL Anywhere differences.

See the [adapter matrix](index.md), [configuration guide](../learn/configuration.md), and [Sybase API reference](../reference/adapters/sybase.md) for shared features and configuration details.
