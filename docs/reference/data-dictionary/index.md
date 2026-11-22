# Data Dictionary

```python
from sqlspec.data_dictionary import (
    ColumnMetadata,
    ColumnProfileMetadata,
    ConstraintMetadata,
    DataDictionaryLoader,
    DDLResult,
    DependencyEdge,
    DependencySortResult,
    DialectConfig,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataCapabilityProfile,
    MetadataResult,
    RoutineMetadata,
    SystemMetadataRequest,
    SystemMetadataResult,
    TableMetadata,
    TriggerMetadata,
    VersionInfo,
    ViewMetadata,
    dependency_edges_from_foreign_keys,
    dependency_edges_from_metadata,
    get_data_dictionary_loader,
    get_dialect_config,
    list_registered_dialects,
    normalize_dialect_name,
    register_dialect,
    sort_ddl_results,
    sort_dependencies,
)
```

Cross-database catalog introspection metadata, DDL extraction, column profiling, capability profiles, dialect registry, and foreign-key topological sorting. Access runtime introspection through `driver.data_dictionary` on any [`SyncDriverAdapterBase`](../driver/sync.md) or [`AsyncDriverAdapterBase`](../driver/async.md) session (see [Runtime Introspection & DDL Synthesis](runtime.md) and the [Dialect SQL Catalogs](dialects/index.md)).

## Loader & Dialect Registry

::: sqlspec.data_dictionary._loader.DataDictionaryLoader
    options:
      heading_level: 3
      members: false

::: sqlspec.data_dictionary._loader.get_data_dictionary_loader
    options:
      heading_level: 4

::: sqlspec.data_dictionary._registry.get_dialect_config
    options:
      heading_level: 4

::: sqlspec.data_dictionary._registry.list_registered_dialects
    options:
      heading_level: 4

::: sqlspec.data_dictionary._registry.normalize_dialect_name
    options:
      heading_level: 4

::: sqlspec.data_dictionary._registry.register_dialect
    options:
      heading_level: 4

## Schema & Catalog Metadata Types

::: sqlspec.data_dictionary._types.VersionInfo
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.TableMetadata
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.ColumnMetadata
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.ColumnProfileMetadata
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.IndexMetadata
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.ForeignKeyMetadata
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.ConstraintMetadata
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.ViewMetadata
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.RoutineMetadata
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.TriggerMetadata
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.DDLResult
    options:
      heading_level: 4
      members: false

## Capability Profiles & Result Envelopes

::: sqlspec.data_dictionary._types.DialectConfig
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.MetadataCapabilityProfile
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.MetadataResult
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.SystemMetadataRequest
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.SystemMetadataResult
    options:
      heading_level: 4
      members: false

## Dependency Graph & Topological Sorting

::: sqlspec.data_dictionary._dependencies.DependencyEdge
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._dependencies.DependencySortResult
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._dependencies.dependency_edges_from_foreign_keys
    options:
      heading_level: 4

::: sqlspec.data_dictionary._dependencies.dependency_edges_from_metadata
    options:
      heading_level: 4

::: sqlspec.data_dictionary._dependencies.sort_dependencies
    options:
      heading_level: 4

::: sqlspec.data_dictionary._dependencies.sort_ddl_results
    options:
      heading_level: 4

## Shared Dialect Base Classes

Every dialect package under `sqlspec.data_dictionary.dialects.<dialect>` exports driver-free, interpreted data dictionary base classes with zero adapter or third-party driver imports. Built-in and external adapter authors targeting one of the 15 supported engines should subclass the matching shared dialect base rather than re-implementing catalog reflection directly on [`SyncDataDictionaryBase`](runtime.md#synchronous-introspection) or [`AsyncDataDictionaryBase`](runtime.md#asynchronous-introspection).

Package-level re-exports (`from sqlspec.data_dictionary.dialects.<dialect> import ...`) load `dictionary.py` lazily via PEP 562 `__getattr__` and `__dir__`, while compiled `config.py` constants and helper functions remain eager. Bare config or registry imports such as `import sqlspec.data_dictionary.dialects` never pull `dictionary.py` or `sqlspec.driver` on startup. Adapter implementations can import shared bases through the package facade (`sqlspec.data_dictionary.dialects.<dialect>`) or directly from `sqlspec.data_dictionary.dialects.<dialect>.dictionary`.

| Dialect | Module | Sync Base Class | Async Base Class | Built-In Adapter Subclasses |
| --- | --- | --- | --- | --- |
| [MySQL](dialects/mysql.md) (`mysql`) | `sqlspec.data_dictionary.dialects.mysql` | `MySQLSyncDataDictionary` | `MySQLAsyncDataDictionary` | `PyMysqlDataDictionary` (`pymysql`), `MysqlConnectorSyncDataDictionary` / `MysqlConnectorAsyncDataDictionary` (`mysqlconnector`), `AsyncmyDataDictionary` (`asyncmy`), `AiomysqlDataDictionary` (`aiomysql`) |
| [MariaDB](dialects/mariadb.md) (`mariadb`) | `sqlspec.data_dictionary.dialects.mariadb` | `MariaDBSyncDataDictionary` | `MariaDBAsyncDataDictionary` | - (shared via the MySQL adapters with runtime MariaDB detection) |
| [PostgreSQL](dialects/postgres.md) (`postgres`) | `sqlspec.data_dictionary.dialects.postgres` | `PostgresSyncDataDictionary` | `PostgresAsyncDataDictionary` | `PsycopgSyncDataDictionary` / `PsycopgAsyncDataDictionary` (`psycopg`), `AsyncpgDataDictionary` (`asyncpg`), `PsqlpyDataDictionary` (`psqlpy`) |
| [CockroachDB](dialects/cockroachdb.md) (`cockroachdb`) | `sqlspec.data_dictionary.dialects.cockroachdb` | `CockroachDBSyncDataDictionary` | `CockroachDBAsyncDataDictionary` | `CockroachPsycopgSyncDataDictionary` / `CockroachPsycopgAsyncDataDictionary` (`cockroach_psycopg`), `CockroachAsyncpgDataDictionary` (`cockroach_asyncpg`) |
| [SQLite](dialects/sqlite.md) (`sqlite`) | `sqlspec.data_dictionary.dialects.sqlite` | `SqliteSyncDataDictionary` | `SqliteAsyncDataDictionary` | `SqliteDataDictionary` (`sqlite`), `AiosqliteDataDictionary` (`aiosqlite`) |
| [SQL Server](dialects/mssql.md) (`mssql`) | `sqlspec.data_dictionary.dialects.mssql` | `MssqlSyncDataDictionary` | `-` | `MssqlPythonSyncDataDictionary` (`mssql_python`), `PymssqlSyncDataDictionary` (`pymssql`) |
| [Oracle Database](dialects/oracle.md) (`oracle`) | `sqlspec.data_dictionary.dialects.oracle` | `OracleSyncDataDictionary` | `OracleAsyncDataDictionary` | `OracledbSyncDataDictionary` / `OracledbAsyncDataDictionary` (`oracledb`) |
| [DuckDB](dialects/duckdb.md) (`duckdb`) | `sqlspec.data_dictionary.dialects.duckdb` | `DuckDBSyncDataDictionary` | `-` | `DuckDBDataDictionary` (`duckdb`) |
| [Google BigQuery](dialects/bigquery.md) (`bigquery`) | `sqlspec.data_dictionary.dialects.bigquery` | `BigQuerySyncDataDictionary` | `-` | `BigQueryDataDictionary` (`bigquery`) |
| [Google Cloud Spanner](dialects/spanner.md) (`spanner`) | `sqlspec.data_dictionary.dialects.spanner` | `SpannerSyncDataDictionary` | `SpannerAsyncDataDictionary` | `SpannerSyncDataDictionary` / `SpannerAsyncDataDictionary` (`spanner`) |
| [IBM Db2](dialects/db2.md) (`db2`) | `sqlspec.data_dictionary.dialects.db2` | `Db2SyncDataDictionary` | `Db2AsyncDataDictionary` | `Db2SyncDataDictionary` / `Db2AsyncDataDictionary` (`db2`) |
| [Snowflake](dialects/snowflake.md) (`snowflake`) | `sqlspec.data_dictionary.dialects.snowflake` | `SnowflakeSyncDataDictionary` | `-` | `SnowflakeSyncDataDictionary` (`snowflake`) |
| [Sybase ASE](dialects/sybase.md) (`sybase`) | `sqlspec.data_dictionary.dialects.sybase` | `SybaseSyncDataDictionary` | `-` | `SybaseSyncDataDictionary` (`sybase`) |
| [SAP SQL Anywhere](dialects/sqlanywhere.md) (`sqlanywhere`) | `sqlspec.data_dictionary.dialects.sqlanywhere` | `SqlanywhereSyncDataDictionary` | `-` | `SqlanywhereSyncDataDictionary` (`sqlanywhere`) |
| [Teradata Vantage](dialects/teradata.md) (`teradata`) | `sqlspec.data_dictionary.dialects.teradata` | `TeradataSyncDataDictionary` | `-` | `TeradataSyncDataDictionary` (`teradata`) |

Multi-dialect transport adapters (`AdbcDataDictionary` in `sqlspec.adapters.adbc` and `ArrowOdbcDataDictionary` in `sqlspec.adapters.arrow_odbc`) dispatch across multiple engines at runtime and remain direct subclasses of `SyncDataDictionaryBase`.

When implementing a third-party adapter for a supported engine, subclass the corresponding shared dialect base, decorate the subclass with `@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)`, and omit `__slots__ = ()` so compiled MyPyC driver bases and interpreted subclasses avoid layout conflicts:

```python
from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.postgres import PostgresAsyncDataDictionary


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class CustomPostgresDataDictionary(PostgresAsyncDataDictionary):
    """Custom asynchronous PostgreSQL data dictionary."""

    dialect: ClassVar[str] = "postgres"
```
