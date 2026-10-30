# Database Adapters

Curated API reference for all SQLSpec database adapters (`sqlspec.adapters.*`). Each adapter page documents its own configuration class(es), `TypedDict` connection and pool parameter schemas, `DriverFeatures` flags, driver adapter class(es), and extension stores (`litestar`, `adk`, `events`) while linking inherited execution methods directly to [`SyncDriverAdapterBase`](../driver/sync.md) and [`AsyncDriverAdapterBase`](../driver/async.md).

## PostgreSQL & CockroachDB

| Adapter Module | Reference Page | Config Classes | Driver Classes | Setup Guide |
| --- | --- | --- | --- | --- |
| `sqlspec.adapters.asyncpg` | [asyncpg](asyncpg.md) | `AsyncpgConfig` | `AsyncpgDriver` | [PostgreSQL (asyncpg)](../../databases/postgres/asyncpg.md) |
| `sqlspec.adapters.psycopg` | [psycopg](psycopg.md) | `PsycopgSyncConfig`, `PsycopgAsyncConfig` | `PsycopgSyncDriver`, `PsycopgAsyncDriver` | [PostgreSQL (psycopg)](../../databases/postgres/psycopg.md) |
| `sqlspec.adapters.psqlpy` | [psqlpy](psqlpy.md) | `PsqlpyConfig` | `PsqlpyDriver` | [PostgreSQL (psqlpy)](../../databases/postgres/psqlpy.md) |
| `sqlspec.adapters.cockroach_asyncpg` | [cockroach-asyncpg](cockroach-asyncpg.md) | `CockroachAsyncpgConfig` | `CockroachAsyncpgDriver` | [CockroachDB](../../databases/cockroachdb.md) |
| `sqlspec.adapters.cockroach_psycopg` | [cockroach-psycopg](cockroach-psycopg.md) | `CockroachPsycopgSyncConfig`, `CockroachPsycopgAsyncConfig` | `CockroachPsycopgSyncDriver`, `CockroachPsycopgAsyncDriver` | [CockroachDB](../../databases/cockroachdb.md) |

## SQLite, MySQL & MariaDB

| Adapter Module | Reference Page | Config Classes | Driver Classes | Setup Guide |
| --- | --- | --- | --- | --- |
| `sqlspec.adapters.sqlite` | [sqlite](sqlite.md) | `SqliteConfig` | `SqliteDriver` | [SQLite](../../databases/sqlite.md) |
| `sqlspec.adapters.aiosqlite` | [aiosqlite](aiosqlite.md) | `AiosqliteConfig` | `AiosqliteDriver` | [SQLite](../../databases/sqlite.md) |
| `sqlspec.adapters.asyncmy` | [asyncmy](asyncmy.md) | `AsyncmyConfig` | `AsyncmyDriver` | [MySQL & MariaDB](../../databases/mysql.md) |
| `sqlspec.adapters.aiomysql` | [aiomysql](aiomysql.md) | `AiomysqlConfig` | `AiomysqlDriver` | [MySQL & MariaDB](../../databases/mysql.md) |
| `sqlspec.adapters.pymysql` | [pymysql](pymysql.md) | `PyMysqlConfig` | `PyMysqlDriver` | [MySQL & MariaDB](../../databases/mysql.md) |
| `sqlspec.adapters.mysqlconnector` | [mysqlconnector](mysqlconnector.md) | `MysqlConnectorSyncConfig`, `MysqlConnectorAsyncConfig` | `MysqlConnectorSyncDriver`, `MysqlConnectorAsyncDriver` | [MySQL & MariaDB](../../databases/mysql.md) |

## Oracle, SQL Server & IBM Db2

| Adapter Module | Reference Page | Config Classes | Driver Classes | Setup Guide |
| --- | --- | --- | --- | --- |
| `sqlspec.adapters.oracledb` | [oracledb](oracledb.md) | `OracleSyncConfig`, `OracleAsyncConfig` | `OracleSyncDriver`, `OracleAsyncDriver` | [Oracle Database](../../databases/oracledb.md) |
| `sqlspec.adapters.mssql_python` | [mssql-python](mssql-python.md) | `MssqlPythonConfig` | `MssqlPythonDriver` | [SQL Server](../../databases/mssql.md) |
| `sqlspec.adapters.pymssql` | [pymssql](pymssql.md) | `PymssqlConfig` | `PymssqlDriver` | [SQL Server](../../databases/mssql.md) |
| `sqlspec.adapters.db2` | [db2](db2.md) | `Db2SyncConfig`, `Db2AsyncConfig` | `Db2SyncDriver`, `Db2AsyncDriver` | [IBM Db2](../../databases/db2.md) |

## Cloud, Analytics & Arrow Connectors

| Adapter Module | Reference Page | Config Classes | Driver Classes | Setup Guide |
| --- | --- | --- | --- | --- |
| `sqlspec.adapters.duckdb` | [duckdb](duckdb.md) | `DuckDBConfig` | `DuckDBDriver` | [DuckDB](../../databases/duckdb.md) |
| `sqlspec.adapters.bigquery` | [bigquery](bigquery.md) | `BigQueryConfig` | `BigQueryDriver` | [BigQuery](../../databases/bigquery.md) |
| `sqlspec.adapters.spanner` | [spanner](spanner.md) | `SpannerSyncConfig`, `SpannerAsyncConfig` | `SpannerSyncDriver`, `SpannerAsyncDriver` | [Cloud Spanner](../../databases/spanner.md) |
| `sqlspec.adapters.adbc` | [adbc](adbc.md) | `AdbcConfig` | `AdbcDriver` | [ADBC](../../databases/adbc.md) |
| `sqlspec.adapters.arrow_odbc` | [arrow-odbc](arrow-odbc.md) | `ArrowOdbcConfig` | `ArrowOdbcDriver` | [Arrow ODBC](../../databases/arrow-odbc.md) |
