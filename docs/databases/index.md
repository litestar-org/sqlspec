# Databases

SQLSpec supports synchronous and asynchronous **database adapters** spanning embedded engines, relational OLTP servers, enterprise mainframes, distributed SQL clusters, and cloud analytical warehouses. Every adapter exposes the same `provide_session()` lifecycle, parameter compiler, migration runner, data dictionary, Litestar store, Events queue store, and Google ADK session/memory stores.

## Adapter Capability Matrix

| Adapter | Install Extra | Mode | Native Param Style | Pooling | Native Arrow | Events Backend | ADK Stores and Vector Search |
| --- | --- | --- | --- | --- | --- | --- | --- |
| [`asyncpg`](postgres/asyncpg.md) | `sqlspec[asyncpg]` | Async | `$1` (`dollar`) | `asyncpg.Pool` | Row / COPY | `LISTEN/NOTIFY` + Table Queue | Sessions + Memory (`pgvector`, `ParadeDB`, `pg_textsearch`, RRF) |
| [`psycopg`](postgres/psycopg.md) | `sqlspec[psycopg]` | Sync + Async | `%s` / `%(name)s` | `ConnectionPool` / `AsyncConnectionPool` / `NullPool` | Row / COPY | `LISTEN/NOTIFY` + Table Queue | Sessions + Memory (`pgvector`, `ParadeDB`, `pg_textsearch`, RRF) |
| [`psqlpy`](postgres/psqlpy.md) | `sqlspec[psqlpy]` | Async | `$1` (`dollar`) | Rust Tokio `ConnectionPool` | Row | `LISTEN/NOTIFY` + Table Queue | Sessions + Memory (`PgVector`, `ParadeDB`, `pg_textsearch`, RRF) |
| [`cockroach_asyncpg`](cockroachdb.md) | `sqlspec[cockroachdb]` | Async | `$1` (`dollar`) | `asyncpg.Pool` | Row | Table Queue | Sessions + Memory (`pgvector`, FTS) |
| [`cockroach_psycopg`](cockroachdb.md) | `sqlspec[cockroachdb]` | Sync + Async | `%s` / `%(name)s` | `ConnectionPool` / `AsyncConnectionPool` | Row | Table Queue | Sessions + Memory (`pgvector`, FTS) |
| [`sqlite`](sqlite.md) | `sqlspec` (stdlib) | Sync | `?` (`qmark`) | `SqliteConnectionPool` | Row / Batched | Table Queue | Sessions + Memory (`FTS5`, `LIKE`) |
| [`aiosqlite`](sqlite.md) | `sqlspec[aiosqlite]` | Async | `?` (`qmark`) | `AiosqliteConnectionPool` | Row / Batched | Table Queue | Sessions + Memory (`FTS5`, `LIKE`) |
| [`asyncmy`](mysql.md) | `sqlspec[asyncmy]` | Async | `%s` (`format`) | `asyncmy.Pool` | Row / `LOAD DATA` | Table Queue | Sessions + Memory (`FULLTEXT`, `LIKE`) |
| [`aiomysql`](mysql.md) | `sqlspec[aiomysql]` | Async | `%s` (`format`) | `aiomysql.Pool` | Row / `LOAD DATA` | Table Queue | Sessions + Memory (`FULLTEXT`, `LIKE`) |
| [`pymysql`](mysql.md) | `sqlspec[pymysql]` | Sync | `%s` (`format`) | `PyMySQLConnectionPool` | Row / `LOAD DATA` | Table Queue | Sessions + Memory (`FULLTEXT`, `LIKE`) |
| [`mysqlconnector`](mysql.md) | `sqlspec[mysql-connector]` | Sync + Async | `%s` (`format`) | `MySQLConnectionPool` / Async Pool | Row / `LOAD DATA` | Table Queue | Sessions + Memory (`FULLTEXT`, `LIKE`) |
| [`oracledb`](oracledb.md) | `sqlspec[oracledb]` | Sync + Async | `:1` / `:name` | `ConnectionPool` / `AsyncConnectionPool` + DRCP | PyCapsule Batches | Oracle AQ (`DBMS_AQ`) + Table Queue | Sessions + Memory (Oracle 26ai `VECTOR` HNSW, Oracle Text, RRF) |
| [`mssql_python`](mssql.md) | `sqlspec[mssql-python]` | Sync | `?` (`qmark`) | `MssqlPythonConnectionPool` | Native `BulkCopy` | Table Queue | Sessions + Memory (`LIKE`) |
| [`pymssql`](mssql.md) | `sqlspec[pymssql]` | Sync | `%(name)s` / `%s` | `PymssqlConnectionPool` | Row | Table Queue | Sessions + Memory (`LIKE`) |
| [`sybase`](sybase.md) | `sqlspec[sybase]` | Sync | `?` (`qmark`) / `%(name)s` | `SybaseConnectionPool` | Row / Stream | Table Queue | Sessions + Memory (`LIKE`) |
| [`sqlanywhere`](sqlanywhere.md) | `sqlspec[sqlanywhere]` | Sync | `?` (`qmark`) | `SqlanywhereConnectionPool` | Row / Stream | Table Queue | Sessions + Memory (`LIKE`) |
| [`db2`](db2.md) | `sqlspec[db2]` | Sync + Async | `?` (`qmark`) | `Db2SyncConnectionPool` / `Db2AsyncConnectionPool` | Schema-Guided Row | Table Queue (`SKIP LOCKED DATA`) | Sessions + Memory (`POSSTR`) |
| [`duckdb`](duckdb.md) | `sqlspec[duckdb]` | Sync | `?` (`qmark`) | `DuckDBConnectionPool` | Native Zero-Copy | Table Queue | Sessions + Memory (`FTS`, `ARRAY` Cosine) |
| [`bigquery`](bigquery.md) | `sqlspec[bigquery]` | Sync | `@name` (`named`) | Stateless HTTP/gRPC Client | Storage Read / Write API | Table Queue | Sessions + Memory (`SEARCH` / `LIKE`) |
| [`spanner`](spanner.md) | `sqlspec[spanner]` | Sync + Async | `@name` (`named`) | `PingingPool` / `TransactionPingingPool` | Row / Batched | Lock-Free Snapshot + `run_in_transaction` | Sessions + Memory (`ARRAY<FLOAT32>`, FTS `SCORE`, RRF) + Artifacts |
| [`adbc`](adbc.md) | `sqlspec[adbc]` | Sync | `?` / `$1` / `:1` | Optional PostgreSQL clone pool | Native C-Stream | Table Queue | Sessions + Memory (PostgreSQL `pgvector` RRF, DuckDB, Oracle, Db2) |
| [`arrow_odbc`](arrow-odbc.md) | `sqlspec[arrow-odbc]` | Sync | `?` (`qmark`) | ODBC Driver Pool (`enable_pooling`) | Native Rust ODBC Batch | Table Queue | Sessions + Memory (`LIKE` / `POSSTR`) |

## Database Setup Guides

- **[PostgreSQL and AlloyDB](postgres/index.md)**: Compare [`asyncpg`](postgres/asyncpg.md), [`psycopg`](postgres/psycopg.md), and [`psqlpy`](postgres/psqlpy.md), configure PgBouncer `pooler_mode` with `direct_dsn` bypasses, and enable Google Cloud AlloyDB columnar scans.
- **[CockroachDB](cockroachdb.md)**: Distributed `cockroach_asyncpg` and `cockroach_psycopg` pools with automatic `40001` serialization retry and `AS OF SYSTEM TIME` follower reads.
- **[SQLite and Aiosqlite](sqlite.md)**: Zero-dependency local persistence, WAL pragma tuning, shared-memory `:memory:` URIs, and custom SQL functions.
- **[MySQL and MariaDB](mysql.md)**: Side-by-side setup for `asyncmy`, `pymysql`, `aiomysql`, and `mysql-connector-python`, TLS verification, and `LOAD DATA LOCAL INFILE` bulk loading.
- **[Oracle Database](oracledb.md)**: Thin and Thick modes, mTLS/PKCS#12 wallets, TNS topologies, Oracle 26ai `VECTOR` + Hybrid RRF search, PyCapsule Arrow streaming, and `DBMS_CLOUD.COPY_DATA`.
- **[Microsoft SQL Server](mssql.md)**: Microsoft's official `mssql-python` driver with streaming Arrow `BulkCopy` alongside FreeTDS `pymssql`.
- **[SAP Adaptive Server Enterprise](sybase.md)**: Sybase ASE 16.0+ via FreeTDS TDS 5.0 (`pymssql`), transactional DDL behavior, and catalog introspection.
- **[SAP SQL Anywhere](sqlanywhere.md)**: Embedded and client-server SQL Anywhere 12+ via `sqlanydb`, recursive dependency analysis, and automatic rollback on context exit.
- **[IBM Db2](db2.md)**: Sync and async DRDA pooling (`Db2SyncConfig` / `Db2AsyncConfig`), savepoint cursor retention, and lowercase column normalization.
- **[DuckDB](duckdb.md)**: In-process OLAP queries over Parquet/S3, hybrid native/Arrow materialization, and JSON statement profiling.
- **[Google Cloud BigQuery](bigquery.md)**: Interactive `query_and_wait` execution, Storage Read/Write APIs, reservation governance, and multi-statement script jobs.
- **[Google Cloud Spanner](spanner.md)**: Symmetric sync and native `asyncio` drivers, GoogleSQL (`spanner`) and PostgreSQL (`spangres`) dialects, `run_in_transaction`, and vector/artifact ADK stores.
- **[Apache Arrow ADBC](adbc.md)**: Zero-copy Arrow C Data Interface ingestion and extraction across PyPI-installable ADBC wire drivers.
- **[Arrow ODBC](arrow-odbc.md)**: Rust-backed zero-copy ODBC batch streaming into Apache Arrow tables for SQL Server, PostgreSQL, and Db2.
