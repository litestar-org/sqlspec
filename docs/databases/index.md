# Databases

SQLSpec supports **20 database adapters** spanning embedded engines, relational OLTP servers, enterprise mainframes, distributed SQL clusters, and cloud analytical warehouses. Every adapter exposes the same `provide_session()` lifecycle, parameter compiler, migration runner, data dictionary, Litestar store, Events queue store, and Google ADK session/memory stores.

## Adapter Capability Matrix

| Adapter | Install Extra | Mode | Native Param Style | Pooling | Native Arrow | Events Backend | ADK Stores and Vector Search |
| --- | --- | --- | --- | --- | --- | --- | --- |
| [`asyncpg`](postgres/asyncpg.md) | `sqlspec[asyncpg]` | Async | `$1` (`dollar`) | `asyncpg.Pool` | Row / COPY | `LISTEN/NOTIFY` + Table Queue | Sessions + Memory (`pgvector`, `ParadeDB`, `pg_textsearch`, RRF) |
| [`psycopg`](postgres/psycopg.md) | `sqlspec[psycopg]` | Sync + Async | `%s` / `%(name)s` | `ConnectionPool` / `AsyncConnectionPool` / `NullPool` | Row / COPY | `LISTEN/NOTIFY` + Table Queue | Sessions + Memory (`pgvector`, `ParadeDB`, `pg_textsearch`, RRF) |
| [`psqlpy`](postgres/psqlpy.md) | `sqlspec[psqlpy]` | Async | `$1` (`dollar`) | Rust Tokio `ConnectionPool` | Row | `LISTEN/NOTIFY` + Table Queue | Sessions + Memory (`PgVector`, `ParadeDB`, `pg_textsearch`, RRF) |
| [`cockroach_asyncpg`](postgres/cockroachdb.md) | `sqlspec[cockroachdb]` | Async | `$1` (`dollar`) | `asyncpg.Pool` | Row | Table Queue | Sessions + Memory (`pgvector`, FTS) |
| [`cockroach_psycopg`](postgres/cockroachdb.md) | `sqlspec[cockroachdb]` | Sync + Async | `%s` / `%(name)s` | `ConnectionPool` / `AsyncConnectionPool` | Row | Table Queue | Sessions + Memory (`pgvector`, FTS) |
| [`sqlite`](sqlite/index.md) | `sqlspec` (stdlib) | Sync | `?` (`qmark`) | `SqliteConnectionPool` | Row / Batched | Table Queue | Sessions + Memory (`FTS5`, `LIKE`) |
| [`aiosqlite`](sqlite/index.md) | `sqlspec[aiosqlite]` | Async | `?` (`qmark`) | `AiosqliteConnectionPool` | Row / Batched | Table Queue | Sessions + Memory (`FTS5`, `LIKE`) |
| [`asyncmy`](mysql/index.md) | `sqlspec[asyncmy]` | Async | `%s` (`format`) | `asyncmy.Pool` | Row / `LOAD DATA` | Table Queue | Sessions + Memory (`FULLTEXT`, `LIKE`) |
| [`aiomysql`](mysql/index.md) | `sqlspec[aiomysql]` | Async | `%s` (`format`) | `aiomysql.Pool` | Row / `LOAD DATA` | Table Queue | Sessions + Memory (`FULLTEXT`, `LIKE`) |
| [`pymysql`](mysql/index.md) | `sqlspec[pymysql]` | Sync | `%s` (`format`) | `PyMySQLConnectionPool` | Row / `LOAD DATA` | Table Queue | Sessions + Memory (`FULLTEXT`, `LIKE`) |
| [`mysqlconnector`](mysql/index.md) | `sqlspec[mysql-connector]` | Sync + Async | `%s` (`format`) | `MySQLConnectionPool` / Async Pool | Row / `LOAD DATA` | Table Queue | Sessions + Memory (`FULLTEXT`, `LIKE`) |
| [`oracledb`](enterprise/oracledb.md) | `sqlspec[oracledb]` | Sync + Async | `:1` / `:name` | `ConnectionPool` / `AsyncConnectionPool` + DRCP | PyCapsule Batches | Oracle AQ (`DBMS_AQ`) + Table Queue | Sessions + Memory (Oracle 26ai `VECTOR` HNSW, Oracle Text, RRF) |
| [`mssql_python`](enterprise/mssql.md) | `sqlspec[mssql-python]` | Sync | `?` (`qmark`) | `MssqlPythonConnectionPool` | Native `BulkCopy` | Table Queue | Sessions + Memory (`LIKE`) |
| [`pymssql`](enterprise/mssql.md) | `sqlspec[pymssql]` | Sync | `%(name)s` / `%s` | `PymssqlConnectionPool` | Row | Table Queue | Sessions + Memory (`LIKE`) |
| [`db2`](enterprise/db2.md) | `sqlspec[db2]` | Sync + Async | `?` (`qmark`) | `Db2SyncConnectionPool` / `Db2AsyncConnectionPool` | Schema-Guided Row | Table Queue (`SKIP LOCKED DATA`) | Sessions + Memory (`POSSTR`) |
| [`duckdb`](cloud-analytics/duckdb.md) | `sqlspec[duckdb]` | Sync | `?` (`qmark`) | `DuckDBConnectionPool` | Native Zero-Copy | Table Queue | Sessions + Memory (`FTS`, `ARRAY` Cosine) |
| [`bigquery`](cloud-analytics/bigquery.md) | `sqlspec[bigquery]` | Sync | `@name` (`named`) | Stateless HTTP/gRPC Client | Storage Read / Write API | Table Queue | Sessions + Memory (`SEARCH` / `LIKE`) |
| [`spanner`](cloud-analytics/spanner.md) | `sqlspec[spanner]` | Sync + Async | `@name` (`named`) | `PingingPool` / `TransactionPingingPool` | Row / Batched | Lock-Free Snapshot + `run_in_transaction` | Sessions + Memory (`ARRAY<FLOAT32>`, FTS `SCORE`, RRF) + Artifacts |
| [`adbc`](cloud-analytics/adbc.md) | `sqlspec[adbc]` | Sync | `?` / `$1` / `:1` | Driver-Managed (`NoPoolSyncConfig`) | Native C-Stream | Table Queue | Sessions + Memory (PostgreSQL `pgvector` RRF, DuckDB, Oracle, Db2) |
| [`arrow_odbc`](cloud-analytics/arrow-odbc.md) | `sqlspec[arrow-odbc]` | Sync | `?` (`qmark`) | ODBC Driver Pool (`enable_driver_pooling`) | Native Rust ODBC Batch | Table Queue | Sessions + Memory (`LIKE` / `POSSTR`) |

## Choosing by Database Family

### PostgreSQL, AlloyDB, and CockroachDB

Start with [PostgreSQL Overview](postgres/index.md) to compare `asyncpg`, `psycopg`, and `psqlpy`, configure PgBouncer `pooler_mode` with `direct_dsn` bypasses, enable Google Cloud AlloyDB columnar acceleration, or connect to [CockroachDB](postgres/cockroachdb.md) with automatic `40001` retry and follower reads.

### Embedded SQLite and Analytics

- [SQLite and Aiosqlite](sqlite/index.md): Zero-dependency local persistence, WAL pragma tuning, shared-memory URIs, and custom SQL functions.
- [DuckDB](cloud-analytics/duckdb.md): In-process OLAP queries over Parquet/S3, hybrid native/Arrow materialization, and JSON statement profiling.
- [Apache Arrow ADBC](cloud-analytics/adbc.md) and [Arrow ODBC](cloud-analytics/arrow-odbc.md): Zero-copy Arrow C Data Interface ingestion and columnar ODBC streaming across warehouses and enterprise servers.

### MySQL and MariaDB

See [MySQL and MariaDB](mysql/index.md) for side-by-side guidance on `asyncmy`, `pymysql`, `aiomysql`, and `mysql-connector-python`, TLS verification, and `LOAD DATA LOCAL INFILE` bulk loading.

### Enterprise Relational Engines

- [Oracle Database](enterprise/oracledb.md): Thin and Thick modes, mTLS/PKCS#12 wallets, TNS topologies, Oracle 26ai `VECTOR` + Hybrid RRF search, PyCapsule Arrow streaming, and `DBMS_CLOUD.COPY_DATA`.
- [Microsoft SQL Server](enterprise/mssql.md): Microsoft's official `mssql-python` driver with streaming Arrow `BulkCopy` alongside FreeTDS `pymssql`.
- [IBM Db2](enterprise/db2.md): Sync and async DRDA pooling (`Db2SyncConfig` / `Db2AsyncConfig`), savepoint cursor retention, and lowercase column normalization.

### Google Cloud Databases

- [Google Cloud Spanner](cloud-analytics/spanner.md): Symmetric sync and native `asyncio` drivers, GoogleSQL (`spanner`) and PostgreSQL (`spangres`) dialects, `run_in_transaction`, and vector/artifact ADK stores.
- [Google Cloud BigQuery](cloud-analytics/bigquery.md): Interactive `query_and_wait` execution, Storage Read/Write APIs, reservation governance, and multi-statement script jobs.
