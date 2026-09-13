========
Adapters
========

SQLSpec ships adapter packages for each supported database or driver. Each adapter
exports a typed config class and a driver implementation.

.. grid:: 3

   .. grid-item-card:: AsyncPG
      :link: asyncpg
      :link-type: doc

      Async PostgreSQL via asyncpg.

   .. grid-item-card:: Psycopg
      :link: psycopg
      :link-type: doc

      Sync + Async PostgreSQL via psycopg.

   .. grid-item-card:: PsqlPy
      :link: psqlpy
      :link-type: doc

      Async PostgreSQL via psqlpy (Rust).

   .. grid-item-card:: SQLite
      :link: sqlite
      :link-type: doc

      Sync SQLite via stdlib sqlite3.

   .. grid-item-card:: aiosqlite
      :link: aiosqlite
      :link-type: doc

      Async SQLite via aiosqlite.

   .. grid-item-card:: DuckDB
      :link: duckdb
      :link-type: doc

      Sync DuckDB with Arrow support.

   .. grid-item-card:: Oracle
      :link: oracledb
      :link-type: doc

      Sync + Async Oracle via oracledb.

   .. grid-item-card:: mysql-connector-python
      :link: mysqlconnector
      :link-type: doc

      Sync + Async MySQL via mysql-connector-python.

   .. grid-item-card:: PyMySQL
      :link: pymysql
      :link-type: doc

      Sync MySQL via PyMySQL.

   .. grid-item-card:: asyncmy
      :link: asyncmy
      :link-type: doc

      Async MySQL via asyncmy.

   .. grid-item-card:: aiomysql
      :link: aiomysql
      :link-type: doc

      Async MySQL via aiomysql.

   .. grid-item-card:: BigQuery
      :link: bigquery
      :link-type: doc

      Google BigQuery.

   .. grid-item-card:: Spanner
      :link: spanner
      :link-type: doc

      Google Cloud Spanner.

   .. grid-item-card:: CockroachDB + AsyncPG
      :link: cockroach_asyncpg
      :link-type: doc

      Async CockroachDB via asyncpg.

   .. grid-item-card:: CockroachDB + Psycopg
      :link: cockroach_psycopg
      :link-type: doc

      Sync + Async CockroachDB via psycopg.

   .. grid-item-card:: ADBC
      :link: adbc
      :link-type: doc

      Arrow Database Connectivity.

   .. grid-item-card:: arrow-odbc
      :link: arrow_odbc
      :link-type: doc

      Sync Arrow-over-ODBC for any ODBC-compliant database.

   .. grid-item-card:: mssql-python
      :link: mssql_python
      :link-type: doc

      Sync SQL Server via Microsoft's official mssql-python driver with Arrow loading and BulkCopy.

   .. grid-item-card:: pymssql
      :link: pymssql
      :link-type: doc

      Sync SQL Server via pymssql / FreeTDS.

Feature Comparison
==================

Read ``config.supports_reliable_rowcount`` from the configuration used to create
the session when deciding whether to verify a modifying statement with a follow-up
query. The flag describes affected-row reporting for individual DML statements;
SELECT, scripts, and batch operations retain their adapter-specific count semantics.

.. list-table::
   :header-rows: 1

   * - Adapter
     - Sync
     - Async
     - Connection Pool
     - Arrow Support
     - Native Pipelines
     - Reliable rowcount
     - Datetime Binding
     - Timestamp Precision
     - JSON Decoded
     - UUID Binding
   * - asyncpg
     -
     - Yes
     - Yes
     - Yes
     - Yes
     - Yes
     - Native
     - Microsecond
     - Yes
     - Native
   * - psycopg
     - Yes
     - Yes
     - Yes
     - Yes
     - Yes
     - Yes
     - Native
     - Microsecond
     - Yes
     - Native
   * - psqlpy
     -
     - Yes
     - Yes
     - Yes
     -
     - Yes
     - Native
     - Microsecond
     -
     - Native
   * - sqlite
     - Yes
     -
     - Yes
     -
     -
     - Yes
     - ISO Text
     - Microsecond
     -
     - Text
   * - aiosqlite
     -
     - Yes
     - Yes
     -
     -
     - Yes
     - ISO Text
     - Microsecond
     -
     - Text
   * - duckdb
     - Yes
     -
     - Yes
     - Yes
     -
     - Yes
     - ISO Text
     - Microsecond
     -
     - Text
   * - oracledb
     - Yes
     - Yes
     - Yes
     -
     - Yes
     - Yes
     - Native
     - Microsecond
     - Yes
     - Native
   * - mysql-connector
     - Yes
     - Yes
     - Yes
     -
     -
     - Yes
     - Native
     - Microsecond
     -
     - Text
   * - pymysql
     - Yes
     -
     - Yes
     -
     -
     - Yes
     - Native
     - Microsecond
     -
     - Text
   * - asyncmy
     -
     - Yes
     - Yes
     -
     -
     - Yes
     - Native
     - Microsecond
     -
     - Text
   * - aiomysql
     -
     - Yes
     - Yes
     -
     -
     - Yes
     - Native
     - Microsecond
     -
     - Text
   * - bigquery
     - Yes
     -
     -
     - Yes
     -
     - Yes
     - Native
     - Microsecond
     -
     - Text
   * - spanner
     - Yes
     -
     - Yes
     -
     -
     - Yes
     - Native
     - Microsecond
     - Yes
     - Text
   * - cockroach (asyncpg)
     -
     - Yes
     - Yes
     - Yes
     - Yes
     - Yes
     - Native
     - Microsecond
     - Yes
     - Native
   * - cockroach (psycopg)
     - Yes
     - Yes
     - Yes
     - Yes
     - Yes
     - Yes
     - Native
     - Microsecond
     - Yes
     - Native
   * - adbc
     - Yes
     -
     -
     - Yes
     -
     - No
     - Native
     - Microsecond
     -
     - Native
   * - arrow_odbc
     - Yes
     -
     -
     - Yes
     -
     - No
     - Native
     - Millisecond
     -
     - Text
   * - mssql_python
     - Yes
     -
     - Yes
     - Yes
     -
     - Yes
     - Native
     - Microsecond
     -
     - Native
   * - pymssql
     - Yes
     -
     - Yes
     -
     -
     - Yes
     - Native
     - Microsecond
     -
     - Text

.. toctree::
   :hidden:

   asyncpg
   psycopg
   psqlpy
   sqlite
   aiosqlite
   duckdb
   oracledb
   mysqlconnector
   pymysql
   asyncmy
   aiomysql
   bigquery
   spanner
   cockroach_asyncpg
   cockroach_psycopg
   adbc
   arrow_odbc
   mssql_python
   pymssql
