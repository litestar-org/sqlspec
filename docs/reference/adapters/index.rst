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

   .. grid-item-card:: MySQL
      :link: mysql
      :link-type: doc

      MySQL via mysql-connector, PyMySQL, asyncmy, and aiomysql.

   .. grid-item-card:: BigQuery
      :link: bigquery
      :link-type: doc

      Google BigQuery.

   .. grid-item-card:: Spanner
      :link: spanner
      :link-type: doc

      Google Cloud Spanner.

   .. grid-item-card:: CockroachDB
      :link: cockroach
      :link-type: doc

      CockroachDB via asyncpg or psycopg.

   .. grid-item-card:: ADBC
      :link: adbc
      :link-type: doc

      Arrow Database Connectivity.

Feature Comparison
==================

.. list-table::
   :header-rows: 1

   * - Adapter
     - Sync
     - Async
     - Connection Pool
     - Arrow Support
     - Native Pipelines
   * - asyncpg
     -
     - Yes
     - Yes
     - Yes
     - Yes
   * - psycopg
     - Yes
     - Yes
     - Yes
     - Yes
     - Yes
   * - psqlpy
     -
     - Yes
     - Yes
     - Yes
     -
   * - sqlite
     - Yes
     -
     - Yes
     -
     -
   * - aiosqlite
     -
     - Yes
     - Yes
     -
     -
   * - duckdb
     - Yes
     -
     - Yes
     - Yes
     -
   * - oracledb
     - Yes
     - Yes
     - Yes
     -
     - Yes
   * - mysql-connector
     - Yes
     - Yes
     - Yes
     -
     -
   * - pymysql
     - Yes
     -
     - Yes
     -
     -
   * - asyncmy
     -
     - Yes
     - Yes
     -
     -
   * - aiomysql
     -
     - Yes
     - Yes
     -
     -
   * - bigquery
     - Yes
     -
     -
     - Yes
     -
   * - spanner
     - Yes
     -
     - Yes
     -
     -
   * - cockroach (asyncpg)
     -
     - Yes
     - Yes
     - Yes
     - Yes
   * - cockroach (psycopg)
     - Yes
     - Yes
     - Yes
     - Yes
     - Yes
   * - adbc
     - Yes
     -
     -
     - Yes
     -

.. toctree::
   :hidden:

   asyncpg
   psycopg
   psqlpy
   sqlite
   aiosqlite
   duckdb
   oracledb
   mysql
   bigquery
   spanner
   cockroach
   adbc
