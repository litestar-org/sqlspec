========
Adapters
========

SQLSpec ships adapter packages for each supported database or driver. Each adapter
exports a typed config class and a driver implementation.

Quick Example
=============

.. literalinclude:: /examples/drivers/sqlite_connection.py
   :language: python
   :caption: ``sqlite config``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Adapter Catalog
===============

.. list-table::
   :header-rows: 1

   * - Adapter
     - Config Class
     - Sync/Async
   * - asyncpg
     - ``AsyncpgConfig``
     - Async
   * - psycopg
     - ``PsycopgSyncConfig`` / ``PsycopgAsyncConfig``
     - Sync + Async
   * - psqlpy
     - ``PsqlpyConfig``
     - Async
   * - cockroach (asyncpg)
     - ``CockroachAsyncpgConfig``
     - Async
   * - cockroach (psycopg)
     - ``CockroachPsycopgSyncConfig`` / ``CockroachPsycopgAsyncConfig``
     - Sync + Async
   * - sqlite
     - ``SqliteConfig``
     - Sync
   * - aiosqlite
     - ``AiosqliteConfig``
     - Async
   * - duckdb
     - ``DuckDBConfig``
     - Sync
   * - mysql-connector
     - ``MysqlConnectorSyncConfig`` / ``MysqlConnectorAsyncConfig``
     - Sync + Async
   * - pymysql
     - ``PyMysqlConfig``
     - Sync
   * - asyncmy
     - ``AsyncmyConfig``
     - Async
   * - oracledb
     - ``OracleSyncConfig`` / ``OracleAsyncConfig``
     - Sync + Async
   * - bigquery
     - ``BigQueryConfig``
     - Sync
   * - spanner
     - ``SpannerSyncConfig``
     - Sync
   * - adbc
     - ``AdbcConfig``
     - Sync

Find each config in ``sqlspec.adapters.<adapter>`` (for example,
``from sqlspec.adapters.asyncpg import AsyncpgConfig``).

Example Library
===============

- :doc:`/usage/drivers_and_querying` for per-adapter configuration examples.
- :doc:`/usage/drivers_and_querying` for execution patterns.
