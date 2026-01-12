========
Backends
========

ADK stores are implemented per adapter. Use the backend config helpers when
connecting to multiple databases or configuring advanced options.

Example
=======

.. literalinclude:: /examples/extensions/adk/backend_config.py
   :language: python
   :caption: ``adk backend config``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Supported Backends
==================

.. list-table::
   :header-rows: 1

   * - Adapter
     - Status
   * - asyncpg
     - Production
   * - psycopg
     - Production
   * - psqlpy
     - Production
   * - asyncmy
     - Production
   * - sqlite
     - Production
   * - aiosqlite
     - Production
   * - oracledb
     - Production
   * - duckdb
     - Production (analytics)
   * - bigquery
     - Production
   * - adbc
     - Production

Notes
=====

- Use async backends for ADK runners; sync backends can be wrapped with anyio.
- Backend stores expose ``create_tables`` to bootstrap schema.
