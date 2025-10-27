=======================
SQLSpec Example Library
=======================

The example catalog now mirrors the way teams explore SQLSpec. Each snippet focuses on a single idea, keeps inline commentary to a minimum, and favors lightweight backends (SQLite, AioSQLite, DuckDB) so the code can run anywhere.

Quick Start
===========

Run a smoke sweep that exercises the SQLite/AioSQLite/DuckDB demos:

.. code-block:: console

   make examples-smoke

Each file exposes a ``main()`` helper so you can execute it directly:

.. code-block:: console

   uv run python docs/examples/frameworks/litestar/aiosqlite_app.py

Folder Guide
============

Frameworks
----------

.. list-table:: Litestar demos
   :header-rows: 1

   * - File
     - Description
   * - ``frameworks/litestar/aiosqlite_app.py``
     - Async Litestar app backed by SQLSpec and AioSQLite with automatic seeding.
   * - ``frameworks/litestar/duckdb_app.py``
     - Sync Litestar handlers using DuckDB for quick analytics dashboards.
   * - ``frameworks/litestar/sqlite_app.py``
      - Litestar routes backed by the synchronous SQLite adapter.

.. list-table:: FastAPI / Starlette / Flask demos
   :header-rows: 1

   * - File
     - Description
   * - ``frameworks/fastapi/aiosqlite_app.py``
     - FastAPI app using the AioSQLite adapter with dependency-injected sessions.
   * - ``frameworks/fastapi/sqlite_app.py``
     - Synchronous FastAPI handlers powered by the SQLite adapter.
   * - ``frameworks/starlette/aiosqlite_app.py``
     - Starlette routes that read from an AioSQLite-backed dataset.
   * - ``frameworks/flask/sqlite_app.py``
     - Flask blueprint that serves data via the synchronous SQLite adapter.

Adapters
--------

.. list-table:: Connection-focused snippets
   :header-rows: 1

   * - File
     - Adapter
     - Highlights
   * - ``adapters/asyncpg/connect_pool.py``
     - AsyncPG
     - Minimal pool configuration plus a version probe.
   * - ``adapters/psycopg/connect_sync.py``
     - Psycopg (sync)
     - Blocking workflow familiar to scripts and management commands.
   * - ``adapters/oracledb/connect_async.py``
     - oracledb (async)
     - Async driver setup with timestamp sampling.

Patterns
--------

.. list-table:: Common tasks
   :header-rows: 1

   * - File
     - Scenario
   * - ``patterns/builder/select_and_insert.py``
     - Fluent SQL builder usage with a tiny articles dataset.
   * - ``patterns/migrations/runner_basic.py``
     - Sync migration commands pointed at bundled demo migrations.
   * - ``patterns/migrations/files/0001_create_articles.py``
     - Python migration file consumed by the runner example.
   * - ``patterns/multi_tenant/router.py``
     - Routing requests to dedicated SQLite configs per tenant slug.
   * - ``patterns/configs/multi_adapter_registry.py``
     - Register multiple adapters on a single SQLSpec registry.

Loaders
-------

.. list-table:: SQL file loading
   :header-rows: 1

   * - File
     - Description
   * - ``loaders/sql_files.py``
     - Shows how ``SQLFileLoader`` binds named queries in ``queries/users.sql`` and executes them with SQLite.

Extensions
----------

.. list-table:: Adapter Development Kit
   :header-rows: 1

   * - File
     - Description
   * - ``extensions/adk/basic_aiosqlite.py``
     - Create an ADK session, append events, and fetch the transcript using SQLSpec’s AioSQLite store.
   * - ``extensions/adk/litestar_aiosqlite.py``
     - Wire ``SQLSpecSessionService`` into Litestar and expose a simple ``/sessions`` endpoint.

Shared Utilities
----------------

``shared/configs.py`` and ``shared/data.py`` provide registry builders and seed data so the individual examples can stay short and consistent.

.. toctree::
   :hidden:

   frameworks/litestar/aiosqlite_app
   frameworks/litestar/duckdb_app
   frameworks/litestar/sqlite_app
   adapters/asyncpg/connect_pool
   adapters/psycopg/connect_sync
   adapters/oracledb/connect_async
   patterns/builder/select_and_insert
   patterns/migrations/runner_basic
   patterns/multi_tenant/router
   loaders/sql_files
   extensions/adk/basic_aiosqlite
   extensions/adk/litestar_aiosqlite
   frameworks/fastapi/aiosqlite_app
   frameworks/fastapi/sqlite_app
   frameworks/starlette/aiosqlite_app
   frameworks/flask/sqlite_app
   patterns/configs/multi_adapter_registry
   README
