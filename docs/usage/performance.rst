==================
Performance Tuning
==================

SQLSpec keeps adapter defaults conservative. Most performance controls are opt-in
because the right value depends on query shape, connection lifetime, network
latency, and the database service in front of the driver.

Start by measuring the query path you want to improve. The optional benchmark
helper documents service-backed scenarios for the cache and fetch controls:

.. code-block:: bash

   uv run python tools/scripts/bench_tuning.py --list

Async bridge thread limits
==========================

``sqlspec.utils.sync_tools.async_()`` keeps its historical default behavior unless
you opt into a shared executor. With no ``executor`` argument and no SQLSpec
default executor configured, SQLSpec delegates to ``asyncio.to_thread()`` and uses
the current event loop's default executor.

Use an explicit ``ThreadPoolExecutor`` when one call site or service already
owns the worker pool:

.. code-block:: python

   from concurrent.futures import ThreadPoolExecutor

   from sqlspec.utils.sync_tools import async_

   executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="app-sql")
   run_blocking_query = async_(blocking_query, executor=executor)

   result = await run_blocking_query()

For process-wide control, set ``SQLSPEC_ASYNC_THREAD_LIMIT`` before SQLSpec first
uses ``async_()``:

.. code-block:: bash

   SQLSPEC_ASYNC_THREAD_LIMIT=8

The environment variable opts ``async_()`` into a SQLSpec-managed
``ThreadPoolExecutor`` shared by every event loop in the process. This is useful
for services that run multiple long-lived loops and want one bounded async bridge
pool instead of one default executor per loop.

Programmatic controls are available when environment configuration is not a good
fit:

.. code-block:: python

   from sqlspec.utils.sync_tools import (
       enable_default_async_thread_pool,
       set_default_async_executor,
       shutdown_default_async_executor,
   )

   enable_default_async_thread_pool(max_workers=8)

   # Or route through an application-owned ThreadPoolExecutor. SQLSpec will not
   # shut this executor down for you.
   set_default_async_executor(app_executor)

   # Optional during application shutdown; SQLSpec also registers an atexit hook.
   shutdown_default_async_executor(wait=False)

Explicit ``executor=`` values must be ``ThreadPoolExecutor`` instances and win
over every configured default. A caller-owned default thread executor set with
``set_default_async_executor()`` wins over the environment-managed pool. SQLSpec
only shuts down pools it creates; caller-owned executors are cleared from SQLSpec
state, not shut down. Both caller-owned and managed defaults are PID-aware, so
forked children never reuse a parent process's worker pool.

All async bridge paths preserve ``contextvars``. ``asyncio.to_thread()`` provides
that behavior on the default path, and SQLSpec copies the caller context before
using ``run_in_executor()`` for explicit or shared executors.

Downstream shim migration
-------------------------

Applications that carried a local ``async_`` shim only to cap worker threads can
usually remove it:

* Replace local shim imports with ``from sqlspec.utils.sync_tools import async_``.
* Set ``SQLSPEC_ASYNC_THREAD_LIMIT`` for process-wide bounded behavior, or call
  ``enable_default_async_thread_pool(max_workers=...)`` during startup.
* If the application already owns a shared ``ThreadPoolExecutor``, pass it with
  ``async_(fn, executor=executor)`` or register it with
  ``set_default_async_executor(executor)``.
* Remove local ``contextvars.copy_context()`` wrapping; SQLSpec preserves caller
  context on every offload path.

Cache and fetch controls
========================

.. list-table::
   :header-rows: 1
   :widths: 18 24 20 24 24

   * - Adapter
     - Knob
     - Classification
     - Use when
     - Avoid when
   * - All drivers
     - ``driver_features={"sqlspec_statement_cache_size": N}``
     - SQLSpec statement cache
     - The same raw SQL text runs repeatedly with simple parameters and no per-call transformers.
     - SQL text is high-cardinality, DDL changes affect cached result shapes, or you need to isolate query preparation behavior. Set ``0`` to disable.
   * - ``asyncpg``
     - ``connection_config={"statement_cache_size": N}``
     - Native asyncpg prepared-statement cache
     - A long-lived connection repeats parameterized statements and talks directly to PostgreSQL.
     - PgBouncer uses transaction or statement pooling, or schema changes happen inside transactions on the same connection. Use ``0`` to disable.
   * - ``psycopg``
     - ``connection_config={"prepare_threshold": N}``
     - Native psycopg server-side prepare threshold
     - A query repeats enough times on the same pooled connection to amortize server-side planning.
     - Queries are rarely repeated, connection middleware cannot keep prepared statements session-local, or schema churn is expected. Use ``None`` to disable.
   * - ``oracledb``
     - ``connection_config={"stmtcachesize": N}``
     - python-oracledb statement cache
     - The same statement text is executed frequently on pooled Oracle sessions.
     - Statement text has high cardinality or memory pressure matters more than parse reuse.
   * - ``oracledb``
     - ``driver_features={"arraysize": N, "prefetchrows": N}``
     - Cursor fetch buffering
     - Large result sets spend more time on network round trips than row materialization.
     - Single-row lookups dominate, rows are very wide, or larger buffers increase memory pressure.
   * - ``oracledb``
     - ``driver_features={"fetch_lobs": False, "fetch_decimals": True}``
     - Per-statement fetch representation
     - You want python-oracledb's native LOB or NUMBER fetch mode for result conversion.
     - Application code depends on the default LOB locator or numeric representation.
   * - ``bigquery``
     - ``driver_features={"query_page_size": N, "query_max_results": N}``
     - Query result paging
     - SELECT result consumption should bound per-page fetch size or cap rows returned by ``QueryJob.result()``.
     - Running DML or scripts. These controls only apply to SELECT-style result fetching and native table Arrow export completion.
   * - ``arrow_odbc``
     - ``driver_features={"chunk_size": N, "max_bytes_per_batch": N}``
     - Native Arrow batch sizing
     - ODBC Arrow reads should trade memory for fewer batches or smaller batches for steadier memory.
     - Downstream processing expects a particular batch size, or the driver/database already determines a better batch shape.
   * - ``arrow_odbc``
     - ``driver_features={"max_text_size": N, "max_binary_size": N, "fetch_concurrently": bool}``
     - ODBC Arrow fetch behavior
     - Text or binary columns need explicit bounds, or concurrent fetch improves a high-latency ODBC source.
     - Column sizes are unknown and truncation is unacceptable, or the ODBC source is unstable under concurrent fetch.

Avoid-when guidance
===================

Prepared-statement and statement-cache controls help only when statement text is
reused on the same connection. They can be counterproductive when SQL is generated
with many literal variations, when schema changes run on long-lived sessions, or
when a proxy changes the server session behind a client connection.

Fetch-size controls help only after the query is already correct and indexed. They
do not fix slow plans. Use them to tune memory and network behavior for known
result shapes, then verify with representative row widths.

BigQuery result paging controls are SELECT-result controls. They are intentionally
not applied to DML or script completion calls, so minimal BigQuery configs keep the
client's default result behavior.

``arrow_odbc`` divergence
=========================

``arrow_odbc`` does not follow DB-API cursor buffering patterns. Its fetch controls
are passed to the driver's Arrow batch reader, where ``chunk_size`` and
``max_bytes_per_batch`` shape Arrow batches directly. This differs from Oracle
``arraysize`` and ``prefetchrows``, which tune cursor buffering before SQLSpec
materializes rows.

For ``arrow_odbc``, benchmark both memory and wall time. A larger batch can reduce
driver round trips but increase peak memory, while smaller batches can make
streaming steadier for downstream Arrow consumers.
