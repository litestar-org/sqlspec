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

Cross-adapter benchmark snapshots
==================================

The main benchmark runner has extended scenarios for the optimized adapter
paths. SQLite remains the default local gate; adapter-specific runs can compare
against the committed JSON snapshots in ``tools/perf_baselines/``.

.. code-block:: bash

   make bench-gate
   make bench-gate-adapters DRIVERS="duckdb aiosqlite"

Service-backed runs use the same command after exporting connection values for
the service under test. These cross-adapter comparisons are opt-in development
and management checks, not an enabled CI gate: host, network, and service
conditions can produce materially different timings. The scenario reads these
variables and skips cleanly when the optional service is not configured; the
comparison command then fails if a required baseline scenario is absent.

The headline scenarios are ``spanner/strings``,
``mysqlconnector/json_rows``, ``adbc/rows``, ``duckdb/bulk``, and
``aiosqlite/worker_hops``. Oracle LOB scenarios remain under the Oracle-specific
benchmark configuration.

``bench_compare.py`` uses the stable ``library_key`` in result JSON, so a
baseline captured from interpreted SQLSpec can be compared with a MyPyC run.
The checked-in baseline snapshots themselves are interpreted/uncompiled only
(``metadata.mypyc_compiled`` is ``false``); they are not compiled-performance
references.

Async bridge executor limits
============================

``sqlspec.utils.sync_tools.async_()`` wraps blocking callables for async code.
By default SQLSpec routes this work through a process-local managed
``ThreadPoolExecutor`` capped at eight workers. This keeps sync work called from
async contexts, such as framework stores backed by sync drivers, from creating
one unbounded event-loop executor per long-lived loop.

Pass an explicit ``ThreadPoolExecutor`` when one call site owns the worker pool:

.. code-block:: python

   from concurrent.futures import ThreadPoolExecutor

   from sqlspec.utils.sync_tools import async_

   executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="sqlspec")
   run_query = async_(blocking_query, executor=executor)

   result = await run_query()

Set ``SQLSPEC_ASYNC_THREAD_LIMIT`` before the first ``async_()`` call to change
the worker limit for SQLSpec's managed pool:

.. code-block:: bash

   export SQLSPEC_ASYNC_THREAD_LIMIT=8

Use the programmatic API when application startup centralizes runtime settings:

.. code-block:: python

   from concurrent.futures import ThreadPoolExecutor

   from sqlspec.utils.sync_tools import (
       enable_default_async_thread_pool,
       get_default_async_executor,
       set_default_async_executor,
       shutdown_default_async_executor,
   )

   enable_default_async_thread_pool(max_workers=8)

   app_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="sqlspec")
   set_default_async_executor(app_executor)

   assert get_default_async_executor() is app_executor
   shutdown_default_async_executor(wait=False)

Executor precedence is explicit: ``async_(fn, executor=...)`` wins over
``set_default_async_executor()``, which wins over SQLSpec's managed pool. The
managed pool uses ``SQLSPEC_ASYNC_THREAD_LIMIT`` when set and otherwise falls
back to ``DEFAULT_ASYNC_THREAD_LIMIT``. SQLSpec only shuts down executors it
creates; caller-owned executors are removed from SQLSpec state but not shut
down. Managed and caller-owned defaults are PID-aware, so forked children do not
reuse a parent process's pool. Calling ``shutdown_default_async_executor()``
tears down the current managed pool and clears caller-owned defaults; the next
``async_()`` call creates a new managed pool from the current configuration.

Only ``ThreadPoolExecutor`` instances are accepted. Process executors are
rejected because SQLSpec preserves ``contextvars`` for every async bridge path,
including explicit and shared executor calls.

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
