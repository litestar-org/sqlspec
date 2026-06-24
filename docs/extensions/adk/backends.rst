========
Backends
========

ADK session/event and memory stores are implemented per adapter. Each backend
has different capabilities for transactional state, event fidelity, and memory
search. Use the support matrix below to select the right backend for your
deployment.

.. _adk-support-matrix:

Support Matrix
==============

The table below classifies every backend by its ADK support level.

.. list-table::
   :header-rows: 1
   :widths: 20 15 15 15 35

   * - Adapter
     - Status
     - Session/Event
     - Memory (FTS)
     - Notes
   * - asyncpg
     - Recommended
     - Full
     - Full
     - Best async PostgreSQL driver.
   * - psycopg (async)
     - Recommended
     - Full
     - Full
     - Supports both sync and async modes.
   * - psqlpy
     - Supported
     - Full
     - Full
     - Rust-backed PostgreSQL driver.
   * - cockroach_asyncpg
     - Supported
     - Full
     - Full
     - CockroachDB with full FTS support.
   * - cockroach_psycopg
     - Supported
     - Full
     - Full
     - CockroachDB with full FTS support.
   * - aiomysql
     - Supported
     - Full
     - Full
     - MySQL/MariaDB async driver.
   * - asyncmy
     - Supported
     - Full
     - Full
     - MySQL/MariaDB async driver.
   * - mysqlconnector
     - Supported
     - Full
     - Full
     - MySQL/MariaDB sync driver.
   * - pymysql
     - Supported
     - Full
     - Full
     - MySQL/MariaDB sync driver.
   * - aiosqlite
     - Supported
     - Full
     - Full
     - Async local backend; single-writer limits still apply.
   * - sqlite
     - Supported
     - Full
     - Full
     - SQLite sync with thread-local pools.
   * - oracledb
     - Supported
     - Full
     - Full
     - Oracle Database driver.
   * - duckdb
     - Reduced-scope
     - Full
     - Limited
     - Analytics-oriented; no concurrent writes.
   * - adbc
     - Reduced-scope
     - Full
     - Basic
     - Portability layer; native adapters provide optimized search.
   * - spanner
     - Supported
     - Full
     - Full
     - Google Cloud Spanner (cloud-managed).

Status Definitions
------------------

**Recommended**
   Production-grade, fully tested, actively optimized. Start here unless you
   have a specific reason not to.

**Supported**
   Fully implemented and tested for session/event and memory operations.

**Reduced-scope**
   Implemented with known limitations. Specific features may be absent or
   behave differently. See backend-specific notes.

Current scoped-state boundary
   The shared session service strips ``temp:`` keys before persistence.
   ``app:`` keys are persisted in the app-scoped state table and ``user:`` keys
   are persisted in the user-scoped state table. Session-local keys remain in
   the session row. Loaded sessions merge those scopes back into the ADK state
   view.

**Removed**
   Previously available but no longer supported. See the removal notice for
   migration guidance.

Removed Backends
----------------

**BigQuery** was removed from the ADK backend surface. BigQuery's batch-oriented
architecture is incompatible with the low-latency, transactional write patterns
that ADK session and event storage require. If you were using BigQuery for ADK
storage, migrate to Spanner for a Google-managed operational backend, or to a
transactional OLTP backend such as PostgreSQL, MySQL, Oracle, SQLite, or
CockroachDB.

Artifact Storage
----------------

The ADK artifact service API and base metadata-store contracts are available in
``sqlspec.extensions.adk.artifact``. Adapter-specific concrete artifact
metadata stores are not part of this support matrix; session/event and memory
support are the backend guarantees listed above.

Backend Details
===============

PostgreSQL Family
-----------------

PostgreSQL backends (asyncpg, psycopg, psqlpy) provide the fullest feature set:

- Native ``JSONB`` storage for session state and event JSON.
- Full-text search via ``tsvector`` for memory entries.
- ``UPSERT`` and ``RETURNING`` clauses for atomic operations.
- ``append_event_and_update_state()`` executes as a single transaction.

**Recommended for production deployments.**

CockroachDB
------------

CockroachDB backends (cockroach_asyncpg, cockroach_psycopg) provide full ADK
support including full-text search. CockroachDB is a distributed SQL database
compatible with the PostgreSQL wire protocol.

- Full FTS support for memory search.
- Distributed transactions for session and event atomicity.
- Horizontal scalability for high-throughput agent deployments.

MySQL Family
------------

MySQL backends (aiomysql, asyncmy, mysqlconnector, pymysql) provide full ADK
support:

- JSON column storage for session state and event records.
- Full-text search on ``InnoDB`` tables for memory entries.
- Transactional writes for ``append_event_and_update_state()``.

SQLite
------

SQLite backends (``aiosqlite``, ``sqlite``) are the recommended local-file
ADK backends for development, tests, and single-process deployments. Both
adapters share the same DDL and store contract; they differ only in their
connection model.

Supported behavior (both adapters):

- JSON1 extension for state and event storage.
- FTS5 virtual tables for memory full-text search, with optional
  ``tokenize='porter'`` stemming and ``detail='none'`` index reduction
  controlled by the ADK extension config.
- File-based or in-memory operation.
- Empty session state is persisted as JSON ``{}``, not ``NULL``. The store
  never serializes the empty-state value as ``None``.
- ``append_event_and_update_state()`` writes the event row and the durable
  state snapshot in a single transaction and returns the updated session
  record without an extra ``get_session()`` round trip.
- WAL journaling and ``synchronous=NORMAL`` for file-based databases, with
  configurable ``busy_timeout``, ``cache_size``, ``mmap_size``, and
  ``journal_size_limit`` PRAGMAs for local tuning.

``sqlite`` vs ``aiosqlite`` boundary:

- ``sqlite`` (``sqlspec.adapters.sqlite``) is the synchronous adapter. ADK
  uses a thread-local connection pool and offloads each store call to a
  worker thread. Best suited for scripts, CLI utilities, test fixtures, and
  small embedded agents where async overhead is undesirable.
- ``aiosqlite`` (``sqlspec.adapters.aiosqlite``) is the native async adapter.
  ADK calls run directly on the event loop without thread offload. Best
  suited for single-process async ADK runners that already operate inside
  an async framework (Litestar, FastAPI, Starlette, etc.).

Both adapters use identical table schemas, so a SQLite database created by
one adapter can be opened and read by the other without migration.

Unsupported lifecycle features:

- No native server-side partitioning, hash sharding, or table compression.
  The capability profile reports these as unsupported. If you need them,
  choose a server-backed adapter (PostgreSQL, MySQL, Oracle, Spanner).
- No concurrent writers. SQLite serializes writes at the file level; use a
  server-backed database for production multi-process or multi-writer
  deployments.

.. note::

   The shared scoped-state contract for ``app:`` and ``user:`` keys is
   defined under the :ref:`adk-support-matrix` above. SQLite is not uniquely
   partial here — it follows the same shared behavior as every other ADK
   session store.

Oracle
------

Oracle Database (oracledb) provides full ADK support:

- Version-aware JSON storage: native ``JSON`` on Oracle 21c+, JSON-checked
  ``BLOB`` on older supported versions, and fallback ``BLOB`` storage where
  native validation is unavailable.
- Oracle Text ``CTXSYS.CONTEXT`` indexes with ``CONTAINS()`` and ``SCORE()``
  ranking for memory entries.
- Optional ``in_memory`` table creation for deployments licensed and configured
  for Oracle Database In-Memory.
- Configurable table compression and hash/range partitioning for session,
  event, and memory tables.
- Full transactional support for atomic operations.

DuckDB
------

DuckDB provides session and event storage but has limitations:

- Optimized for analytics, not OLTP workloads.
- Single-writer constraint limits concurrent access.
- Memory search capabilities are limited compared to server databases.

**Best suited for analytics pipelines and offline agent evaluation.**

ADBC
----

ADBC (Arrow Database Connectivity) provides a driver-agnostic interface:

- Works with any ADBC-compatible driver (PostgreSQL, SQLite, DuckDB, etc.).
- Arrow-native data transfer for high-throughput event ingestion.
- Backend capabilities depend on the underlying database driver.
- Memory search uses the portable baseline path; choose a native adapter for
  backend-specific FTS, retention, and storage tuning.

Spanner
-------

Google Cloud Spanner provides globally distributed ADK storage:

- Cloud-managed, horizontally scalable.
- Optional hash sharding via ``shard_count`` to reduce hot spots.
- Full-text search support for memory entries with ``TOKENIZE_FULLTEXT`` and
  search indexes.
- Explicit table/index option passthrough for deployments that need
  Spanner-specific DDL tuning.
- Native row-deletion TTL policies generated from ADK retention settings.
- Strong consistency across regions.
- Suitable for multi-region agent deployments.

Configuration
=============

All backends are configured through ``extension_config["adk"]``:

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig

   config = AsyncpgConfig(
       connection_config={"dsn": "postgresql://localhost/mydb"},
       extension_config={
           "adk": {
               "session_table": "adk_session",
               "events_table": "adk_event",
               "memory_table": "adk_memory",
               "memory_use_fts": True,
               "owner_id_column": "tenant_id INTEGER NOT NULL",
           }
       },
   )

See :doc:`adapters` for adapter-specific configuration patterns.
