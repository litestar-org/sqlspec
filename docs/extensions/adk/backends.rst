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
     - Preferred async local backend; single-writer limits still apply.
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

SQLite backends (aiosqlite, sqlite) are ideal for local development, testing,
and single-process deployments:

- JSON1 extension for state and event storage.
- FTS5 virtual tables for memory full-text search.
- File-based or in-memory operation.

.. note::

   SQLite does not support concurrent writers. Use a server-backed database
   for production multi-process deployments.

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
               "session_table": "adk_sessions",
               "events_table": "adk_events",
               "memory_table": "adk_memory_entries",
               "memory_use_fts": True,
               "owner_id_column": "tenant_id INTEGER NOT NULL",
           }
       },
   )

See :doc:`adapters` for adapter-specific configuration patterns.
