========
Backends
========

ADK stores are implemented per adapter. Each backend has different capabilities
for session, event, memory, and artifact storage. Use the support matrix below
to select the right backend for your deployment.

.. _adk-support-matrix:

Support Matrix
==============

The table below classifies every backend by its ADK support level.

.. list-table::
   :header-rows: 1
   :widths: 20 15 15 15 15 20

   * - Adapter
     - Status
     - Session/Event
     - Memory (FTS)
     - Artifacts
     - Notes
   * - asyncpg
     - Recommended
     - Full
     - Full
     - Full
     - Best async PostgreSQL driver.
   * - psycopg (async)
     - Recommended
     - Full
     - Full
     - Full
     - Supports both sync and async modes.
   * - psqlpy
     - Supported
     - Full
     - Full
     - Full
     - Rust-backed PostgreSQL driver.
   * - cockroach_asyncpg
     - Supported
     - Full
     - Full
     - Full
     - CockroachDB with full FTS support.
   * - cockroach_psycopg
     - Supported
     - Full
     - Full
     - Full
     - CockroachDB with full FTS support.
   * - asyncmy
     - Supported
     - Full
     - Full
     - Full
     - MySQL/MariaDB async driver.
   * - mysqlconnector
     - Supported
     - Full
     - Full
     - Full
     - MySQL/MariaDB sync driver.
   * - pymysql
     - Supported
     - Full
     - Full
     - Full
     - MySQL/MariaDB sync driver.
   * - aiosqlite
     - Supported
     - Full
     - Full
     - Full
     - SQLite async, ideal for development.
   * - sqlite
     - Supported
     - Full
     - Full
     - Full
     - SQLite sync with thread-local pools.
   * - oracledb
     - Supported
     - Full
     - Full
     - Full
     - Oracle Database driver.
   * - duckdb
     - Reduced-scope
     - Full
     - Limited
     - Full
     - Analytics-oriented; no concurrent writes.
   * - adbc
     - Supported
     - Full
     - Full
     - Full
     - Arrow-native database connectivity.
   * - spanner
     - Supported
     - Full
     - Full
     - Full
     - Google Cloud Spanner (cloud-managed).

Status Definitions
------------------

**Recommended**
   Production-grade, fully tested, actively optimized. Start here unless you
   have a specific reason not to.

**Supported**
   Fully implemented and tested. Works correctly for all ADK operations.

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
storage, migrate to PostgreSQL (asyncpg or psycopg) or any other supported
backend.

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

MySQL backends (asyncmy, mysqlconnector, pymysql) provide full ADK support:

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

- Native JSON column support (Oracle 21c+).
- Oracle Text for full-text search on memory entries.
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

Spanner
-------

Google Cloud Spanner provides globally distributed ADK storage:

- Cloud-managed, horizontally scalable.
- Full-text search support for memory entries.
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
               "artifact_table": "adk_artifact_versions",
               "owner_id_column": "tenant_id INTEGER NOT NULL",
           }
       },
   )

See :doc:`adapters` for adapter-specific configuration patterns.
