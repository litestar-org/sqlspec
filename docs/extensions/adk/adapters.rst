==================
Database Adapters
==================

The ADK extension provides database-specific store implementations for each supported SQLSpec adapter.
Each store is optimized for its database's native features and parameter style.

Overview
========

All adapters implement either :class:`~sqlspec.extensions.adk.store.BaseAsyncADKStore` (async) or
:class:`~sqlspec.extensions.adk.store.BaseSyncADKStore` (sync), providing a consistent API across
databases while leveraging database-specific optimizations.

**Common Features:**

- Session and event CRUD operations
- JSON storage for session state and event metadata
- Indexed queries for performance
- Foreign key constraints with cascade delete
- Customizable table names

PostgreSQL Adapters
===================

PostgreSQL is the recommended production database for AI agents. All PostgreSQL drivers share the same
SQL dialect and parameter style (``$1``, ``$2``, etc.).

AsyncPG (Recommended)
---------------------

**Import:**

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore

.. seealso::

   :doc:`/examples/adk_basic_asyncpg`
      Complete runnable example using AsyncPG with PostgreSQL

**Features:**

- Fast, async-native PostgreSQL driver
- Built-in connection pooling
- JSONB for efficient state storage
- BYTEA for pickled actions
- Microsecond-precision TIMESTAMPTZ
- GIN indexes for JSONB queries
- HOT updates with FILLFACTOR 80

**Configuration:**

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore

   config = AsyncpgConfig(pool_config={
       "dsn": "postgresql://user:pass@localhost:5432/agentdb",
       "min_size": 10,
       "max_size": 20,
       "command_timeout": 60.0
   })

   store = AsyncpgADKStore(config)
   await store.create_tables()

**Schema DDL:**

.. code-block:: sql

   CREATE TABLE IF NOT EXISTS adk_sessions (
       id VARCHAR(128) PRIMARY KEY,
       app_name VARCHAR(128) NOT NULL,
       user_id VARCHAR(128) NOT NULL,
       state JSONB NOT NULL DEFAULT '{}'::jsonb,
       create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
       update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
   ) WITH (fillfactor = 80);

   CREATE INDEX idx_adk_sessions_app_user
       ON adk_sessions(app_name, user_id);

   CREATE INDEX idx_adk_sessions_update_time
       ON adk_sessions(update_time DESC);

   CREATE INDEX idx_adk_sessions_state
       ON adk_sessions USING GIN (state)
       WHERE state != '{}'::jsonb;

**Optimizations:**

- JSONB provides efficient JSON operations (``->``, ``->>``, ``@>``, etc.)
- GIN index enables fast JSONB queries (``WHERE state @> '{"key": "value"}'::jsonb``)
- FILLFACTOR 80 leaves 20% free space for HOT updates, reducing table bloat
- Partial index on state excludes empty states

Psycopg
-------

**Import:**

.. code-block:: python

   from sqlspec.adapters.psycopg import PsycopgAsyncConfig
   from sqlspec.adapters.psycopg.adk import PsycopgADKStore

**Features:**

- Modern PostgreSQL adapter (psycopg3)
- Both sync and async support
- Same SQL schema as AsyncPG
- Row factory for direct TypedDict conversion

**Configuration:**

.. code-block:: python

   from sqlspec.adapters.psycopg import PsycopgAsyncConfig
   from sqlspec.adapters.psycopg.adk import PsycopgADKStore

   config = PsycopgAsyncConfig(pool_config={
       "conninfo": "postgresql://user:pass@localhost/agentdb",
       "min_size": 5,
       "max_size": 20
   })

   store = PsycopgADKStore(config)
   await store.create_tables()

Psqlpy
------

**Import:**

.. code-block:: python

   from sqlspec.adapters.psqlpy import PsqlpyConfig
   from sqlspec.adapters.psqlpy.adk import PsqlpyADKStore

**Features:**

- High-performance Rust-based PostgreSQL driver
- Excellent async performance
- Same SQL schema as AsyncPG
- Tokio-based connection pooling

**Configuration:**

.. code-block:: python

   from sqlspec.adapters.psqlpy import PsqlpyConfig
   from sqlspec.adapters.psqlpy.adk import PsqlpyADKStore

   config = PsqlpyConfig(pool_config={
       "dsn": "postgresql://user:pass@localhost/agentdb",
       "max_db_pool_size": 20
   })

   store = PsqlpyADKStore(config)
   await store.create_tables()

MySQL / MariaDB Adapter
=======================

AsyncMy
-------

**Import:**

.. code-block:: python

   from sqlspec.adapters.asyncmy import AsyncmyConfig
   from sqlspec.adapters.asyncmy.adk import AsyncmyADKStore

.. seealso::

   :doc:`/examples/adk_basic_mysql`
      Complete runnable example using AsyncMy with MySQL/MariaDB

**Features:**

- Async MySQL/MariaDB driver
- JSON column type (MySQL 5.7.8+)
- Microsecond-precision TIMESTAMP(6)
- InnoDB engine for foreign keys
- Composite indexes

**Configuration:**

.. code-block:: python

   from sqlspec.adapters.asyncmy import AsyncmyConfig
   from sqlspec.adapters.asyncmy.adk import AsyncmyADKStore

   config = AsyncmyConfig(pool_config={
       "host": "localhost",
       "port": 3306,
       "user": "agent_user",
       "password": "secure_password",
       "database": "agentdb",
       "minsize": 5,
       "maxsize": 20
   })

   store = AsyncmyADKStore(config)
   await store.create_tables()

**Schema DDL:**

.. code-block:: sql

   CREATE TABLE IF NOT EXISTS adk_sessions (
       id VARCHAR(128) PRIMARY KEY,
       app_name VARCHAR(128) NOT NULL,
       user_id VARCHAR(128) NOT NULL,
       state JSON NOT NULL,
       create_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
       update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
           ON UPDATE CURRENT_TIMESTAMP(6),
       INDEX idx_adk_sessions_app_user (app_name, user_id),
       INDEX idx_adk_sessions_update_time (update_time DESC)
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

**Requirements:**

- MySQL 5.7.8+ or MariaDB 10.2.7+ (for JSON type)
- InnoDB engine (for foreign key support)
- utf8mb4 character set (for full Unicode support)

**Limitations:**

- No JSONB (uses JSON type, less optimized than PostgreSQL)
- No native JSON indexing (use virtual generated columns for indexing)
- AUTO-UPDATE requires application-level handling

SQLite Adapters
===============

SQLite is excellent for development, testing, and single-user applications.

SQLite (Sync)
-------------

**Import:**

.. code-block:: python

   from sqlspec.adapters.sqlite import SqliteConfig
   from sqlspec.adapters.sqlite.adk import SqliteADKStore

.. seealso::

   :doc:`/examples/adk_basic_sqlite`
      Complete runnable example using SQLite for local development

**Features:**

- Synchronous SQLite driver (stdlib sqlite3)
- Async wrapper for compatibility
- JSON stored as TEXT
- Julian Day timestamps
- Single-file database

**Configuration:**

.. code-block:: python

   from sqlspec.adapters.sqlite import SqliteConfig
   from sqlspec.adapters.sqlite.adk import SqliteADKStore

   config = SqliteConfig(pool_config={
       "database": "/path/to/agent.db",
       "check_same_thread": False  # Allow multi-threaded access
   })

   store = SqliteADKStore(config)
   store.create_tables()  # Sync method

**Schema DDL:**

.. code-block:: sql

   CREATE TABLE IF NOT EXISTS adk_sessions (
       id TEXT PRIMARY KEY,
       app_name TEXT NOT NULL,
       user_id TEXT NOT NULL,
       state TEXT NOT NULL DEFAULT '{}',  -- JSON as TEXT
       create_time REAL NOT NULL DEFAULT (julianday('now')),
       update_time REAL NOT NULL DEFAULT (julianday('now'))
   );

   CREATE INDEX IF NOT EXISTS idx_adk_sessions_app_user
       ON adk_sessions(app_name, user_id);

**Notes:**

- Timestamps stored as Julian Day numbers (REAL type)
- JSON stored as TEXT (use ``json_extract()`` for queries)
- BLOB for binary data (pickled actions)
- INTEGER for boolean values (0/1)

AIOSqlite
---------

**Import:**

.. code-block:: python

   from sqlspec.adapters.aiosqlite import AiosqliteConfig
   from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore

**Features:**

- Native async SQLite driver
- Same schema as sync SQLite
- Async/await interface
- Single-file database

**Configuration:**

.. code-block:: python

   from sqlspec.adapters.aiosqlite import AiosqliteConfig
   from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore

   config = AiosqliteConfig(pool_config={
       "database": "/path/to/agent.db"
   })

   store = AiosqliteADKStore(config)
   await store.create_tables()  # Async method

Oracle Adapter
==============

OracleDB
--------

**Import:**

.. code-block:: python

   from sqlspec.adapters.oracledb import OracleConfig
   from sqlspec.adapters.oracledb.adk import OracleADKStore

**Features:**

- Oracle Database 19c+ support
- CLOB for JSON storage
- BLOB for binary data
- TIMESTAMP(6) precision
- Both sync and async modes

**Configuration:**

.. code-block:: python

   from sqlspec.adapters.oracledb import OracleConfig
   from sqlspec.adapters.oracledb.adk import OracleADKStore

   config = OracleConfig(pool_config={
       "user": "agent_user",
       "password": "secure_password",
       "dsn": "localhost:1521/ORCLPDB1",
       "min": 5,
       "max": 20
   })

   store = OracleADKStore(config)
   await store.create_tables()

**Schema DDL:**

.. code-block:: sql

   CREATE TABLE adk_sessions (
       id VARCHAR2(128) PRIMARY KEY,
       app_name VARCHAR2(128) NOT NULL,
       user_id VARCHAR2(128) NOT NULL,
       state CLOB NOT NULL,  -- JSON stored as CLOB
       create_time TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL,
       update_time TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL
   );

   CREATE INDEX idx_adk_sessions_app_user
       ON adk_sessions(app_name, user_id);

**Notes:**

- JSON stored as CLOB (use ``JSON_VALUE()``, ``JSON_QUERY()`` for queries)
- VARCHAR2 for string fields (max 4000 bytes)
- BLOB for binary data
- NUMBER(1) for boolean values (0/1)

DuckDB Adapter (Development Only)
==================================

.. warning::

   **DuckDB is for development and testing ONLY.** DuckDB is an OLAP (analytical) database
   optimized for read-heavy analytical workloads, not concurrent transactional writes.
   It has limited concurrency support and write performance. **Do NOT use in production.**

DuckDB
------

**Import:**

.. code-block:: python

   from sqlspec.adapters.duckdb import DuckDBConfig
   from sqlspec.adapters.duckdb.adk import DuckDBADKStore

**Features:**

- Embedded analytical database
- Fast analytical queries
- JSON type support
- Single-file or in-memory

**Configuration:**

.. code-block:: python

   from sqlspec.adapters.duckdb import DuckDBConfig
   from sqlspec.adapters.duckdb.adk import DuckDBADKStore

   config = DuckDBConfig(pool_config={
       "database": ":memory:"  # Or "/path/to/agent.duckdb"
   })

   store = DuckDBADKStore(config)
   await store.create_tables()

**Limitations:**

- **Poor write concurrency** - not suitable for concurrent agent sessions
- **Not ACID compliant** for concurrent writes
- **Limited locking** - single-writer model
- **No production support** - use PostgreSQL, MySQL, or SQLite instead

**Use Cases:**

- Local development and prototyping
- Offline analysis of session logs
- Testing with analytical queries
- Single-user demos

Adapter Comparison
==================

.. list-table::
   :header-rows: 1
   :widths: 15 15 15 15 20 20

   * - Adapter
     - Database
     - Async
     - JSON Type
     - Best For
     - Notes
   * - AsyncPG
     - PostgreSQL
     - ✅
     - JSONB
     - Production (high scale)
     - Recommended
   * - Psycopg
     - PostgreSQL
     - ✅
     - JSONB
     - Production
     - Sync/Async support
   * - Psqlpy
     - PostgreSQL
     - ✅
     - JSONB
     - Production (performance)
     - Rust-based
   * - AsyncMy
     - MySQL
     - ✅
     - JSON
     - Production (MySQL shops)
     - Requires 5.7.8+
   * - SQLite
     - SQLite
     - ❌
     - TEXT
     - Development, single-user
     - Simple setup
   * - AIOSqlite
     - SQLite
     - ✅
     - TEXT
     - Development, testing
     - Native async
   * - OracleDB
     - Oracle
     - ✅
     - CLOB
     - Enterprise
     - Requires 19c+
   * - DuckDB
     - DuckDB
     - ❌
     - JSON
     - **Development ONLY**
     - Not for production

Custom Table Names
==================

All adapters support custom table names for multi-tenancy:

.. code-block:: python

   # Tenant A
   store_a = AsyncpgADKStore(
       config,
       session_table="tenant_a_sessions",
       events_table="tenant_a_events"
   )
   await store_a.create_tables()

   # Tenant B
   store_b = AsyncpgADKStore(
       config,
       session_table="tenant_b_sessions",
       events_table="tenant_b_events"
   )
   await store_b.create_tables()

Table name validation:

- Must start with letter or underscore
- Only alphanumeric characters and underscores
- Maximum 63 characters (PostgreSQL limit)
- Prevents SQL injection

Migration Considerations
========================

When migrating between databases:

**PostgreSQL → MySQL:**

- JSONB → JSON (less optimized)
- TIMESTAMPTZ → TIMESTAMP(6) (loses timezone)
- BYTEA → BLOB

**PostgreSQL → SQLite:**

- JSONB → TEXT (requires manual parsing)
- TIMESTAMPTZ → REAL (Julian Day)
- BYTEA → BLOB

**MySQL → PostgreSQL:**

- JSON → JSONB (more optimized)
- TIMESTAMP(6) → TIMESTAMPTZ (add timezone)
- BLOB → BYTEA

See :doc:`migrations` for migration script examples.

See Also
========

- :doc:`schema` - Detailed schema reference
- :doc:`api` - API documentation
- :doc:`/reference/adapters` - SQLSpec adapters reference
- :doc:`/examples/adk_basic_asyncpg` - PostgreSQL example
- :doc:`/examples/adk_basic_mysql` - MySQL example
- :doc:`/examples/adk_basic_sqlite` - SQLite example
- :doc:`/examples/adk_multi_tenant` - Multi-tenant deployment example
