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

   :doc:`/examples/adapters/asyncpg/connect_pool`
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

- PostgreSQL adapter (psycopg3)
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

   :doc:`/examples/extensions/adk/basic_aiosqlite`
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

   :doc:`/examples/extensions/adk/basic_aiosqlite`
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

- Oracle Database 12c+ support
- Automatic JSON storage type detection:
  - Oracle 21c+: Native JSON type
  - Oracle 12c-20c: BLOB with IS JSON constraint (recommended)
  - Oracle <12c: Plain BLOB
- BLOB for binary data
- TIMESTAMP WITH TIME ZONE for timezone-aware timestamps
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

**Schema DDL (Oracle 21c+ with Native JSON):**

.. code-block:: sql

   CREATE TABLE adk_sessions (
       id VARCHAR2(128) PRIMARY KEY,
       app_name VARCHAR2(128) NOT NULL,
       user_id VARCHAR2(128) NOT NULL,
       state JSON NOT NULL,  -- Native JSON type (Oracle 21c+)
       create_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
       update_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
   );

   CREATE INDEX idx_adk_sessions_app_user
       ON adk_sessions(app_name, user_id);

**Schema DDL (Oracle 12c-20c with BLOB + JSON Constraint):**

.. code-block:: sql

   CREATE TABLE adk_sessions (
       id VARCHAR2(128) PRIMARY KEY,
       app_name VARCHAR2(128) NOT NULL,
       user_id VARCHAR2(128) NOT NULL,
       state BLOB CHECK (state IS JSON) NOT NULL,  -- BLOB with JSON validation
       create_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
       update_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
   );

   CREATE INDEX idx_adk_sessions_app_user
       ON adk_sessions(app_name, user_id);

**Notes:**

- **Automatic version detection:** Store automatically uses the best storage type for your Oracle version
- **JSON storage:** Native JSON (21c+), BLOB with IS JSON (12c-20c), or plain BLOB (<12c)
- **BLOB preferred over CLOB:** Better performance due to character set independence
- VARCHAR2 for string fields (max 4000 bytes)
- NUMBER(1) for boolean values (0/1)
- Use ``JSON_VALUE()``, ``JSON_QUERY()`` for JSON queries

BigQuery Adapter
================

Google Cloud BigQuery is a serverless, highly scalable data warehouse optimized for
analytics workloads. It's an excellent choice for storing and analyzing large volumes
of AI agent session and event data.

.. seealso::

   :doc:`backends/bigquery`
      Complete BigQuery backend documentation with cost optimization guide

BigQuery
--------

**Import:**

.. code-block:: python

   from sqlspec.adapters.bigquery import BigQueryConfig
   from sqlspec.adapters.bigquery.adk import BigQueryADKStore

**Features:**

- **Serverless** - No infrastructure management required
- **Scalable** - Handles petabyte-scale data seamlessly
- **Native JSON type** - Efficient JSON storage and querying
- **Partitioning & Clustering** - Automatic query optimization
- **Cost-effective** - Pay only for queries run (bytes scanned)
- **Analytics-optimized** - Built for complex aggregations

**Configuration:**

.. code-block:: python

   from sqlspec.adapters.bigquery import BigQueryConfig
   from sqlspec.adapters.bigquery.adk import BigQueryADKStore

   config = BigQueryConfig(
       connection_config={
           "project": "my-gcp-project",
           "dataset_id": "my_dataset",
           "use_query_cache": True,
           "maximum_bytes_billed": 100000000,  # 100 MB cost limit
       }
   )

   store = BigQueryADKStore(config)
   await store.create_tables()

**Schema DDL:**

.. code-block:: sql

   CREATE TABLE `dataset.adk_sessions` (
       id STRING NOT NULL,
       app_name STRING NOT NULL,
       user_id STRING NOT NULL,
       state JSON NOT NULL,  -- Native JSON type
       create_time TIMESTAMP NOT NULL,
       update_time TIMESTAMP NOT NULL
   )
   PARTITION BY DATE(create_time)
   CLUSTER BY app_name, user_id;

   CREATE TABLE `dataset.adk_events` (
       id STRING NOT NULL,
       session_id STRING NOT NULL,
       app_name STRING NOT NULL,
       user_id STRING NOT NULL,
       invocation_id STRING,
       author STRING,
       actions BYTES,
       long_running_tool_ids_json JSON,
       branch STRING,
       timestamp TIMESTAMP NOT NULL,
       content JSON,
       grounding_metadata JSON,
       custom_metadata JSON,
       partial BOOL,
       turn_complete BOOL,
       interrupted BOOL,
       error_code STRING,
       error_message STRING
   )
   PARTITION BY DATE(timestamp)
   CLUSTER BY session_id, timestamp;

**Best For:**

- Large-scale AI agent deployments (millions of users)
- Analytics and insights on agent interactions
- Long-term storage of conversation history
- Multi-region deployments requiring global scalability
- Applications already using Google Cloud Platform

**Considerations:**

- Eventual consistency (writes may take seconds to be visible)
- Pay-per-query cost model (optimize queries carefully)
- No foreign keys (implements cascade delete manually)
- Optimized for analytics, not high-frequency transactional updates

**Cost Optimization:**

BigQuery charges based on bytes scanned. The store implements:

- **Partitioning by date** - Reduces data scanned for time-based queries
- **Clustering** - Optimizes filtering on app_name, user_id, session_id
- **Query caching** - Automatically caches results for 24 hours
- **Byte limits** - Prevents runaway query costs

.. note::

   For highly concurrent transactional workloads with frequent small DML operations,
   PostgreSQL or Oracle are better choices. BigQuery excels at storing and analyzing
   large volumes of session/event data with complex analytical queries.

DuckDB Adapter
==============

DuckDB is an embedded OLAP database optimized for analytical queries. It provides excellent
performance for read-heavy workloads and analytical operations on session data.

.. note::

   DuckDB is optimized for OLAP workloads and analytical queries. For highly concurrent
   DML operations (frequent inserts/updates/deletes), consider PostgreSQL or other
   OLTP-optimized databases.

DuckDB
------

**Import:**

.. code-block:: python

   from sqlspec.adapters.duckdb import DuckDBConfig
   from sqlspec.adapters.duckdb.adk import DuckdbADKStore

**Features:**

- **Zero-configuration setup** - embedded database, no server required
- **Native JSON type** - efficient JSON storage and querying
- **Columnar storage** - excellent for analytical queries on session data
- **Single-file or in-memory** - flexible deployment options
- **ACID guarantees** - reliable transaction support

**Configuration:**

.. code-block:: python

   from sqlspec.adapters.duckdb import DuckDBConfig
   from sqlspec.adapters.duckdb.adk import DuckdbADKStore

   # File-based database
   config = DuckDBConfig(pool_config={
       "database": "/path/to/sessions.duckdb"
   })

   # Or in-memory for testing
   config = DuckDBConfig(pool_config={
       "database": ":memory:"
   })

   store = DuckdbADKStore(config)
   store.create_tables()  # Sync interface

**Schema DDL:**

.. code-block:: sql

   CREATE TABLE IF NOT EXISTS adk_sessions (
       id VARCHAR PRIMARY KEY,
       app_name VARCHAR NOT NULL,
       user_id VARCHAR NOT NULL,
       state JSON NOT NULL,  -- Native JSON type
       create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
   );

   CREATE INDEX idx_adk_sessions_app_user
       ON adk_sessions(app_name, user_id);
   CREATE INDEX idx_adk_sessions_update_time
       ON adk_sessions(update_time DESC);

**Best For:**

- Development and testing (zero-configuration setup)
- Analytical workloads on session data (session analytics, reporting)
- Embedded applications (single-file database)
- Offline analysis of session logs
- Prototyping and demos

**Considerations:**

- Optimized for OLAP, not high-concurrency writes
- For production systems with frequent concurrent writes, PostgreSQL is recommended
- Manual cascade delete required (DuckDB doesn't support CASCADE in foreign keys)

ADBC (Arrow Database Connectivity)
===================================

ADBC provides a vendor-neutral API for database access using Apache Arrow's columnar format.
It supports multiple backend databases through a single consistent interface.

**Import:**

.. code-block:: python

   from sqlspec.adapters.adbc import AdbcConfig
   from sqlspec.adapters.adbc.adk import AdbcADKStore

.. seealso::

   :doc:`backends/adbc`
      Complete ADBC backend guide with examples for PostgreSQL, SQLite, DuckDB, and more

**Features:**

- Zero-copy data transfer via Apache Arrow
- Columnar format for analytical workloads
- Vendor-neutral (PostgreSQL, SQLite, DuckDB, Snowflake, Flight SQL)
- High-performance bulk operations
- Arrow ecosystem integration (Polars, PyArrow)

**Configuration:**

.. code-block:: python

   from sqlspec.adapters.adbc import AdbcConfig
   from sqlspec.adapters.adbc.adk import AdbcADKStore

   # SQLite backend
   config = AdbcConfig(connection_config={
       "driver_name": "sqlite",
       "uri": "file:agent.db"
   })

   # PostgreSQL backend
   config = AdbcConfig(connection_config={
       "driver_name": "postgresql",
       "uri": "postgresql://user:pass@localhost:5432/agentdb"
   })

   store = AdbcADKStore(config)
   store.create_tables()

**Schema DDL (Database-Agnostic):**

.. code-block:: sql

   CREATE TABLE IF NOT EXISTS adk_sessions (
       id VARCHAR(128) PRIMARY KEY,
       app_name VARCHAR(128) NOT NULL,
       user_id VARCHAR(128) NOT NULL,
       state TEXT NOT NULL DEFAULT '{}',
       create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
   )

   CREATE INDEX IF NOT EXISTS idx_adk_sessions_app_user
       ON adk_sessions(app_name, user_id);
   CREATE INDEX IF NOT EXISTS idx_adk_sessions_update_time
       ON adk_sessions(update_time DESC);

**Best For:**

- Multi-database applications requiring portability
- Analytical AI agents processing large datasets
- Integration with Arrow ecosystem tools
- Bulk data operations and ETL pipelines
- Applications needing zero-copy data transfer

**Considerations:**

- Synchronous API (no native async support)
- TEXT storage for JSON (less optimized than native JSONB)
- SQLite backend: Foreign key cascade deletes require explicit connection-level setup
- Creates new connection per operation by default

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
   * - BigQuery
     - Google Cloud
     - ✅
     - JSON
     - Analytics, massive scale
     - Serverless, partitioned
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
     - JSON/BLOB+CHECK
     - Enterprise
     - Auto-detects version
   * - DuckDB
     - DuckDB
     - ❌ (sync)
     - JSON
     - OLAP/Analytics
     - Embedded, zero-config
   * - ADBC
     - Multi (PostgreSQL, SQLite, DuckDB, etc.)
     - ❌ (sync)
     - TEXT
     - Arrow ecosystem, analytics
     - Zero-copy, vendor-neutral

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
- :doc:`backends/adbc` - ADBC backend guide
- :doc:`backends/bigquery` - BigQuery backend guide
- :doc:`/examples/adapters/asyncpg/connect_pool` - PostgreSQL example
- :doc:`/examples/extensions/adk/basic_aiosqlite` - SQLite example
- :doc:`/examples/extensions/adk/litestar_aiosqlite` - Litestar integration example
- :doc:`/examples/patterns/multi_tenant/router` - Multi-tenant deployment example
