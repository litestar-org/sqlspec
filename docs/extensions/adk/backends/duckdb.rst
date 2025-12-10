===============
DuckDB Backend
===============

Overview
========

DuckDB is an embedded analytical database (OLAP) optimized for complex queries and aggregations.
While not designed for high-concurrency transactional workloads, DuckDB excels at session analytics,
reporting, and embedded use cases with zero-configuration setup.

**Key Features:**

- **Embedded Database**: No server setup required, single-file or in-memory
- **Native JSON Support**: Efficient JSON type for state and metadata storage
- **Columnar Storage**: Optimized for analytical queries on session data
- **ACID Transactions**: Reliable transaction support
- **SQL Analytics**: Advanced SQL features for session analysis
- **Zero Configuration**: Perfect for development and testing

**Ideal Use Cases:**

- Development and testing environments
- Session analytics and reporting dashboards
- Embedded applications requiring local data storage
- Offline analysis of exported session logs
- Prototyping AI agent applications

.. warning::

   **DuckDB is optimized for OLAP workloads**, not high-frequency transactional operations.
   For production AI agents with concurrent user sessions and frequent writes, use
   PostgreSQL or MySQL. DuckDB is best suited for analytics, development, and embedded scenarios.

Installation
============

Install SQLSpec with DuckDB support:

.. code-block:: bash

   pip install sqlspec[duckdb] google-genai
   # or
   uv pip install sqlspec[duckdb] google-genai

Quick Start
===========

Basic File-Based Database
--------------------------

.. code-block:: python

   from sqlspec.adapters.duckdb import DuckDBConfig
   from sqlspec.adapters.duckdb.adk import DuckdbADKStore
   from sqlspec.extensions.adk import SQLSpecSessionService

   # Create file-based database
   config = DuckDBConfig(database="./agent_sessions.duckdb")

   store = DuckdbADKStore(config)
   store.create_tables()

   service = SQLSpecSessionService(store)

   # Create session
   session = service.create_session(
       app_name="analytics_agent",
       user_id="analyst_1",
       state={"dashboard": "active"}
   )

In-Memory Database (Testing)
-----------------------------

.. code-block:: python

   from sqlspec.adapters.duckdb import DuckDBConfig
   from sqlspec.adapters.duckdb.adk import DuckdbADKStore

   # Create in-memory database
   config = DuckDBConfig(database=":memory:")

   store = DuckdbADKStore(config)
   store.create_tables()

.. tip::

   In-memory databases are perfect for unit tests and ephemeral workloads.
   All data is lost when the process exits.

Configuration
=============

Basic Configuration
-------------------

.. code-block:: python

   from sqlspec.adapters.duckdb import DuckDBConfig

   config = DuckDBConfig(
       database="/path/to/database.duckdb",  # or ":memory:"
       read_only=False,
       config={
           "threads": 4,
           "max_memory": "1GB",
       }
   )

Custom Table Names
------------------

.. code-block:: python

   store = DuckdbADKStore(
       config,
       session_table="agent_sessions",
       events_table="agent_events"
   )

Schema
======

Sessions Table
--------------

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

Events Table
------------

.. code-block:: sql

   CREATE TABLE IF NOT EXISTS adk_events (
       id VARCHAR PRIMARY KEY,
       session_id VARCHAR NOT NULL,
       app_name VARCHAR NOT NULL,
       user_id VARCHAR NOT NULL,
       invocation_id VARCHAR,
       author VARCHAR,
       actions BLOB,
       long_running_tool_ids_json VARCHAR,
       branch VARCHAR,
       timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       content JSON,
       grounding_metadata JSON,
       custom_metadata JSON,
       partial BOOLEAN,
       turn_complete BOOLEAN,
       interrupted BOOLEAN,
       error_code VARCHAR,
       error_message VARCHAR,
       FOREIGN KEY (session_id) REFERENCES adk_sessions(id)
   );

   CREATE INDEX idx_adk_events_session
       ON adk_events(session_id, timestamp ASC);

.. note::

   DuckDB supports foreign keys but **does not support CASCADE deletes**.
   The store manually deletes events when a session is deleted.

Analytical Queries
==================

DuckDB's strength is analytical SQL. Here are examples for session analysis:

Session Activity by User
-------------------------

.. code-block:: sql

   SELECT
       user_id,
       COUNT(*) as total_sessions,
       AVG(julianday(update_time) - julianday(create_time)) as avg_duration_days
   FROM adk_sessions
   WHERE app_name = 'my_agent'
   GROUP BY user_id
   ORDER BY total_sessions DESC
   LIMIT 10;

Event Distribution
------------------

.. code-block:: sql

   SELECT
       author,
       COUNT(*) as event_count,
       COUNT(DISTINCT session_id) as sessions_with_events
   FROM adk_events
   WHERE app_name = 'my_agent'
   GROUP BY author;

Most Active Sessions
--------------------

.. code-block:: sql

   SELECT
       s.id,
       s.user_id,
       COUNT(e.id) as event_count,
       MIN(e.timestamp) as first_event,
       MAX(e.timestamp) as last_event
   FROM adk_sessions s
   LEFT JOIN adk_events e ON s.id = e.session_id
   WHERE s.app_name = 'my_agent'
   GROUP BY s.id, s.user_id
   ORDER BY event_count DESC
   LIMIT 20;

JSON Extraction
---------------

.. code-block:: sql

   -- Extract values from session state
   SELECT
       id,
       user_id,
       json_extract(state, '$.dashboard') as dashboard_type,
       json_extract(state, '$.filters.date_range') as date_range
   FROM adk_sessions
   WHERE app_name = 'analytics_bot';

Time-Series Analysis
--------------------

.. code-block:: sql

   -- Events per hour
   SELECT
       DATE_TRUNC('hour', timestamp) as hour,
       COUNT(*) as event_count,
       COUNT(DISTINCT session_id) as active_sessions
   FROM adk_events
   WHERE app_name = 'my_agent'
     AND timestamp >= CURRENT_TIMESTAMP - INTERVAL 24 HOUR
   GROUP BY hour
   ORDER BY hour;

Use Cases
=========

Development & Testing
---------------------

DuckDB's zero-configuration setup makes it ideal for development:

.. code-block:: python

   # Quick setup for development
   config = DuckDBConfig(database=":memory:")
   store = DuckdbADKStore(config)
   store.create_tables()

   # No database server needed!
   service = SQLSpecSessionService(store)
   session = service.create_session("dev_app", "dev_user", {})

Session Analytics Dashboard
----------------------------

Build analytics on top of session data:

.. code-block:: python

   import duckdb

   # Connect to existing DuckDB database
   conn = duckdb.connect("agent_sessions.duckdb")

   # Run analytical query
   result = conn.execute("""
       SELECT
           DATE_TRUNC('day', create_time) as day,
           COUNT(*) as sessions_created,
           COUNT(DISTINCT user_id) as unique_users
       FROM adk_sessions
       WHERE app_name = 'my_agent'
       GROUP BY day
       ORDER BY day DESC
       LIMIT 30
   """).fetchall()

   for day, sessions, users in result:
       print(f"{day}: {sessions} sessions, {users} unique users")

Embedded Applications
---------------------

Embed DuckDB in desktop applications:

.. code-block:: python

   from pathlib import Path

   # Store database in application data directory
   app_data = Path.home() / ".my_agent" / "sessions.duckdb"
   app_data.parent.mkdir(parents=True, exist_ok=True)

   config = DuckDBConfig(database=str(app_data))
   store = DuckdbADKStore(config)
   store.create_tables()

Performance Characteristics
===========================

Strengths
---------

- **Analytical Queries**: Excellent for aggregations, joins, and complex analytics
- **Columnar Storage**: Efficient for scanning large datasets
- **Single-File Portability**: Easy to backup, copy, and deploy
- **Memory Efficiency**: Can handle datasets larger than RAM
- **SQL Features**: Advanced SQL analytics functions available

Limitations
-----------

- **Concurrent Writes**: Limited support for concurrent INSERT/UPDATE/DELETE
- **No CASCADE Deletes**: Must manually handle cascading deletes
- **Transaction Model**: Optimized for read-heavy workloads
- **Single Writer**: Only one write transaction at a time

When to Use DuckDB
==================

**Ideal For:**

✅ Development and testing environments
✅ Session analytics and reporting
✅ Embedded applications (desktop, mobile)
✅ Offline analysis of session logs
✅ Prototyping and demos
✅ Data science workflows on session data

**Consider PostgreSQL Instead When:**

❌ High-concurrency production AI agent (many simultaneous users)
❌ Frequent transactional updates required
❌ Need server-based deployment with connection pooling
❌ Require JSONB indexing for performance
❌ Need CASCADE deletes and full referential integrity

Comparison: DuckDB vs PostgreSQL
---------------------------------

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Feature
     - DuckDB
     - PostgreSQL
   * - Setup Complexity
     - Zero config, embedded
     - Requires server setup
   * - Concurrent Writes
     - Limited
     - Excellent
   * - Analytical Queries
     - Excellent
     - Good
   * - JSON Support
     - Native JSON type
     - Native JSONB with indexes
   * - Deployment
     - Single file
     - Client-server
   * - Best Use Case
     - Analytics, development
     - Production AI agents

Example: Full Application
==========================

Follow the same script as :doc:`/examples/extensions/adk/basic_aiosqlite` and swap in
``DuckDBConfig`` plus the DuckDB ADK store. Because DuckDB runs in-process, you can drop the
connection pooling configuration entirely while keeping the session service logic identical.

Troubleshooting
===============

Foreign Key Constraint Errors
------------------------------

If you see foreign key errors, ensure the session exists before creating events:

.. code-block:: python

   # Always create session first
   session = service.create_session("app", "user", {})

   # Then create events
   event = service.append_event(session, user_event)

Database File Locked
--------------------

DuckDB uses file locking. If you see "database is locked" errors:

.. code-block:: python

   # Close connection properly
   store.close()  # If available

   # Or use in-memory for testing
   config = DuckDBConfig(database=":memory:")

Migration from DuckDB to PostgreSQL
====================================

When your prototype becomes production, migrate to PostgreSQL:

.. code-block:: python

   # Export from DuckDB
   import duckdb

   duck_conn = duckdb.connect("agent_sessions.duckdb")
   sessions = duck_conn.execute("SELECT * FROM adk_sessions").fetchall()
   events = duck_conn.execute("SELECT * FROM adk_events").fetchall()

   # Import to PostgreSQL
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore

   pg_config = AsyncpgConfig(connection_config={"dsn": "postgresql://..."})
   pg_store = AsyncpgADKStore(pg_config)
   await pg_store.create_tables()

   # Insert data (handle async properly)
   for session in sessions:
       await pg_store.create_session(
           session_id=session[0],
           app_name=session[1],
           user_id=session[2],
           state=session[3]
       )

API Reference
=============

.. autoclass:: sqlspec.adapters.duckdb.adk.DuckdbADKStore
   :members:
   :inherited-members:
   :show-inheritance:
   :no-index:

See Also
========

- :doc:`../quickstart` - Quick start guide
- :doc:`../adapters` - Adapter comparison
- :doc:`../schema` - Database schema details
- :doc:`/examples/extensions/adk/basic_aiosqlite` - Reference implementation (swap in DuckDB config)
- `DuckDB Documentation <https://duckdb.org/docs/>`_ - Official DuckDB documentation
- `DuckDB SQL Reference <https://duckdb.org/docs/sql/introduction>`_ - SQL syntax and functions
