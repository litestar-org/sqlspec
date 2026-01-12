=================
Psycopg Backend
=================

Overview
========

Psycopg3 is a redesigned PostgreSQL adapter that provides both **synchronous and asynchronous**
database access with native support for PostgreSQL-specific features like JSONB, server-side cursors,
and the COPY protocol.

**Key Features:**

- **Dual Mode**: Native async/await AND synchronous execution in a single adapter
- **Type Safety**: Explicit ``Jsonb()`` wrapper for type-safe JSONB operations
- **SQL Composition**: Secure SQL building with ``pg_sql.SQL()`` and ``pg_sql.Identifier()``
- **Binary Protocol**: Efficient binary data transfer by default
- **Connection Pooling**: Built-in ``psycopg_pool`` with async support
- **Server-Side Cursors**: Memory-efficient processing of large result sets
- **Contemporary Design**: Fully redesigned API for PostgreSQL

.. warning::

   **CRITICAL: JSONB Type Safety**

   Unlike asyncpg or psqlpy, psycopg3 requires explicitly wrapping Python dicts
   with ``Jsonb()`` when inserting JSONB data. This provides stronger type safety
   but means you cannot pass raw dicts directly to JSONB columns.

   .. code-block:: python

      from psycopg.types.json import Jsonb

      # WRONG - Will fail
      await cur.execute("INSERT INTO table (data) VALUES (%s)", ({"key": "value"},))

      # CORRECT - Wrap with Jsonb()
      await cur.execute("INSERT INTO table (data) VALUES (%s)", (Jsonb({"key": "value"}),))

Installation
============

Install SQLSpec with Psycopg support:

.. code-block:: bash

   # Binary distribution (recommended for development)
   pip install sqlspec[psycopg] google-genai

   # C extension (better performance for production)
   pip install sqlspec[psycopg] psycopg[c] google-genai

   # With connection pooling (recommended)
   pip install sqlspec[psycopg] psycopg-pool google-genai

.. tip::

   **Performance Options:**

   - ``psycopg[binary]`` - Pure Python, easier installation
   - ``psycopg[c]`` - C extension, ~30% faster, requires compiler
   - ``psycopg-pool`` - Connection pooling, required for production

Quick Start
===========

Async Usage (Recommended)
--------------------------

.. code-block:: python

   from sqlspec.adapters.psycopg import PsycopgAsyncConfig
   from sqlspec.adapters.psycopg.adk import PsycopgAsyncADKStore
   from sqlspec.extensions.adk import SQLSpecSessionService

   # Create async config with connection pool
   config = PsycopgAsyncConfig(
       connection_config={
           "conninfo": "postgresql://user:pass@localhost/db",
           "min_size": 5,
           "max_size": 20,
       }
   )

   # Create async store
   store = PsycopgAsyncADKStore(config)
   await store.create_tables()

   # Create session service
   service = SQLSpecSessionService(store)

   # Create session with JSONB state
   session = await service.create_session(
       app_name="my_agent",
       user_id="user_123",
       state={"context": "active", "preferences": {"theme": "dark"}}
   )

Sync Usage
----------

.. code-block:: python

   from sqlspec.adapters.psycopg import PsycopgSyncConfig
   from sqlspec.adapters.psycopg.adk import PsycopgSyncADKStore
   from sqlspec.extensions.adk import SQLSpecSessionService

   # Create sync config with connection pool
   config = PsycopgSyncConfig(
       connection_config={
           "conninfo": "postgresql://user:pass@localhost/db",
           "min_size": 5,
           "max_size": 20,
       }
   )

   # Create sync store
   store = PsycopgSyncADKStore(config)
   store.create_tables()

   # Create session service
   service = SQLSpecSessionService(store)

   # Create session
   session = service.create_session(
       app_name="my_agent",
       user_id="user_123",
       state={"context": "active"}
   )

Configuration
=============

Basic Async Configuration
--------------------------

.. code-block:: python

   from sqlspec.adapters.psycopg import PsycopgAsyncConfig

   config = PsycopgAsyncConfig(
       connection_config={
           "conninfo": "postgresql://user:pass@localhost:5432/dbname",
           "min_size": 5,           # Minimum pool connections
           "max_size": 20,          # Maximum pool connections
           "timeout": 30.0,         # Connection acquisition timeout
           "max_lifetime": 3600.0,  # Max connection lifetime (1 hour)
           "max_idle": 600.0,       # Max connection idle time (10 min)
       }
   )

Basic Sync Configuration
-------------------------

.. code-block:: python

   from sqlspec.adapters.psycopg import PsycopgSyncConfig

   config = PsycopgSyncConfig(
       connection_config={
           "conninfo": "postgresql://user:pass@localhost:5432/dbname",
           "min_size": 5,
           "max_size": 20,
       }
   )

Advanced Configuration
----------------------

.. code-block:: python

   config = PsycopgAsyncConfig(
       connection_config={
           # Connection string
           "conninfo": "postgresql://user:pass@localhost/db?sslmode=require",

           # OR individual parameters
           "host": "localhost",
           "port": 5432,
           "user": "myuser",
           "password": "mypass",
           "dbname": "mydb",

           # Pool settings
           "min_size": 5,
           "max_size": 20,
           "timeout": 30.0,
           "max_waiting": 0,        # Max queued connection requests
           "max_lifetime": 3600.0,  # Recycle connections hourly
           "max_idle": 600.0,       # Close idle connections after 10min
           "reconnect_timeout": 300.0,
           "num_workers": 3,        # Background worker threads

           # Connection settings
           "connect_timeout": 10,
           "application_name": "my_adk_agent",
           "sslmode": "require",
           "autocommit": False,
       }
   )

Custom Table Names
------------------

.. code-block:: python

   store = PsycopgAsyncADKStore(
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
       id VARCHAR(128) PRIMARY KEY,
       app_name VARCHAR(128) NOT NULL,
       user_id VARCHAR(128) NOT NULL,
       state JSONB NOT NULL DEFAULT '{}'::jsonb,  -- PostgreSQL JSONB type
       create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
       update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
   ) WITH (fillfactor = 80);  -- HOT updates optimization

   -- Composite index for listing sessions
   CREATE INDEX IF NOT EXISTS idx_adk_sessions_app_user
       ON adk_sessions(app_name, user_id);

   -- Index for recent sessions queries
   CREATE INDEX IF NOT EXISTS idx_adk_sessions_update_time
       ON adk_sessions(update_time DESC);

   -- Partial GIN index for JSONB queries (only non-empty state)
   CREATE INDEX IF NOT EXISTS idx_adk_sessions_state
       ON adk_sessions USING GIN (state)
       WHERE state != '{}'::jsonb;

Events Table
------------

.. code-block:: sql

   CREATE TABLE IF NOT EXISTS adk_events (
       id VARCHAR(128) PRIMARY KEY,
       session_id VARCHAR(128) NOT NULL,
       app_name VARCHAR(128) NOT NULL,
       user_id VARCHAR(128) NOT NULL,
       invocation_id VARCHAR(256),
       author VARCHAR(256),
       actions BYTEA,                        -- Binary serialized actions
       long_running_tool_ids_json TEXT,
       branch VARCHAR(256),
       timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
       content JSONB,                        -- Message content
       grounding_metadata JSONB,             -- Grounding information
       custom_metadata JSONB,                -- Custom application data
       partial BOOLEAN,
       turn_complete BOOLEAN,
       interrupted BOOLEAN,
       error_code VARCHAR(256),
       error_message VARCHAR(1024),
       FOREIGN KEY (session_id) REFERENCES adk_sessions(id) ON DELETE CASCADE
   );

   -- Composite index for event retrieval
   CREATE INDEX IF NOT EXISTS idx_adk_events_session
       ON adk_events(session_id, timestamp ASC);

.. note::

   **PostgreSQL-Specific Features:**

   - ``JSONB`` - Binary JSON type, more efficient than JSON text
   - ``TIMESTAMPTZ`` - Timezone-aware timestamps with microsecond precision
   - ``BYTEA`` - Binary data storage for pickled actions
   - ``FILLFACTOR 80`` - Leaves space for HOT updates, reduces bloat
   - ``GIN Index`` - Efficient JSONB queries and containment operations
   - ``CASCADE DELETE`` - Automatic cleanup of events when session deleted

Usage Patterns
==============

CRITICAL: Jsonb() Wrapper Requirement
--------------------------------------

Psycopg3 requires explicit type wrapping for JSONB data:

.. code-block:: python

   from psycopg.types.json import Jsonb

   # Creating session with JSONB state
   state = {"user": "alice", "preferences": {"theme": "dark"}}

   # Store handles Jsonb() wrapping internally
   session = await service.create_session(
       app_name="my_app",
       user_id="alice",
       state=state  # Automatically wrapped internally
   )

   # Manual cursor usage - MUST wrap yourself
   async with config.provide_connection() as conn:
       async with conn.cursor() as cur:
           # WRONG - Will fail with type error
           await cur.execute(
               "INSERT INTO sessions (state) VALUES (%s)",
               ({"key": "value"},)
           )

           # CORRECT - Wrap with Jsonb()
           await cur.execute(
               "INSERT INTO sessions (state) VALUES (%s)",
               (Jsonb({"key": "value"}),)
           )

SQL Composition with pg_sql
----------------------------

Psycopg3 provides safe SQL composition tools:

.. code-block:: python

   from psycopg import sql as pg_sql
   from psycopg.types.json import Jsonb

   # Safe dynamic table/column names
   async with config.provide_connection() as conn:
       async with conn.cursor() as cur:
           # Compose SQL with identifiers (prevents SQL injection)
           query = pg_sql.SQL("""
               INSERT INTO {table} (id, state, update_time)
               VALUES (%s, %s, CURRENT_TIMESTAMP)
           """).format(table=pg_sql.Identifier("adk_sessions"))

           await cur.execute(query, (session_id, Jsonb(state)))

           # Multiple identifiers
           query = pg_sql.SQL("""
               SELECT {col1}, {col2} FROM {table} WHERE {col1} = %s
           """).format(
               col1=pg_sql.Identifier("user_id"),
               col2=pg_sql.Identifier("state"),
               table=pg_sql.Identifier("adk_sessions")
           )

           await cur.execute(query, ("user_123",))

.. warning::

   **Never use f-strings or format() for SQL construction!**

   Use ``pg_sql.SQL()`` and ``pg_sql.Identifier()`` to prevent SQL injection.

Cursor Context Managers
------------------------

Psycopg3 requires cursor context managers:

.. code-block:: python

   # Async cursor pattern
   async with config.provide_connection() as conn:
       async with conn.cursor() as cur:
           await cur.execute("SELECT * FROM adk_sessions WHERE user_id = %s", ("alice",))
           rows = await cur.fetchall()

   # Sync cursor pattern
   with config.provide_connection() as conn:
       with conn.cursor() as cur:
           cur.execute("SELECT * FROM adk_sessions WHERE user_id = %s", ("alice",))
           rows = cur.fetchall()

Server-Side Cursors (Large Result Sets)
----------------------------------------

For processing large event histories:

.. code-block:: python

   async with config.provide_connection() as conn:
       # Named cursor creates server-side cursor
       async with conn.cursor(name="large_event_query") as cur:
           await cur.execute("""
               SELECT * FROM adk_events
               WHERE app_name = %s
               ORDER BY timestamp ASC
           """, ("my_app",))

           # Stream results without loading all into memory
           async for row in cur:
               process_event(row)

Transaction Management
----------------------

.. code-block:: python

   # Async transaction with context manager
   async with config.provide_connection() as conn:
       async with conn.transaction():
           async with conn.cursor() as cur:
               await cur.execute(sql1)
               await cur.execute(sql2)
               # Auto-commit on success, rollback on exception

   # Sync transaction
   with config.provide_connection() as conn:
       with conn.transaction():
           with conn.cursor() as cur:
               cur.execute(sql1)
               cur.execute(sql2)

   # Manual transaction control
   async with config.provide_connection() as conn:
       await conn.set_autocommit(False)
       async with conn.cursor() as cur:
           try:
               await cur.execute(sql1)
               await cur.execute(sql2)
               await conn.commit()
           except Exception:
               await conn.rollback()
               raise

Performance Considerations
==========================

JSONB with Jsonb() Wrapper
---------------------------

The explicit ``Jsonb()`` wrapper provides:

**Advantages:**

- Type safety - Catch errors at insert time, not query time
- Explicit conversion - Clear when JSONB type is intended
- Performance - Binary protocol optimization for JSONB

**Pattern:**

.. code-block:: python

   from psycopg.types.json import Jsonb

   # Session state
   state = {"key": "value"}

   # Event content
   content = {"parts": [{"text": "Hello"}]}

   # Metadata
   metadata = {"source": "web", "version": "1.0"}

   # All must be wrapped when inserting manually
   await cur.execute(
       "INSERT INTO events (content, metadata) VALUES (%s, %s)",
       (Jsonb(content), Jsonb(metadata))
   )

Connection Pooling
------------------

Psycopg3 has built-in connection pooling via ``psycopg_pool``:

.. code-block:: python

   config = PsycopgAsyncConfig(
       connection_config={
           "conninfo": "postgresql://...",
           "min_size": 5,           # Pre-create 5 connections
           "max_size": 20,          # Allow up to 20 connections
           "max_lifetime": 3600.0,  # Recycle connections hourly
           "max_idle": 600.0,       # Close idle connections after 10min
           "num_workers": 3,        # Background maintenance workers
       }
   )

**Pool Benefits:**

- Connection reuse - Avoid expensive connection establishment
- Resource limits - Prevent connection exhaustion
- Auto-reconnect - Handle connection failures gracefully
- Background maintenance - Periodic connection health checks

Binary Protocol
---------------

Psycopg3 uses binary protocol by default:

- Faster than text protocol (~30% for large datasets)
- More efficient for JSONB, BYTEA, arrays
- Automatic type adaptation

COPY Protocol (Bulk Operations)
--------------------------------

For bulk event insertion:

.. code-block:: python

   async with config.provide_connection() as conn:
       async with conn.cursor() as cur:
           # COPY is much faster than executemany for bulk inserts
           async with cur.copy("COPY adk_events (id, session_id, ...) FROM STDIN") as copy:
               for event in large_event_list:
                   await copy.write_row(event)

Prepared Statements
-------------------

Psycopg3 automatically prepares frequently-used queries:

- No manual preparation needed
- Performance benefit for repeated queries
- Automatic cache management

Best Practices
==============

SQL Composition Best Practices
-------------------------------

.. code-block:: python

   from psycopg import sql as pg_sql

   # GOOD - Safe identifier composition
   query = pg_sql.SQL("SELECT * FROM {table} WHERE {col} = %s").format(
       table=pg_sql.Identifier("adk_sessions"),
       col=pg_sql.Identifier("user_id")
   )

   # BAD - SQL injection risk
   table_name = "adk_sessions"
   query = f"SELECT * FROM {table_name} WHERE user_id = %s"  # DON'T!

JSONB Query Patterns
--------------------

.. code-block:: python

   # Query JSONB fields
   await cur.execute("""
       SELECT id, state->>'theme' as theme
       FROM adk_sessions
       WHERE state @> %s::jsonb
   """, (Jsonb({"preferences": {"theme": "dark"}}),))

   # JSONB containment
   await cur.execute("""
       SELECT * FROM adk_sessions
       WHERE state @> %s::jsonb
   """, (Jsonb({"active": True}),))

   # JSONB path queries
   await cur.execute("""
       SELECT * FROM adk_sessions
       WHERE state #> '{preferences,theme}' = %s
   """, ("dark",))

Connection Pool Sizing
----------------------

.. code-block:: python

   # For web applications
   config = PsycopgAsyncConfig(
       connection_config={
           "min_size": 10,          # Match expected concurrent requests
           "max_size": 50,          # 2-3x min_size for burst traffic
           "max_lifetime": 3600.0,  # Recycle hourly
       }
   )

   # For background workers
   config = PsycopgAsyncConfig(
       connection_config={
           "min_size": 2,
           "max_size": 10,
       }
   )

Comparison to Other PostgreSQL Drivers
=======================================

Psycopg3 vs AsyncPG
-------------------

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Feature
     - Psycopg3
     - AsyncPG
   * - Async/Sync Support
     - Both native
     - Async only
   * - JSONB Handling
     - Explicit ``Jsonb()`` wrapper
     - Direct dict insertion
   * - Parameter Style
     - ``%s`` (pyformat)
     - ``$1, $2`` (numeric)
   * - SQL Composition
     - ``pg_sql.SQL()``
     - Manual string composition
   * - Performance
     - Very fast (binary protocol)
     - Very fast (Cython-optimized)
   * - Type Safety
     - Explicit, safer
     - Implicit, convenient
   * - Cursor Model
     - Context managers required
     - Direct cursor usage

Psycopg3 vs Psqlpy
-------------------

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Feature
     - Psycopg3
     - Psqlpy
   * - Implementation
     - Python + C extensions
     - Rust-based
   * - Maturity
     - Stable, production-ready
     - Evolving
   * - JSONB Handling
     - ``Jsonb()`` wrapper
     - Direct dict insertion
   * - Parameter Style
     - ``%s`` (pyformat)
     - ``$1, $2`` (numeric)
   * - Ecosystem
     - Large, mature
     - Growing
   * - Performance
     - Very fast
     - Extremely fast

Troubleshooting
===============

Jsonb() Wrapper Errors
----------------------

**Error:**

.. code-block:: text

   psycopg.errors.UndefinedFunction: operator does not exist: jsonb = record

**Solution:** Wrap dicts with ``Jsonb()``:

.. code-block:: python

   from psycopg.types.json import Jsonb

   # WRONG
   await cur.execute("INSERT INTO table (data) VALUES (%s)", ({"key": "value"},))

   # CORRECT
   await cur.execute("INSERT INTO table (data) VALUES (%s)", (Jsonb({"key": "value"}),))

SQL Composition Errors
----------------------

**Error:**

.. code-block:: text

   psycopg.sql.Composable object is not iterable

**Solution:** Format SQL before execution:

.. code-block:: python

   from psycopg import sql as pg_sql

   # WRONG - Missing .format()
   query = pg_sql.SQL("SELECT * FROM {table}").format(table=pg_sql.Identifier("users"))
   await cur.execute(query)  # Already formatted!

   # CORRECT
   query = pg_sql.SQL("SELECT * FROM {table}").format(table=pg_sql.Identifier("users"))
   await cur.execute(query, ())  # No need to format again

Parameter Style Confusion
--------------------------

**Error:** Using wrong parameter placeholders:

.. code-block:: python

   # WRONG - PostgreSQL numeric style (that's asyncpg!)
   await cur.execute("SELECT * FROM users WHERE id = $1", (123,))

   # CORRECT - Psycopg uses %s
   await cur.execute("SELECT * FROM users WHERE id = %s", (123,))

Connection Pool Not Opening
----------------------------

**Error:**

.. code-block:: text

   pool is not open

**Solution:** Ensure async pool is opened:

.. code-block:: python

   # Pool is automatically opened by config
   async with config.provide_connection() as conn:
       # This works
       pass

   # Or manually if using pool directly
   pool = AsyncConnectionPool(conninfo, open=False)
   await pool.open()

Cursor Not Found
----------------

**Error:**

.. code-block:: text

   cursor does not exist

**Solution:** Use context managers for cursors:

.. code-block:: python

   # WRONG - Cursor closed prematurely
   conn = await config.create_connection()
   cur = await conn.cursor()
   await cur.execute(query)
   # cur is closed here

   # CORRECT - Use context manager
   async with config.provide_connection() as conn:
       async with conn.cursor() as cur:
           await cur.execute(query)
           rows = await cur.fetchall()

Migration from Psycopg2
=======================

Key Differences
---------------

.. code-block:: python

   # Psycopg2 (old)
   import psycopg2
   conn = psycopg2.connect("dbname=test")
   cur = conn.cursor()
   cur.execute("SELECT * FROM table")

   # Psycopg3 (new) - Async
   import psycopg
   async with await psycopg.AsyncConnection.connect("dbname=test") as conn:
       async with conn.cursor() as cur:
           await cur.execute("SELECT * FROM table")

JSONB Handling Changes
----------------------

.. code-block:: python

   # Psycopg2
   import json
   cur.execute("INSERT INTO table (data) VALUES (%s)", (json.dumps({"key": "value"}),))

   # Psycopg3
   from psycopg.types.json import Jsonb
   await cur.execute("INSERT INTO table (data) VALUES (%s)", (Jsonb({"key": "value"}),))

Connection Pool Migration
--------------------------

.. code-block:: python

   # Psycopg2 (using psycopg2.pool)
   from psycopg2.pool import ThreadedConnectionPool
   pool = ThreadedConnectionPool(5, 20, dsn="...")

   # Psycopg3 (using psycopg_pool)
   from psycopg_pool import AsyncConnectionPool
   pool = AsyncConnectionPool("...", min_size=5, max_size=20)
   await pool.open()

API Reference
=============

.. autoclass:: sqlspec.adapters.psycopg.adk.PsycopgAsyncADKStore
   :members:
   :inherited-members:
   :show-inheritance:
   :no-index:

.. autoclass:: sqlspec.adapters.psycopg.adk.PsycopgSyncADKStore
   :members:
   :inherited-members:
   :show-inheritance:
   :no-index:

See Also
========

- :doc:`../quickstart` - Quick start guide
- :doc:`../adapters` - Adapter comparison
- :doc:`../schema` - Database schema details
- :doc:`/reference/adapters/psycopg` - SQLSpec Psycopg adapter reference
- `Psycopg3 Documentation <https://www.psycopg.org/psycopg3/docs/>`_ - Official documentation
- `Psycopg3 Basic Usage <https://www.psycopg.org/psycopg3/docs/basic/usage.html>`_ - Usage guide
- `PostgreSQL JSONB Functions <https://www.postgresql.org/docs/current/functions-json.html>`_ - JSONB operations
