==================
AIOSQLite Backend
==================

Overview
========

AIOSQLite is an asynchronous wrapper for SQLite that runs operations in a thread pool executor, providing native async/await support for Python's built-in SQLite database.

**Key Features:**

- **Native Async Support**: True async/await interface via aiosqlite
- **Zero Configuration**: Embedded database with no server setup
- **Thread Pool Executor**: Runs SQLite operations in background threads
- **Same SQLite Features**: Full access to all SQLite capabilities
- **File-Based or In-Memory**: Flexible storage options
- **ACID Transactions**: Reliable transaction support
- **WAL Mode**: Better concurrency with Write-Ahead Logging

.. warning::

   **SQLite has single-writer limitations**. While aiosqlite provides async access,
   SQLite itself only supports one write transaction at a time.

Installation
============

Install SQLSpec with AIOSQLite support:

.. code-block:: bash

   pip install sqlspec[aiosqlite] google-genai
   # or
   uv pip install sqlspec[aiosqlite] google-genai

.. note::

   AIOSQLite is included with SQLSpec's async extras. No additional database server needed!

Quick Start
===========

Basic Async File-Based Database
--------------------------------

.. code-block:: python

   import asyncio
   from sqlspec.adapters.aiosqlite import AiosqliteConfig
   from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore
   from sqlspec.extensions.adk import SQLSpecSessionService

   async def main():
       # Create async file-based database
       config = AiosqliteConfig(connection_config={"database": "./agent_sessions.db"})

       store = AiosqliteADKStore(config)
       await store.create_tables()

       service = SQLSpecSessionService(store)

       # Create session with async/await
       session = await service.create_session(
           app_name="async_chatbot",
           user_id="user_123",
           state={"mode": "conversational"}
       )
       print(f"Created session: {session['id']}")

   asyncio.run(main())

Async In-Memory Database (Testing)
-----------------------------------

.. code-block:: python

   import asyncio
   from sqlspec.adapters.aiosqlite import AiosqliteConfig
   from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore

   async def test_setup():
       # Create async in-memory database
       config = AiosqliteConfig(connection_config={"database": ":memory:"})

       store = AiosqliteADKStore(config)
       await store.create_tables()

       # Perfect for async tests!
       return store

   asyncio.run(test_setup())

.. tip::

   In-memory databases are excellent for async unit tests and ephemeral workloads.
   Use shared memory mode (``file::memory:?cache=shared``) to share across connections.

Configuration
=============

Basic Configuration
-------------------

.. code-block:: python

   from sqlspec.adapters.aiosqlite import AiosqliteConfig

   config = AiosqliteConfig(
       connection_config={
           "database": "/path/to/database.db",  # or ":memory:"
           "timeout": 5.0,                      # Connection timeout
           "isolation_level": "DEFERRED",       # Transaction isolation
           "check_same_thread": False,          # Allow multi-thread (safe with aiosqlite)
           "uri": True,                         # Enable URI mode for advanced features
       }
   )

.. note::

   AIOSQLite automatically converts ``:memory:`` to ``file::memory:?cache=shared``
   for better connection sharing in async environments.

Connection Pooling
------------------

.. code-block:: python

   config = AiosqliteConfig(
       connection_config={
           "database": "./sessions.db",
           "pool_size": 5,              # Connection pool size
           "connect_timeout": 30.0,     # Pool acquire timeout
           "idle_timeout": 3600.0,      # Idle connection timeout
           "operation_timeout": 10.0,   # Per-operation timeout
       }
   )

Custom Table Names
------------------

.. code-block:: python

   store = AiosqliteADKStore(
       config,
       session_table="chatbot_sessions",
       events_table="chatbot_events"
   )

Schema
======

The AIOSQLite ADK store uses the same schema as the sync SQLite adapter, optimized for SQLite's capabilities.

Sessions Table
--------------

.. code-block:: sql

   CREATE TABLE IF NOT EXISTS adk_sessions (
       id TEXT PRIMARY KEY,
       app_name TEXT NOT NULL,
       user_id TEXT NOT NULL,
       state TEXT NOT NULL DEFAULT '{}',      -- JSON as TEXT
       create_time REAL NOT NULL,             -- Julian Day number
       update_time REAL NOT NULL              -- Julian Day number
   );

   CREATE INDEX idx_adk_sessions_app_user
       ON adk_sessions(app_name, user_id);

   CREATE INDEX idx_adk_sessions_update_time
       ON adk_sessions(update_time DESC);

Events Table
------------

.. code-block:: sql

   CREATE TABLE IF NOT EXISTS adk_events (
       id TEXT PRIMARY KEY,
       session_id TEXT NOT NULL,
       app_name TEXT NOT NULL,
       user_id TEXT NOT NULL,
       invocation_id TEXT NOT NULL,
       author TEXT NOT NULL,
       actions BLOB NOT NULL,                  -- Pickled actions
       long_running_tool_ids_json TEXT,
       branch TEXT,
       timestamp REAL NOT NULL,                -- Julian Day number
       content TEXT,                           -- JSON as TEXT
       grounding_metadata TEXT,                -- JSON as TEXT
       custom_metadata TEXT,                   -- JSON as TEXT
       partial INTEGER,                        -- Boolean as 0/1/NULL
       turn_complete INTEGER,                  -- Boolean as 0/1/NULL
       interrupted INTEGER,                    -- Boolean as 0/1/NULL
       error_code TEXT,
       error_message TEXT,
       FOREIGN KEY (session_id) REFERENCES adk_sessions(id) ON DELETE CASCADE
   );

   CREATE INDEX idx_adk_events_session
       ON adk_events(session_id, timestamp ASC);

.. note::

   **SQLite Data Type Mapping:**

   - **TEXT**: Strings, JSON (via ``to_json``/``from_json``)
   - **REAL**: Julian Day timestamps (efficient date arithmetic)
   - **INTEGER**: Booleans (0=False, 1=True, NULL=None)
   - **BLOB**: Binary data (pickled actions from Google ADK)

   Foreign key constraints are enabled per connection via ``PRAGMA foreign_keys = ON``.

Usage Patterns
==============

Async Context Managers
-----------------------

.. code-block:: python

   from sqlspec.adapters.aiosqlite import AiosqliteConfig
   from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore

   async def use_store():
       config = AiosqliteConfig(connection_config={"database": ":memory:"})

       # Async context manager for connections
       async with config.provide_connection() as conn:
           await conn.execute("PRAGMA journal_mode=WAL")
           await conn.commit()

       # Or use the store directly
       store = AiosqliteADKStore(config)
       await store.create_tables()

Native Async/Await Operations
------------------------------

.. code-block:: python

   from datetime import datetime, timezone

   # All store operations are native async
   session = await store.create_session(
       session_id="session_123",
       app_name="assistant",
       user_id="user_456",
       state={"context": "active"}
   )

   # Retrieve with await
   retrieved = await store.get_session("session_123")

   # Update session state
   await store.update_session_state(
       "session_123",
       {"context": "active", "last_query": "What's the weather?"}
   )

   # List sessions
   sessions = await store.list_sessions("assistant", "user_456")

   # Get events with filtering
   recent_events = await store.get_events(
       session_id="session_123",
       after_timestamp=datetime.now(timezone.utc),
       limit=50
   )

   # Delete session (cascade deletes events)
   await store.delete_session("session_123")

JSON Serialization
------------------

.. code-block:: python

   # JSON is stored as TEXT using SQLSpec serializers
   state = {
       "preferences": {"theme": "dark", "language": "en"},
       "conversation_mode": "chat",
       "tools_enabled": ["web_search", "calculator"]
   }

   session = await store.create_session(
       session_id="sess_1",
       app_name="app",
       user_id="user",
       state=state
   )

   # Retrieved state is automatically deserialized
   retrieved = await store.get_session("sess_1")
   print(retrieved["state"]["preferences"]["theme"])  # "dark"

.. note::

   SQLSpec uses the best available JSON serializer: msgspec > orjson > stdlib json.
   All JSON serialization is handled transparently via ``to_json``/``from_json``.

Performance Considerations
==========================

Thread Pool Executor Model
---------------------------

AIOSQLite runs SQLite operations in a thread pool to provide async access:

**Implications:**

- **Thread switching overhead**: Each operation switches to a thread pool
- **Batch operations recommended**: Use ``executemany()`` for bulk inserts
- **Not true parallelism**: SQLite's single-writer model still applies

**Best Practices:**

.. code-block:: python

   # BAD: Many individual async operations
   for event in events:
       await store.append_event(event)

   # BETTER: Batch when possible (consider implementing executemany)
   # Or accept the overhead for simplicity in low-frequency scenarios

WAL Mode for Better Concurrency
--------------------------------

**Enable Write-Ahead Logging (WAL) mode** for better concurrent read/write performance:

.. code-block:: python

   async with config.provide_connection() as conn:
       await conn.execute("PRAGMA journal_mode=WAL")
       await conn.commit()

**Benefits:**

- Multiple readers can access database while writer is active
- Better performance for read-heavy workloads
- Reduced lock contention

**Trade-offs:**

- Slightly more disk I/O
- Requires file system that supports WAL (supported on most systems)

Performance Tuning PRAGMAs
---------------------------

.. code-block:: python

   async with config.provide_connection() as conn:
       # Enable WAL mode (recommended)
       await conn.execute("PRAGMA journal_mode=WAL")

       # Faster synchronization (less durable, but faster)
       await conn.execute("PRAGMA synchronous=NORMAL")

       # Increase cache size (64MB)
       await conn.execute("PRAGMA cache_size=-64000")

       # Memory-mapped I/O (256MB)
       await conn.execute("PRAGMA mmap_size=268435456")

       await conn.commit()

.. warning::

   ``PRAGMA synchronous=NORMAL`` trades durability for performance.
   Only use in development or with acceptable data loss risk.

Comparison: AIOSQLite vs Other Backends
----------------------------------------

.. list-table::
   :header-rows: 1
   :widths: 20 25 25 30

   * - Feature
     - AIOSQLite
     - SQLite (sync)
     - AsyncPG (PostgreSQL)
   * - Setup Complexity
     - Zero config, embedded
     - Zero config, embedded
     - Requires PostgreSQL server
   * - Async Support
     - Native async/await
     - Sync with async wrapper
     - Native async/await
   * - Concurrent Writes
     - Single writer (SQLite limit)
     - Single writer (SQLite limit)
     - Excellent multi-writer
   * - Thread Pool
     - Yes (aiosqlite executor)
     - No (direct calls)
     - No (native async I/O)
   * - Performance
     - Good for moderate async
     - Better for sync-only apps
     - Best for high concurrency
   * - Deployment
     - Single file
     - Single file
     - Client-server

Example: Full Application
==========================

See the complete runnable example:

.. literalinclude:: ../../examples/extensions/adk/basic_aiosqlite.py
   :language: python

This example demonstrates:

- Async/await throughout the application
- File-based and in-memory configurations
- Session and event management
- Proper async cleanup
- WAL mode configuration

Troubleshooting
===============

Database Locked Errors
-----------------------

**Error:**

.. code-block:: text

   sqlite3.OperationalError: database is locked

**Causes:**

- Multiple writers attempting simultaneous writes
- Long-running transactions holding locks
- Lack of WAL mode in concurrent scenarios

**Solutions:**

.. code-block:: python

   # 1. Enable WAL mode (most important!)
   async with config.provide_connection() as conn:
       await conn.execute("PRAGMA journal_mode=WAL")
       await conn.commit()

   # 2. Increase timeout
   config = AiosqliteConfig(
       connection_config={"database": "./db.sqlite", "timeout": 30.0}
   )

   # 3. Use transaction batching (reduce write frequency)

Async Context Manager Issues
-----------------------------

**Error:**

.. code-block:: text

   RuntimeError: Event loop is closed

**Solution:**

Ensure you're using ``asyncio.run()`` or managing the event loop properly:

.. code-block:: python

   import asyncio

   async def main():
       config = AiosqliteConfig(connection_config={"database": ":memory:"})
       store = AiosqliteADKStore(config)
       await store.create_tables()

   # Correct
   asyncio.run(main())

   # Incorrect (creates/closes loop improperly)
   loop = asyncio.get_event_loop()
   loop.run_until_complete(main())
   loop.close()  # May cause issues

Thread Safety Concerns
-----------------------

**Issue:** SQLite ``check_same_thread`` restriction

**Solution:** AIOSQLite handles this automatically via thread pool executor:

.. code-block:: python

   # check_same_thread=False is safe with aiosqlite
   config = AiosqliteConfig(
       connection_config={
           "database": "./db.sqlite",
           "check_same_thread": False  # Safe with aiosqlite
       }
   )

Foreign Key Not Enforced
-------------------------

**Issue:** Foreign key constraints not working

**Solution:** The store automatically enables foreign keys per connection:

.. code-block:: python

   # Handled automatically by AiosqliteADKStore
   await conn.execute("PRAGMA foreign_keys = ON")

   # Verify foreign keys are enabled
   async with config.provide_connection() as conn:
       cursor = await conn.execute("PRAGMA foreign_keys")
       result = await cursor.fetchone()
       print(f"Foreign keys enabled: {result[0]}")  # Should be 1

Best Practices
==============

Enable WAL Mode Early
----------------------

.. code-block:: python

   async def initialize_database(config: AiosqliteConfig):
       """Initialize database with optimal settings."""
       async with config.provide_connection() as conn:
           await conn.execute("PRAGMA journal_mode=WAL")
           await conn.execute("PRAGMA synchronous=NORMAL")
           await conn.execute("PRAGMA cache_size=-64000")
           await conn.commit()

       store = AiosqliteADKStore(config)
       await store.create_tables()
       return store

Use Connection Pooling
-----------------------

.. code-block:: python

   # Good: Reuse connection pool
   config = AiosqliteConfig(connection_config={"database": "./db.sqlite", "pool_size": 5})
   store = AiosqliteADKStore(config)

   # All operations use the pool
   await store.create_session(...)
   await store.get_session(...)

Avoid Long-Running Transactions
--------------------------------

.. code-block:: python

   # BAD: Long transaction blocks other writers
   async with config.provide_connection() as conn:
       await conn.execute("BEGIN")
       # ... many operations ...
       await asyncio.sleep(10)  # Holding lock!
       await conn.commit()

   # GOOD: Short, focused transactions
   async with config.provide_connection() as conn:
       await conn.execute("BEGIN")
       await conn.execute(...)
       await conn.execute(...)
       await conn.commit()  # Quick commit

Graceful Cleanup
----------------

.. code-block:: python

   async def application_lifecycle():
       config = AiosqliteConfig(connection_config={"database": "./db.sqlite"})
       store = AiosqliteADKStore(config)
       await store.create_tables()

       try:
           # Application logic
           yield store
       finally:
           # Clean up connection pool
           await config.close_pool()

Migration from Sync SQLite
===========================

Migrating from sync SQLite to AIOSQLite is straightforward:

.. code-block:: python

   # Before: Sync SQLite
   from sqlspec.adapters.sqlite import SqliteConfig
   from sqlspec.adapters.sqlite.adk import SqliteADKStore

   def sync_app():
       config = SqliteConfig(database="./db.sqlite")
       store = SqliteADKStore(config)
       # ... sync operations ...

   # After: Async AIOSQLite
   from sqlspec.adapters.aiosqlite import AiosqliteConfig
   from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore

   async def async_app():
       config = AiosqliteConfig(connection_config={"database": "./db.sqlite"})
       store = AiosqliteADKStore(config)
       # ... async operations with await ...

**Key Changes:**

1. Import from ``aiosqlite`` instead of ``sqlite``
2. Add ``async``/``await`` keywords
3. Use ``connection_config`` parameter (not direct kwargs)
4. Use ``asyncio.run()`` to execute

API Reference
=============

.. autoclass:: sqlspec.adapters.aiosqlite.adk.AiosqliteADKStore
   :members:
   :inherited-members:
   :show-inheritance:
   :no-index:

See Also
========

- :doc:`../quickstart` - Quick start guide
- :doc:`../adapters` - Adapter comparison
- :doc:`../schema` - Database schema details
- :doc:`sqlite` - Sync SQLite backend (comparison)
- :doc:`duckdb` - DuckDB backend (embedded OLAP alternative)
- `AIOSQLite Documentation <https://aiosqlite.omnilib.dev/>`_ - Official aiosqlite documentation
- `SQLite Documentation <https://www.sqlite.org/docs.html>`_ - SQLite reference
- `SQLite WAL Mode <https://www.sqlite.org/wal.html>`_ - Write-Ahead Logging explained
