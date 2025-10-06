===============
SQLite Backend
===============

Overview
========

SQLite is a zero-configuration, embedded SQL database engine that runs in the same process as your application. It's ideal for development, testing, embedded applications, and single-user scenarios where simplicity and portability are priorities.

**Key Features:**

- **Embedded Database**: No server setup required, single-file or in-memory
- **Zero Configuration**: Works out-of-the-box with Python's stdlib
- **Portable**: Single file makes backup and deployment trivial
- **ACID Transactions**: Reliable transaction support with WAL mode
- **Small Footprint**: Minimal resource usage
- **Cross-Platform**: Works identically on all platforms

**Ideal Use Cases:**

- Development and testing environments
- Embedded desktop applications
- Single-user AI agents
- Prototyping and demos
- Offline-first applications
- Local data storage with zero infrastructure

.. warning::

   **SQLite is optimized for embedded and single-user scenarios**, not high-concurrency 
   production deployments. For production AI agents with many simultaneous users, use 
   PostgreSQL or MySQL. SQLite excels at development, testing, and embedded use cases.

Installation
============

SQLite is built into Python's standard library - no additional installation needed!

.. code-block:: bash

   pip install sqlspec google-genai
   # SQLite support is included by default

Quick Start
===========

File-Based Database
-------------------

.. code-block:: python

   from sqlspec.adapters.sqlite import SqliteConfig
   from sqlspec.adapters.sqlite.adk import SqliteADKStore
   from sqlspec.extensions.adk import SQLSpecSessionService

   # Create file-based database
   config = SqliteConfig(pool_config={"database": "./agent_sessions.db"})

   store = SqliteADKStore(config)
   await store.create_tables()

   service = SQLSpecSessionService(store)

   # Create session
   session = await service.create_session(
       app_name="chatbot",
       user_id="user_123",
       state={"conversation_started": True}
   )

In-Memory Database (Testing)
-----------------------------

.. code-block:: python

   from sqlspec.adapters.sqlite import SqliteConfig
   from sqlspec.adapters.sqlite.adk import SqliteADKStore

   # Create in-memory database (ephemeral)
   config = SqliteConfig(pool_config={"database": ":memory:"})

   store = SqliteADKStore(config)
   await store.create_tables()

.. tip::

   In-memory databases are perfect for unit tests and ephemeral workloads.
   All data is lost when the process exits.

Configuration
=============

Basic Configuration
-------------------

.. code-block:: python

   from sqlspec.adapters.sqlite import SqliteConfig

   config = SqliteConfig(
       pool_config={
           "database": "/path/to/database.db",  # or ":memory:"
           "timeout": 5.0,  # Lock timeout in seconds
           "check_same_thread": False,  # Allow multi-threaded access
           "isolation_level": None,  # Autocommit mode
       }
   )

WAL Mode (Recommended)
----------------------

Write-Ahead Logging (WAL) mode significantly improves concurrency:

.. code-block:: python

   from sqlspec.adapters.sqlite import SqliteConfig

   config = SqliteConfig(
       pool_config={
           "database": "./agent.db",
           "check_same_thread": False,
       }
   )

   # Enable WAL mode after table creation
   with config.provide_connection() as conn:
       conn.execute("PRAGMA journal_mode=WAL")
       conn.execute("PRAGMA foreign_keys=ON")
       conn.commit()

.. note::

   WAL mode benefits:
   
   - Readers don't block writers
   - Writers don't block readers  
   - Better concurrency than default rollback journal
   - Faster in most cases

Custom Table Names
------------------

.. code-block:: python

   store = SqliteADKStore(
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
       id TEXT PRIMARY KEY,
       app_name TEXT NOT NULL,
       user_id TEXT NOT NULL,
       state TEXT NOT NULL DEFAULT '{}',  -- JSON as TEXT
       create_time REAL NOT NULL,  -- Julian Day number
       update_time REAL NOT NULL   -- Julian Day number
   );

   CREATE INDEX IF NOT EXISTS idx_adk_sessions_app_user
       ON adk_sessions(app_name, user_id);

   CREATE INDEX IF NOT EXISTS idx_adk_sessions_update_time
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
       actions BLOB NOT NULL,  -- Pickled actions from ADK
       long_running_tool_ids_json TEXT,
       branch TEXT,
       timestamp REAL NOT NULL,  -- Julian Day number
       content TEXT,  -- JSON as TEXT
       grounding_metadata TEXT,  -- JSON as TEXT
       custom_metadata TEXT,  -- JSON as TEXT
       partial INTEGER,  -- Boolean as INTEGER (0/1/NULL)
       turn_complete INTEGER,
       interrupted INTEGER,
       error_code TEXT,
       error_message TEXT,
       FOREIGN KEY (session_id) REFERENCES adk_sessions(id) ON DELETE CASCADE
   );

   CREATE INDEX IF NOT EXISTS idx_adk_events_session
       ON adk_events(session_id, timestamp ASC);

**SQLite Type Mappings:**

- **JSON**: Stored as ``TEXT`` using SQLSpec serializers (msgspec/orjson/stdlib)
- **Boolean**: Stored as ``INTEGER`` (0=False, 1=True, NULL=None)
- **Timestamps**: Stored as ``REAL`` (Julian Day number for efficient date operations)
- **Binary**: Stored as ``BLOB`` (pickled actions from Google ADK)
- **Strings**: Stored as ``TEXT``

.. note::

   **Julian Day Numbers**: SQLite stores timestamps as Julian Day numbers (REAL type),
   which represents days since November 24, 4714 BCE. This enables direct comparison
   with SQLite's ``julianday('now')`` function and efficient date operations.

Usage Patterns
==============

Synchronous Driver with Async Wrapper
--------------------------------------

SQLSpec's SQLite adapter uses Python's synchronous ``sqlite3`` driver wrapped with
``async_`` utility from Litestar's ``sync_tools`` for async compatibility:

.. code-block:: python

   # Internally, SQLite operations run in thread pool
   session = await store.create_session(...)  # Wrapped sync operation
   events = await store.get_events(...)       # Wrapped sync operation

This approach:

- Uses battle-tested stdlib ``sqlite3`` driver
- Provides async interface for consistency with other adapters
- Runs SQLite operations in thread pool to avoid blocking event loop
- Maintains compatibility with async frameworks (Litestar, FastAPI, etc.)

JSON Serialization
------------------

SQLite doesn't have native JSON type. SQLSpec handles JSON serialization transparently:

.. code-block:: python

   # JSON automatically serialized to TEXT
   session = await store.create_session(
       session_id="s1",
       app_name="bot",
       user_id="user1",
       state={"key": "value", "nested": {"data": 123}}
   )

   # JSON automatically deserialized from TEXT
   retrieved = await store.get_session("s1")
   print(retrieved.state)  # {"key": "value", "nested": {"data": 123}}

.. tip::

   SQLSpec uses the best available JSON serializer:
   
   1. ``msgspec`` (fastest, if available)
   2. ``orjson`` (fast, if available)
   3. ``stdlib json`` (always available)

Foreign Key Constraints
-----------------------

SQLite requires foreign keys to be enabled per connection:

.. code-block:: python

   # Foreign keys enabled automatically by store
   with config.provide_connection() as conn:
       conn.execute("PRAGMA foreign_keys=ON")
       
       # Now cascade deletes work correctly
       await store.delete_session(session_id)  # Events auto-deleted

Parameter Style
---------------

SQLite uses ``?`` positional placeholders:

.. code-block:: python

   # Internally handled by SQLSpec
   cursor.execute(
       "SELECT * FROM adk_sessions WHERE app_name = ? AND user_id = ?",
       (app_name, user_id)
   )

Performance Considerations
==========================

Strengths
---------

- **Zero Configuration**: No server setup or connection pooling complexity
- **Small Footprint**: Minimal memory and disk usage
- **Fast Reads**: Excellent read performance for single-user scenarios
- **ACID Transactions**: Reliable with proper WAL mode configuration
- **Portability**: Single file makes backup and deployment trivial

Limitations
-----------

- **Single Writer**: Only one write transaction at a time (even with WAL mode)
- **Concurrency**: Limited support for concurrent writes
- **No Server**: Cannot scale across multiple processes/machines
- **Sync Driver**: Wrapped with async, adds thread pool overhead
- **Type Affinity**: Flexible typing can lead to type inconsistencies

Best Practices
==============

1. Enable WAL Mode for Concurrency
-----------------------------------

.. code-block:: python

   with config.provide_connection() as conn:
       conn.execute("PRAGMA journal_mode=WAL")
       conn.execute("PRAGMA synchronous=NORMAL")  # Balance safety/speed
       conn.commit()

2. Use Transactions for Bulk Operations
----------------------------------------

.. code-block:: python

   with config.provide_connection() as conn:
       conn.execute("BEGIN")
       try:
           for session_data in bulk_data:
               # Insert sessions
               ...
           conn.commit()
       except Exception:
           conn.rollback()
           raise

3. Regular Database Maintenance
--------------------------------

.. code-block:: python

   # Vacuum to reclaim space (periodic maintenance)
   with config.provide_connection() as conn:
       conn.execute("VACUUM")

   # Analyze for query optimization
   with config.provide_connection() as conn:
       conn.execute("ANALYZE")

4. Proper File Locations
-------------------------

.. code-block:: python

   from pathlib import Path

   # Good: Application data directory
   app_data = Path.home() / ".myagent" / "sessions.db"
   app_data.parent.mkdir(parents=True, exist_ok=True)
   config = SqliteConfig(pool_config={"database": str(app_data)})

   # Bad: Hard-coded paths
   config = SqliteConfig(pool_config={"database": "/tmp/sessions.db"})

5. Backup Strategy
------------------

.. code-block:: python

   import shutil
   from pathlib import Path

   # Simple file-based backup
   db_path = Path("./agent_sessions.db")
   backup_path = Path("./backups") / f"sessions_{datetime.now():%Y%m%d_%H%M%S}.db"
   backup_path.parent.mkdir(exist_ok=True)
   
   # Close connections before backup
   config.close()
   shutil.copy2(db_path, backup_path)

When to Use SQLite
==================

**Ideal For:**

✅ Development and testing environments
✅ Embedded desktop applications
✅ Single-user AI agents  
✅ Prototyping and demos
✅ Offline-first applications
✅ Learning and experimentation
✅ CI/CD test suites
✅ Local-first tools

**Graduate to PostgreSQL When:**

❌ Need high-concurrency production deployment
❌ Multiple simultaneous users writing data
❌ Require server-based architecture
❌ Need advanced indexing (GIN/GiST for JSON)
❌ Require full-text search capabilities
❌ Need replication or clustering

Comparison: SQLite vs Other Databases
--------------------------------------

.. list-table::
   :header-rows: 1
   :widths: 25 25 25 25

   * - Feature
     - SQLite
     - AIOSQLite
     - PostgreSQL
   * - Setup Complexity
     - Zero config
     - Zero config
     - Server required
   * - Driver Type
     - Sync (wrapped)
     - Native async
     - Native async
   * - Concurrent Writes
     - Single writer
     - Single writer
     - Excellent
   * - JSON Support
     - TEXT + serializers
     - TEXT + serializers
     - Native JSONB
   * - Deployment
     - Single file
     - Single file
     - Client-server
   * - Best Use Case
     - Development, embedded
     - Async apps, testing
     - Production agents

Use Cases
=========

Development Environment
-----------------------

SQLite's zero-configuration makes it perfect for rapid development:

.. code-block:: python

   # Quick setup - no database server needed!
   config = SqliteConfig(pool_config={"database": ":memory:"})
   store = SqliteADKStore(config)
   await store.create_tables()

   service = SQLSpecSessionService(store)
   session = await service.create_session("dev_app", "dev_user", {})

Embedded Desktop Application
-----------------------------

Store agent sessions locally in desktop apps:

.. code-block:: python

   from pathlib import Path

   # Store in user's application data directory
   app_data = Path.home() / ".my_agent" / "sessions.db"
   app_data.parent.mkdir(parents=True, exist_ok=True)

   config = SqliteConfig(pool_config={"database": str(app_data)})
   store = SqliteADKStore(config)
   await store.create_tables()

   # Enable WAL for better UI responsiveness
   with config.provide_connection() as conn:
       conn.execute("PRAGMA journal_mode=WAL")

Unit Testing
------------

In-memory databases for fast, isolated tests:

.. code-block:: python

   import pytest
   from sqlspec.adapters.sqlite import SqliteConfig
   from sqlspec.adapters.sqlite.adk import SqliteADKStore

   @pytest.fixture
   async def test_store():
       """Provide fresh in-memory store for each test."""
       config = SqliteConfig(pool_config={"database": ":memory:"})
       store = SqliteADKStore(config)
       await store.create_tables()
       yield store
       # Cleanup automatic (in-memory)

   async def test_session_creation(test_store):
       session = await test_store.create_session(
           "s1", "app", "user", {"test": True}
       )
       assert session.id == "s1"
       assert session.state["test"] is True

Troubleshooting
===============

Database Locked Errors
----------------------

**Symptom:**

.. code-block:: text

   sqlite3.OperationalError: database is locked

**Solutions:**

1. **Enable WAL mode** (readers don't block writers):

.. code-block:: python

   with config.provide_connection() as conn:
       conn.execute("PRAGMA journal_mode=WAL")

2. **Increase timeout**:

.. code-block:: python

   config = SqliteConfig(pool_config={
       "database": "./agent.db",
       "timeout": 30.0  # Wait up to 30 seconds for locks
   })

3. **Ensure proper transaction handling**:

.. code-block:: python

   # Good: Explicit transaction scope
   with config.provide_connection() as conn:
       conn.execute("BEGIN")
       try:
           # ... operations ...
           conn.commit()
       except Exception:
           conn.rollback()
           raise

File Permission Errors
----------------------

**Symptom:**

.. code-block:: text

   sqlite3.OperationalError: unable to open database file

**Solutions:**

1. **Ensure directory exists**:

.. code-block:: python

   from pathlib import Path
   
   db_path = Path("./data/agent.db")
   db_path.parent.mkdir(parents=True, exist_ok=True)
   config = SqliteConfig(pool_config={"database": str(db_path)})

2. **Check write permissions**:

.. code-block:: bash

   # Ensure user has write access to database directory
   chmod 755 /path/to/database/directory
   chmod 644 /path/to/database/file.db

Foreign Key Constraint Violations
----------------------------------

**Symptom:**

.. code-block:: text

   sqlite3.IntegrityError: FOREIGN KEY constraint failed

**Solution:**

Ensure foreign keys are enabled:

.. code-block:: python

   # Foreign keys enabled automatically by SqliteADKStore
   # But verify if using custom queries:
   with config.provide_connection() as conn:
       result = conn.execute("PRAGMA foreign_keys").fetchone()
       print(f"Foreign keys: {result[0]}")  # Should be 1

Migration to Production Database
=================================

When ready for production, migrate from SQLite to PostgreSQL:

.. code-block:: python

   # Export from SQLite
   from sqlspec.adapters.sqlite import SqliteConfig
   from sqlspec.adapters.sqlite.adk import SqliteADKStore

   sqlite_config = SqliteConfig(pool_config={"database": "./dev.db"})
   sqlite_store = SqliteADKStore(sqlite_config)

   # Get all sessions
   sessions = await sqlite_store.list_sessions("app_name", "user_id")

   # Import to PostgreSQL
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore

   pg_config = AsyncpgConfig(pool_config={"dsn": "postgresql://..."})
   pg_store = AsyncpgADKStore(pg_config)
   await pg_store.create_tables()

   # Migrate sessions
   for session in sessions:
       await pg_store.create_session(
           session_id=session.id,
           app_name=session.app_name,
           user_id=session.user_id,
           state=session.state
       )
       
       # Migrate events
       events = await sqlite_store.get_events(session.id)
       for event in events:
           await pg_store.append_event(event)

Example: Full Application
==========================

Complete runnable example demonstrating SQLite ADK integration:

.. code-block:: python

   """Example: Google ADK session storage with SQLite.
   
   SQLite is perfect for:
   - Development and testing (zero-configuration)
   - Embedded applications
   - Single-user AI agents
   - Prototyping
   
   Requirements:
       - pip install sqlspec google-genai
   """

   from datetime import datetime, timezone
   from pathlib import Path

   from google.adk.events.event import Event
   from google.genai import types

   from sqlspec.adapters.sqlite import SqliteConfig
   from sqlspec.adapters.sqlite.adk import SqliteADKStore
   from sqlspec.extensions.adk import SQLSpecSessionService


   async def main():
       """Demonstrate SQLite ADK session storage."""
       # File-based database
       db_path = Path("./agent_sessions.db")
       config = SqliteConfig(pool_config={"database": str(db_path)})

       store = SqliteADKStore(config)
       await store.create_tables()
       print(f"✅ Created ADK tables in SQLite: {db_path}")

       # Enable WAL mode for better concurrency
       with config.provide_connection() as conn:
           conn.execute("PRAGMA journal_mode=WAL")
           conn.execute("PRAGMA foreign_keys=ON")
           conn.commit()

       service = SQLSpecSessionService(store)

       # Create session
       session = await service.create_session(
           app_name="chatbot",
           user_id="user_123",
           state={"conversation_started": True}
       )
       print(f"\n📝 Created session: {session.id}")

       # Add events
       user_event = Event(
           id="evt_user_1",
           invocation_id="inv_1",
           author="user",
           branch="main",
           actions=[],
           timestamp=datetime.now(timezone.utc).timestamp(),
           content=types.Content(parts=[types.Part(text="Hello!")]),
           partial=False,
           turn_complete=True,
       )
       await service.append_event(session, user_event)
       print(f"✅ Added user event: {user_event.id}")

       # Retrieve session with events
       retrieved = await service.get_session(
           app_name="chatbot",
           user_id="user_123", 
           session_id=session.id
       )
       print(f"\n📥 Retrieved session with {len(retrieved.events)} events")

       # Cleanup
       await service.delete_session(session.id)
       print(f"\n🗑️  Deleted session: {session.id}")

       if db_path.exists():
           db_path.unlink()
           print(f"🧹 Cleaned up database: {db_path}")


   if __name__ == "__main__":
       import asyncio
       asyncio.run(main())

API Reference
=============

.. autoclass:: sqlspec.adapters.sqlite.adk.SqliteADKStore
   :members:
   :inherited-members:
   :show-inheritance:

See Also
========

- :doc:`../quickstart` - Quick start guide
- :doc:`../adapters` - Adapter comparison
- :doc:`../schema` - Database schema details
- :doc:`duckdb` - DuckDB backend (OLAP alternative)
- :doc:`adbc` - ADBC backend (multi-database)
- `SQLite Documentation <https://www.sqlite.org/docs.html>`_ - Official SQLite docs
- `SQLite WAL Mode <https://www.sqlite.org/wal.html>`_ - Write-Ahead Logging explained
