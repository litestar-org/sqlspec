================
AsyncPG Backend
================

Overview
========

AsyncPG is a high-performance, async-native PostgreSQL driver for Python, written in Cython for exceptional speed. It is the **fastest Python PostgreSQL driver** and the recommended choice for production async AI agent deployments.

**Key Features:**

- **Blazing Fast**: Written in Cython, 3-5x faster than other PostgreSQL drivers
- **Native Async**: Pure asyncio implementation, no thread pool overhead
- **Connection Pooling**: Built-in sophisticated connection pool management
- **Native JSONB**: Direct dict to/from JSONB conversion without manual serialization
- **Prepared Statements**: Automatic statement preparation and caching
- **Microsecond Precision**: TIMESTAMPTZ with microsecond-level accuracy
- **Type Safety**: Rich PostgreSQL type support (arrays, composite types, UUIDs)

**Ideal Use Cases:**

- Production AI agents with high-concurrency async workloads
- Real-time conversational AI requiring fast response times
- Multi-user agent platforms with thousands of concurrent sessions
- Applications requiring maximum PostgreSQL performance
- Async web frameworks (Litestar, FastAPI, Starlette)

.. tip::

   **Performance Benchmark**: AsyncPG can handle 10,000+ queries per second in typical workloads,
   making it ideal for production AI agent applications where response time is critical.

Installation
============

Install SQLSpec with AsyncPG support:

.. code-block:: bash

   pip install sqlspec[asyncpg] google-genai
   # or
   uv pip install sqlspec[asyncpg] google-genai

PostgreSQL Server Setup
-----------------------

AsyncPG requires a PostgreSQL server (version 10+):

**Docker (Development):**

.. code-block:: bash

   docker run --name postgres-adk \
     -e POSTGRES_PASSWORD=secret \
     -e POSTGRES_DB=agentdb \
     -p 5432:5432 \
     -d postgres:16

**Production Setup:**

- **Managed Services**: AWS RDS, Google Cloud SQL, Azure Database for PostgreSQL
- **Self-Hosted**: PostgreSQL 14+ with connection pooling (PgBouncer recommended)
- **Configuration**: Tune ``max_connections``, ``shared_buffers``, ``work_mem`` for workload

Quick Start
===========

Basic Configuration
-------------------

.. code-block:: python

   import asyncio
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore
   from sqlspec.extensions.adk import SQLSpecSessionService

   async def main():
       # Create configuration with connection pool
       config = AsyncpgConfig(
           pool_config={
               "dsn": "postgresql://user:password@localhost:5432/agentdb",
               "min_size": 5,
               "max_size": 20,
               "command_timeout": 60.0,
           }
       )

       # Initialize store and create tables
       store = AsyncpgADKStore(config)
       await store.create_tables()

       # Create service for session management
       service = SQLSpecSessionService(store)

       # Create session
       session = await service.create_session(
           app_name="assistant_bot",
           user_id="user_123",
           state={"conversation_context": "greeting", "language": "en"}
       )
       print(f"Created session: {session.id}")

   asyncio.run(main())

Connection String Formats
-------------------------

AsyncPG supports multiple connection string formats:

.. code-block:: python

   # Full DSN
   config = AsyncpgConfig(pool_config={
       "dsn": "postgresql://user:password@host:5432/database"
   })

   # Individual parameters
   config = AsyncpgConfig(pool_config={
       "host": "localhost",
       "port": 5432,
       "user": "agent_user",
       "password": "secure_password",
       "database": "agentdb"
   })

   # With SSL
   config = AsyncpgConfig(pool_config={
       "dsn": "postgresql://user:pass@host:5432/db?sslmode=require"
   })

Configuration
=============

Connection Pool Configuration
------------------------------

AsyncPG's built-in connection pool is highly configurable:

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig

   config = AsyncpgConfig(
       pool_config={
           # Connection parameters
           "dsn": "postgresql://localhost/agentdb",
           "user": "agent_user",
           "password": "secure_password",

           # Pool sizing
           "min_size": 5,              # Minimum connections (default: 10)
           "max_size": 20,             # Maximum connections (default: 10)

           # Connection lifecycle
           "max_queries": 50000,       # Reconnect after N queries (default: 50000)
           "max_inactive_connection_lifetime": 300.0,  # Close idle after 5min

           # Timeouts
           "command_timeout": 60.0,    # Query timeout in seconds
           "connect_timeout": 10.0,    # Connection timeout

           # Statement caching
           "statement_cache_size": 100,             # LRU cache size (default: 100)
           "max_cached_statement_lifetime": 300,    # Cache lifetime in seconds
           "max_cacheable_statement_size": 1024*15, # Max statement size to cache

           # SSL configuration
           "ssl": "require",           # or ssl.SSLContext object

           # Server settings
           "server_settings": {
               "jit": "off",           # Disable JIT compilation if needed
               "application_name": "ai_agent"
           }
       }
   )

Pool Sizing Guidelines
----------------------

Choose pool size based on your workload:

.. list-table::
   :header-rows: 1
   :widths: 30 20 50

   * - Workload Type
     - Pool Size
     - Notes
   * - Development/Testing
     - 2-5
     - Minimal overhead, fast startup
   * - Low-Concurrency Production
     - 10-20
     - Typical web application
   * - High-Concurrency Production
     - 20-50
     - Thousands of concurrent users
   * - Extreme Scale
     - 50-100
     - Consider PgBouncer for connection pooling

.. warning::

   **Pool Exhaustion**: If you see "pool exhausted" errors, either increase ``max_size``
   or reduce query duration. Monitor with ``pool.get_size()`` and ``pool.get_idle_size()``.

Custom Table Names
------------------

.. code-block:: python

   store = AsyncpgADKStore(
       config,
       session_table="production_sessions",
       events_table="production_events"
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
       state JSONB NOT NULL DEFAULT '{}'::jsonb,
       create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
       update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
   ) WITH (fillfactor = 80);

   CREATE INDEX IF NOT EXISTS idx_adk_sessions_app_user
       ON adk_sessions(app_name, user_id);

   CREATE INDEX IF NOT EXISTS idx_adk_sessions_update_time
       ON adk_sessions(update_time DESC);

   CREATE INDEX IF NOT EXISTS idx_adk_sessions_state
       ON adk_sessions USING GIN (state)
       WHERE state != '{}'::jsonb;

**Schema Design Notes:**

- **VARCHAR(128)**: Sufficient for UUIDs and application names
- **JSONB**: Native PostgreSQL binary JSON format (faster than JSON)
- **TIMESTAMPTZ**: Timezone-aware timestamps with microsecond precision
- **FILLFACTOR 80**: Leaves 20% free space for HOT updates (reduces table bloat)
- **Composite Index**: ``(app_name, user_id)`` for efficient session listing
- **Temporal Index**: ``update_time DESC`` for recent session queries
- **Partial GIN Index**: Only indexes non-empty JSONB state (saves space)

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
       actions BYTEA,
       long_running_tool_ids_json TEXT,
       branch VARCHAR(256),
       timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
       content JSONB,
       grounding_metadata JSONB,
       custom_metadata JSONB,
       partial BOOLEAN,
       turn_complete BOOLEAN,
       interrupted BOOLEAN,
       error_code VARCHAR(256),
       error_message VARCHAR(1024),
       FOREIGN KEY (session_id) REFERENCES adk_sessions(id) ON DELETE CASCADE
   );

   CREATE INDEX IF NOT EXISTS idx_adk_events_session
       ON adk_events(session_id, timestamp ASC);

**Schema Design Notes:**

- **VARCHAR Sizes**: Optimized for typical Google ADK data
- **BYTEA**: Binary storage for pre-serialized actions (no double-pickling)
- **JSONB**: Direct dict conversion for content, grounding, and custom metadata
- **BOOLEAN**: Native boolean type (more efficient than integers)
- **CASCADE DELETE**: Automatically removes events when session deleted
- **Composite Index**: ``(session_id, timestamp ASC)`` for chronological event retrieval

Usage Patterns
==============

Session Management
------------------

.. code-block:: python

   import asyncio
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore

   async def session_example():
       config = AsyncpgConfig(pool_config={"dsn": "postgresql://..."})
       store = AsyncpgADKStore(config)
       await store.create_tables()

       # Create session with initial state
       session = await store.create_session(
           session_id="sess_abc123",
           app_name="chatbot",
           user_id="user_789",
           state={
               "conversation_context": "product_inquiry",
               "user_preferences": {"language": "en", "theme": "dark"},
               "cart_items": []
           }
       )

       # Get session by ID
       retrieved = await store.get_session("sess_abc123")
       if retrieved:
           print(f"State: {retrieved['state']}")

       # Update session state (full replacement)
       await store.update_session_state("sess_abc123", {
           "conversation_context": "checkout",
           "user_preferences": {"language": "en", "theme": "dark"},
           "cart_items": ["item1", "item2"]
       })

       # List all sessions for user
       sessions = await store.list_sessions("chatbot", "user_789")
       for session in sessions:
           print(f"Session {session['id']}: {session['update_time']}")

       # Delete session (cascade deletes events)
       await store.delete_session("sess_abc123")

   asyncio.run(session_example())

Event Management
----------------

.. code-block:: python

   from datetime import datetime, timezone
   from google.adk.events.event import Event
   from google.genai import types

   async def event_example():
       config = AsyncpgConfig(pool_config={"dsn": "postgresql://..."})
       store = AsyncpgADKStore(config)

       # Create session first
       session = await store.create_session(
           session_id="sess_xyz",
           app_name="assistant",
           user_id="user_456",
           state={}
       )

       # Append user event
       user_event = Event(
           id="evt_user_1",
           invocation_id="inv_123",
           author="user",
           branch="main",
           actions=[],
           timestamp=datetime.now(timezone.utc).timestamp(),
           content=types.Content(parts=[types.Part(text="Hello!")]),
           partial=False,
           turn_complete=True
       )
       await store.append_event(user_event)

       # Append assistant event with metadata
       assistant_event = Event(
           id="evt_asst_1",
           invocation_id="inv_123",
           author="assistant",
           branch="main",
           actions=[],
           timestamp=datetime.now(timezone.utc).timestamp(),
           content=types.Content(parts=[types.Part(text="Hi! How can I help?")]),
           grounding_metadata={"sources": ["knowledge_base_v2"]},
           custom_metadata={"confidence": 0.95, "model": "gemini-pro"},
           partial=False,
           turn_complete=True
       )
       await store.append_event(assistant_event)

       # Get all events for session (chronological order)
       events = await store.get_events("sess_xyz")
       for event in events:
           print(f"{event['author']}: {event['content']}")

       # Get recent events (since timestamp)
       from datetime import timedelta
       recent_time = datetime.now(timezone.utc) - timedelta(hours=1)
       recent_events = await store.get_events(
           "sess_xyz",
           after_timestamp=recent_time
       )

       # Limit number of events
       latest_10 = await store.get_events("sess_xyz", limit=10)

   asyncio.run(event_example())

Integration with SQLSpecSessionService
---------------------------------------

.. code-block:: python

   from sqlspec.extensions.adk import SQLSpecSessionService

   async def service_example():
       config = AsyncpgConfig(pool_config={"dsn": "postgresql://..."})
       store = AsyncpgADKStore(config)
       await store.create_tables()

       # Create high-level service
       service = SQLSpecSessionService(store)

       # Create session via service
       session = await service.create_session(
           app_name="support_bot",
           user_id="user_123",
           state={"ticket_id": "TKT-456"}
       )

       # Add events via service
       user_event = Event(...)
       await service.append_event(session, user_event)

       # Get session with full event history
       full_session = await service.get_session(
           app_name="support_bot",
           user_id="user_123",
           session_id=session.id
       )
       print(f"Session has {len(full_session.events)} events")

   asyncio.run(service_example())

Performance Considerations
==========================

JSONB Optimization
------------------

AsyncPG automatically converts Python dicts to/from JSONB without manual serialization:

.. code-block:: python

   # AsyncPG handles this automatically - no json.dumps() needed!
   await store.update_session_state("sess_id", {
       "complex": {"nested": {"data": [1, 2, 3]}},
       "arrays": [{"id": 1}, {"id": 2}],
       "nulls": None
   })

**JSONB Query Performance:**

.. code-block:: sql

   -- Fast: Uses GIN index on state
   SELECT * FROM adk_sessions WHERE state @> '{"user_preferences": {"language": "en"}}';

   -- Fast: JSON path extraction
   SELECT state->'conversation_context' FROM adk_sessions WHERE id = $1;

   -- Fast: Array operations
   SELECT * FROM adk_sessions WHERE state->'cart_items' @> '["item1"]';

Connection Pooling Best Practices
----------------------------------

**Recommended Pattern:**

.. code-block:: python

   # Create config and pool once at application startup
   config = AsyncpgConfig(pool_config={
       "dsn": "postgresql://...",
       "min_size": 10,
       "max_size": 20
   })

   # Reuse config across requests
   store = AsyncpgADKStore(config)
   await store.create_tables()

   # Pool is automatically managed
   async def handle_request():
       # Each operation acquires/releases from pool
       session = await store.get_session(session_id)

**Anti-Pattern (Avoid):**

.. code-block:: python

   # BAD: Creating new config per request
   async def handle_request():
       config = AsyncpgConfig(...)  # Don't do this!
       store = AsyncpgADKStore(config)

HOT Updates
-----------

PostgreSQL **Heap-Only Tuple (HOT)** updates reduce table bloat:

.. code-block:: python

   # HOT update works best when:
   # 1. Only updating indexed columns
   # 2. New row fits in same page (fillfactor = 80 provides space)

   # This is HOT-eligible (only updating state and update_time)
   await store.update_session_state(session_id, new_state)

   # Monitor table bloat
   # SELECT pg_stat_user_tables WHERE relname = 'adk_sessions';

Index Strategy
--------------

**Composite Index Performance:**

.. code-block:: sql

   -- Fast: Uses idx_adk_sessions_app_user
   SELECT * FROM adk_sessions WHERE app_name = $1 AND user_id = $2;

   -- Fast: Index-only scan on update_time
   SELECT * FROM adk_sessions ORDER BY update_time DESC LIMIT 10;

   -- Fast: Uses idx_adk_events_session
   SELECT * FROM adk_events WHERE session_id = $1 ORDER BY timestamp ASC;

**JSONB GIN Index:**

.. code-block:: sql

   -- Fast: Partial GIN index on non-empty state
   SELECT * FROM adk_sessions WHERE state ? 'conversation_context';

Prepared Statements
-------------------

AsyncPG automatically prepares frequently-used statements:

.. code-block:: python

   # AsyncPG caches prepared statements (LRU cache, default 100)
   # Repeated queries use cached prepared statement (faster)

   for i in range(1000):
       await store.get_session(f"sess_{i}")  # Same SQL, different param

   # Statement cache is per-connection
   # Pool provides multiple connections, each with own cache

Best Practices
==============

Schema Design
-------------

✅ **DO:**

- Use JSONB for flexible state storage
- Create composite indexes for common query patterns
- Set FILLFACTOR 80 for frequently-updated tables
- Use partial indexes to save space
- Enable CASCADE deletes for referential integrity

❌ **DON'T:**

- Store large binary data in JSONB (use BYTEA)
- Create indexes on rarely-queried columns
- Use TEXT for JSON (use JSONB instead)
- Forget to set update_time on state changes

Query Patterns
--------------

✅ **DO:**

.. code-block:: python

   # Good: Leverages composite index
   sessions = await store.list_sessions("app", "user")

   # Good: Ordered by indexed column
   events = await store.get_events("session_id", limit=100)

   # Good: Uses GIN index
   # SELECT * FROM adk_sessions WHERE state @> '{"key": "value"}'

❌ **DON'T:**

.. code-block:: python

   # Bad: Sequential scan
   # SELECT * FROM adk_sessions WHERE state::text LIKE '%value%'

   # Bad: No limit on large result sets
   events = await store.get_events("session_id")  # Could be millions!

Connection Management
---------------------

✅ **DO:**

.. code-block:: python

   # Good: Reuse config and pool
   config = AsyncpgConfig(...)
   store = AsyncpgADKStore(config)

   async def many_queries():
       for i in range(1000):
           await store.get_session(f"sess_{i}")

❌ **DON'T:**

.. code-block:: python

   # Bad: New pool per query
   async def bad_pattern():
       config = AsyncpgConfig(...)  # Creates new pool!
       store = AsyncpgADKStore(config)
       await store.get_session("sess_id")

Monitoring
----------

Monitor AsyncPG pool health:

.. code-block:: python

   async def monitor_pool():
       pool = await config.provide_pool()

       # Check pool statistics
       print(f"Pool size: {pool.get_size()}")
       print(f"Idle connections: {pool.get_idle_size()}")
       print(f"Min size: {pool.get_min_size()}")
       print(f"Max size: {pool.get_max_size()}")

       # Log slow queries
       async with config.provide_connection() as conn:
           await conn.execute("SET log_min_duration_statement = 1000;")

Use Cases
=========

Production Async Web Applications
----------------------------------

AsyncPG is ideal for async web frameworks:

.. code-block:: python

   from litestar import Litestar, get
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore

   # Initialize at app startup
   config = AsyncpgConfig(pool_config={"dsn": "postgresql://..."})
   store = AsyncpgADKStore(config)

   @get("/session/{session_id:str}")
   async def get_session(session_id: str) -> dict:
       session = await store.get_session(session_id)
       return session or {"error": "not found"}

   app = Litestar(
       route_handlers=[get_session],
       on_startup=[lambda: store.create_tables()]
   )

High-Concurrency AI Agents
---------------------------

Handle thousands of concurrent users:

.. code-block:: python

   config = AsyncpgConfig(pool_config={
       "dsn": "postgresql://...",
       "min_size": 20,
       "max_size": 50,
       "command_timeout": 60.0
   })

   store = AsyncpgADKStore(config)
   service = SQLSpecSessionService(store)

   async def handle_concurrent_users():
       tasks = []
       for user_id in range(10000):
           task = service.create_session(
               app_name="assistant",
               user_id=f"user_{user_id}",
               state={}
           )
           tasks.append(task)

       # AsyncPG efficiently handles concurrent operations
       sessions = await asyncio.gather(*tasks)
       print(f"Created {len(sessions)} sessions")

Real-Time Conversational AI
----------------------------

Minimize latency with AsyncPG's speed:

.. code-block:: python

   import time

   async def measure_latency():
       start = time.perf_counter()

       # Create session
       session = await store.create_session(
           session_id="sess_timing",
           app_name="realtime_chat",
           user_id="user_456",
           state={}
       )

       # Add event
       event = Event(...)
       await store.append_event(event)

       # Get session with events
       full_session = await store.get_events("sess_timing")

       elapsed_ms = (time.perf_counter() - start) * 1000
       print(f"Total latency: {elapsed_ms:.2f}ms")  # Typically < 10ms

When to Choose AsyncPG
======================

**Use AsyncPG When:**

✅ Building production async AI agents
✅ Require maximum PostgreSQL performance
✅ Using async web frameworks (Litestar, FastAPI, Starlette)
✅ Need connection pooling for high concurrency
✅ Working with JSONB data extensively
✅ Require microsecond timestamp precision
✅ Want automatic prepared statement caching

**Consider Alternatives When:**

❌ **Psycopg3**: Need sync AND async in same codebase (psycopg supports both)
❌ **Psqlpy**: Require Rust-level performance (experimental, but faster)
❌ **ADBC**: Need cross-database portability with Arrow format
❌ **SQLite**: Development/testing without PostgreSQL server
❌ **DuckDB**: Analytical workloads, not transactional

Comparison: AsyncPG vs Other PostgreSQL Drivers
------------------------------------------------

.. list-table::
   :header-rows: 1
   :widths: 20 20 20 20 20

   * - Feature
     - AsyncPG
     - Psycopg3
     - Psqlpy
     - ADBC
   * - Performance
     - ⭐⭐⭐⭐⭐
     - ⭐⭐⭐⭐
     - ⭐⭐⭐⭐⭐
     - ⭐⭐⭐
   * - Async Support
     - Native
     - Native
     - Native
     - Yes
   * - Sync Support
     - No
     - Yes
     - No
     - Yes
   * - Connection Pool
     - Built-in
     - Via pgpool
     - Built-in
     - No
   * - JSONB Handling
     - Automatic
     - Manual
     - Automatic
     - Manual
   * - Prepared Stmts
     - Automatic
     - Manual
     - Automatic
     - N/A
   * - Maturity
     - Stable
     - Stable
     - Experimental
     - Stable
   * - Best For
     - Async prod
     - Sync+async
     - Max speed
     - Portability

.. note::

   **Recommendation**: Use AsyncPG for production async workloads. If you need both
   sync and async in the same application, use Psycopg3. For cutting-edge performance
   and willing to deal with less maturity, try Psqlpy.

Troubleshooting
===============

Connection Pool Exhausted
--------------------------

**Error:**

.. code-block:: text

   asyncpg.exceptions.TooManyConnectionsError: pool exhausted

**Solution:**

.. code-block:: python

   # Increase pool size
   config = AsyncpgConfig(pool_config={
       "max_size": 50,  # Increase from default 10
       "command_timeout": 30.0  # Prevent hung connections
   })

   # Or use a transaction timeout
   async with config.provide_connection() as conn:
       async with conn.transaction():
           await conn.execute("SET LOCAL statement_timeout = '30s'")

Connection Refused
------------------

**Error:**

.. code-block:: text

   asyncpg.exceptions.ConnectionDoesNotExistError: connection refused

**Solution:**

.. code-block:: bash

   # Verify PostgreSQL is running
   psql -h localhost -U postgres -d agentdb

   # Check connection parameters
   config = AsyncpgConfig(pool_config={
       "host": "localhost",  # Correct host
       "port": 5432,         # Correct port
       "user": "postgres",   # Correct user
       "database": "agentdb" # Correct database
   })

Slow Queries
------------

**Symptom**: Queries taking longer than expected

**Solution:**

.. code-block:: python

   # Enable query logging
   async with config.provide_connection() as conn:
       await conn.execute("SET log_min_duration_statement = 100;")

   # Check query plan
   result = await conn.fetch("EXPLAIN ANALYZE SELECT * FROM adk_sessions ...")

   # Ensure indexes exist
   await conn.execute("""
       SELECT schemaname, tablename, indexname
       FROM pg_indexes
       WHERE tablename IN ('adk_sessions', 'adk_events')
   """)

SSL Connection Issues
---------------------

**Error:**

.. code-block:: text

   asyncpg.exceptions.InvalidAuthorizationSpecificationError: SSL required

**Solution:**

.. code-block:: python

   import ssl

   # Require SSL
   config = AsyncpgConfig(pool_config={
       "dsn": "postgresql://...",
       "ssl": "require"
   })

   # Or use custom SSL context
   ssl_context = ssl.create_default_context()
   ssl_context.check_hostname = False
   ssl_context.verify_mode = ssl.CERT_NONE

   config = AsyncpgConfig(pool_config={
       "dsn": "postgresql://...",
       "ssl": ssl_context
   })

JSONB Type Codec Errors
------------------------

**Error:**

.. code-block:: text

   TypeError: Object of type X is not JSON serializable

**Solution:**

.. code-block:: python

   # Custom JSON serializer
   import json
   from datetime import datetime

   def custom_json_serializer(obj):
       if isinstance(obj, datetime):
           return obj.isoformat()
       return json.dumps(obj)

   config = AsyncpgConfig(
       pool_config={"dsn": "postgresql://..."},
       driver_features={
           "json_serializer": custom_json_serializer
       }
   )

Migration from Other Databases
===============================

From SQLite to AsyncPG
----------------------

.. code-block:: python

   from sqlspec.adapters.sqlite import SqliteConfig
   from sqlspec.adapters.sqlite.adk import SqliteADKStore
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore

   # Export from SQLite
   sqlite_config = SqliteConfig(database="./agent.db")
   sqlite_store = SqliteADKStore(sqlite_config)

   sessions = sqlite_store.list_sessions("app", "user")

   # Import to AsyncPG
   pg_config = AsyncpgConfig(pool_config={"dsn": "postgresql://..."})
   pg_store = AsyncpgADKStore(pg_config)
   await pg_store.create_tables()

   for session in sessions:
       await pg_store.create_session(
           session_id=session["id"],
           app_name=session["app_name"],
           user_id=session["user_id"],
           state=session["state"]
       )

From Psycopg to AsyncPG
-----------------------

Both use the same SQL schema, so migration is straightforward:

.. code-block:: python

   # Old Psycopg config
   from sqlspec.adapters.psycopg import PsycopgAsyncConfig
   from sqlspec.adapters.psycopg.adk import PsycopgADKStore

   # New AsyncPG config (same connection params)
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore

   # Just change the config class - SQL is identical
   config = AsyncpgConfig(pool_config={
       "dsn": "postgresql://..."  # Same connection string
   })

API Reference
=============

.. autoclass:: sqlspec.adapters.asyncpg.adk.AsyncpgADKStore
   :members:
   :inherited-members:
   :show-inheritance:
   :no-index:

.. autoclass:: sqlspec.adapters.asyncpg.AsyncpgConfig
   :members:
   :show-inheritance:
   :no-index:

See Also
========

- :doc:`../quickstart` - Quick start guide
- :doc:`../adapters` - Adapter comparison
- :doc:`../schema` - Database schema details
- :doc:`/adapters/asyncpg` - AsyncPG adapter documentation
- `AsyncPG Documentation <https://magicstack.github.io/asyncpg/>`_ - Official AsyncPG docs
- `PostgreSQL JSONB Documentation <https://www.postgresql.org/docs/current/datatype-json.html>`_ - JSONB reference
- `PostgreSQL Performance Tuning <https://wiki.postgresql.org/wiki/Performance_Optimization>`_ - Performance guide
