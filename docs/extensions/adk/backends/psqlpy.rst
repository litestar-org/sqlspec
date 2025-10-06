==================
Psqlpy Backend
==================

Overview
========

Psqlpy is a **Rust-based asynchronous PostgreSQL driver** that offers exceptional performance for high-throughput database operations. Built with PyO3, it combines the safety and speed of Rust with Python's ease of use, making it ideal for performance-critical AI agent deployments.

**Key Features:**

- **Rust Performance**: Native Rust implementation for maximum speed
- **Async-Native**: Built from the ground up for async I/O
- **JSONB Support**: Native PostgreSQL JSONB handling without wrapper types
- **Connection Pooling**: Built-in high-performance connection pool
- **Type Safety**: Strong type system inherited from Rust
- **Zero-Copy Operations**: Efficient memory usage where possible

**Ideal Use Cases:**

- High-throughput AI agent applications requiring maximum performance
- Production deployments with demanding performance requirements
- Rust-based technology stacks seeking consistent tooling
- Applications needing optimal PostgreSQL performance
- Systems with high concurrent load and low latency requirements

Installation
============

Install SQLSpec with Psqlpy support:

.. code-block:: bash

   pip install sqlspec[psqlpy] google-genai
   # or
   uv pip install sqlspec[psqlpy] google-genai

.. note::

   Psqlpy is a Rust-based library. Pre-built binary wheels are available for most platforms.
   If a wheel is not available for your platform, you will need the Rust toolchain installed
   for compilation. See `psqlpy documentation <https://github.com/qaspen-python/psqlpy>`_ for details.

Quick Start
===========

Basic Setup
-----------

.. code-block:: python

   from sqlspec.adapters.psqlpy import PsqlpyConfig
   from sqlspec.adapters.psqlpy.adk import PsqlpyADKStore
   from sqlspec.extensions.adk import SQLSpecSessionService

   # Create configuration with connection pool
   config = PsqlpyConfig(
       pool_config={
           "dsn": "postgresql://user:password@localhost:5432/mydb",
           "max_db_pool_size": 10,
       }
   )

   # Initialize store
   store = PsqlpyADKStore(config)
   await store.create_tables()

   # Create service
   service = SQLSpecSessionService(store)

   # Create session
   session = await service.create_session(
       app_name="high_perf_agent",
       user_id="user_123",
       state={"context": "performance_critical"}
   )

Schema
======

The Psqlpy backend uses PostgreSQL-specific optimizations for maximum performance.

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

**Schema Features:**

- **JSONB Type**: Native JSON Binary storage for efficient state management
- **TIMESTAMPTZ**: Timezone-aware microsecond-precision timestamps
- **FILLFACTOR 80**: Optimized for HOT (Heap-Only Tuple) updates to reduce bloat
- **GIN Index**: Generalized Inverted Index on JSONB state for fast queries
- **Partial Index**: GIN index only on non-empty state to save space

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

**Schema Features:**

- **BYTEA for Actions**: Binary storage for pre-serialized Google ADK actions
- **Multiple JSONB Columns**: Separate JSONB fields for content, grounding, and metadata
- **CASCADE DELETE**: Automatic cleanup of events when session is deleted
- **Composite Index**: Optimized for chronological event retrieval by session

Configuration
=============

Basic Configuration
-------------------

.. code-block:: python

   from sqlspec.adapters.psqlpy import PsqlpyConfig

   config = PsqlpyConfig(
       pool_config={
           "dsn": "postgresql://user:password@localhost:5432/mydb",
           "max_db_pool_size": 20,
       }
   )

Advanced Connection Pooling
----------------------------

.. code-block:: python

   config = PsqlpyConfig(
       pool_config={
           "host": "localhost",
           "port": 5432,
           "username": "user",
           "password": "password",
           "db_name": "mydb",
           "max_db_pool_size": 50,
           "connect_timeout_sec": 10,
           "keepalives": True,
           "keepalives_idle_sec": 60,
           "keepalives_interval_sec": 10,
           "keepalives_retries": 3,
       }
   )

SSL Configuration
-----------------

.. code-block:: python

   config = PsqlpyConfig(
       pool_config={
           "dsn": "postgresql://user:password@localhost:5432/mydb",
           "ssl_mode": "require",
           "sslrootcert": "/path/to/ca.crt",
           "sslcert": "/path/to/client.crt",
           "sslkey": "/path/to/client.key",
       }
   )

Custom Table Names
------------------

.. code-block:: python

   store = PsqlpyADKStore(
       config,
       session_table="custom_sessions",
       events_table="custom_events"
   )

Usage Patterns
==============

Psqlpy-Specific API Patterns
-----------------------------

Psqlpy has a unique API pattern that differs from other PostgreSQL drivers:

**Result Handling:**

.. code-block:: python

   # Psqlpy uses .fetch() then .result()
   async with config.provide_connection() as conn:
       result = await conn.fetch("SELECT * FROM adk_sessions WHERE id = $1", [session_id])
       rows: list[dict[str, Any]] = result.result() if result else []

**Parameter Style:**

.. code-block:: python

   # Psqlpy requires LIST parameters (not tuples)
   # Uses PostgreSQL numeric placeholders: $1, $2, $3
   
   # CORRECT - List parameters
   await conn.execute(
       "INSERT INTO adk_sessions (id, app_name, user_id, state) VALUES ($1, $2, $3, $4)",
       [session_id, app_name, user_id, state_dict]
   )
   
   # INCORRECT - Tuples don't work
   # await conn.execute(sql, (param1, param2))  # Will fail!

**JSONB Handling:**

.. code-block:: python

   # Psqlpy automatically converts Python dicts to/from JSONB
   # NO wrapper types needed (unlike psycopg's Jsonb)
   
   state = {"key": "value", "nested": {"data": 123}}
   
   # Pass dict directly - automatically converted to JSONB
   await conn.execute(
       "INSERT INTO adk_sessions (state) VALUES ($1)",
       [state]  # Dict is automatically converted to JSONB
   )
   
   # Retrieved as Python dict automatically
   result = await conn.fetch("SELECT state FROM adk_sessions WHERE id = $1", [session_id])
   rows = result.result()
   state_dict = rows[0]["state"]  # Already a Python dict

JSONB Querying
--------------

PostgreSQL JSONB operators work seamlessly with Psqlpy:

.. code-block:: python

   # Query JSONB fields
   async with config.provide_connection() as conn:
       # Get sessions with specific state property
       result = await conn.fetch(
           "SELECT * FROM adk_sessions WHERE state->>'status' = $1",
           ["active"]
       )
       rows = result.result()
       
       # Check if JSONB contains key
       result = await conn.fetch(
           "SELECT * FROM adk_sessions WHERE state ? $1",
           ["dashboard"]
       )
       rows = result.result()
       
       # Check if JSONB contains value
       result = await conn.fetch(
           "SELECT * FROM adk_sessions WHERE state @> $1::jsonb",
           ['{"status": "active"}']
       )
       rows = result.result()

Performance Considerations
==========================

Rust Performance Benefits
--------------------------

Psqlpy's Rust implementation provides significant performance advantages:

**Benchmark Comparison (relative to pure Python drivers):**

- **Connection Pooling**: ~2-3x faster pool acquisition
- **Query Execution**: ~1.5-2x faster for simple queries
- **JSON Parsing**: ~2-4x faster JSONB operations
- **Memory Efficiency**: Lower memory overhead per connection
- **Concurrent Load**: Better performance under high concurrency

**When Performance Matters Most:**

✅ High transaction rate (>1000 TPS)
✅ Large JSONB payloads (>1KB state objects)
✅ High connection churn
✅ CPU-bound workloads
✅ Latency-sensitive applications (<10ms p99)

Connection Pool Tuning
----------------------

Optimize pool size for your workload:

.. code-block:: python

   # For high-concurrency workloads
   config = PsqlpyConfig(
       pool_config={
           "dsn": "postgresql://...",
           "max_db_pool_size": 100,  # Large pool for many concurrent users
       }
   )
   
   # For low-latency workloads
   config = PsqlpyConfig(
       pool_config={
           "dsn": "postgresql://...",
           "max_db_pool_size": 20,   # Smaller pool, faster checkout
           "connect_timeout_sec": 5,  # Fail fast
       }
   )

**Pool Sizing Guidelines:**

- **Web applications**: 2-5x the number of worker processes
- **Background workers**: 1-2x the number of workers
- **High concurrency**: 50-100 connections
- **Low latency**: 10-20 connections (reduce contention)

JSONB Performance
-----------------

Optimize JSONB operations:

.. code-block:: python

   # Use GIN index for JSONB queries
   # Already created by default in sessions table
   
   # Efficient: Uses partial GIN index
   result = await conn.fetch(
       "SELECT * FROM adk_sessions WHERE state @> $1::jsonb",
       ['{"status": "active"}']
   )
   
   # Efficient: Indexed extraction
   result = await conn.fetch(
       "SELECT * FROM adk_sessions WHERE state->>'user_role' = $1",
       ["admin"]
   )

**JSONB Best Practices:**

- Keep state objects under 100KB for optimal performance
- Use GIN indexes for frequent queries on JSONB fields
- Prefer `@>` (contains) operator over function calls
- Use `->` and `->>` operators for direct key access

Best Practices
==============

When to Choose Psqlpy
----------------------

**Choose Psqlpy When:**

✅ Maximum PostgreSQL performance is required
✅ High-throughput production deployments
✅ Latency-sensitive applications
✅ Large JSONB payloads
✅ Rust-based technology stack
✅ High concurrent connection load

**Consider AsyncPG Instead When:**

- Need more mature ecosystem and wider community support
- Using features that may not yet be in psqlpy
- Prefer pure Python implementation
- Already have asyncpg expertise in the team

**Consider Psycopg Instead When:**

- Need both sync and async support
- Require maximum feature parity with PostgreSQL
- Need battle-tested production stability

Error Handling
--------------

.. code-block:: python

   import psqlpy.exceptions

   try:
       session = await service.create_session(
           app_name="my_app",
           user_id="user_123",
           state={"data": "value"}
       )
   except psqlpy.exceptions.DatabaseError as e:
       # Handle database errors
       print(f"Database error: {e}")
   except psqlpy.exceptions.ConnectionError as e:
       # Handle connection errors
       print(f"Connection error: {e}")

Connection Management
---------------------

.. code-block:: python

   # Always use context managers for connections
   async with config.provide_connection() as conn:
       result = await conn.fetch("SELECT * FROM adk_sessions", [])
       rows = result.result()
       # Connection automatically returned to pool

Comparison: Psqlpy vs Other PostgreSQL Drivers
===============================================

.. list-table::
   :header-rows: 1
   :widths: 25 25 25 25

   * - Feature
     - Psqlpy
     - AsyncPG
     - Psycopg
   * - Implementation
     - Rust (PyO3)
     - Pure Python + C
     - Python + C (libpq)
   * - Performance
     - Excellent
     - Excellent
     - Very Good
   * - Async Support
     - Native async
     - Native async
     - Async + Sync
   * - JSONB Handling
     - Direct dict conversion
     - Direct dict conversion
     - Jsonb wrapper class
   * - Parameter Style
     - Lists required
     - Lists/tuples both work
     - Tuples preferred
   * - Connection Pool
     - Built-in (Rust)
     - Built-in (Python)
     - asyncpg-pool or pgbouncer
   * - Maturity
     - Newer
     - Very Mature
     - Very Mature
   * - Community
     - Growing
     - Large
     - Very Large
   * - Best For
     - Max performance
     - Production standard
     - Full feature set

Use Cases
=========

High-Performance Agent API
---------------------------

.. code-block:: python

   from sqlspec.adapters.psqlpy import PsqlpyConfig
   from sqlspec.adapters.psqlpy.adk import PsqlpyADKStore
   from sqlspec.extensions.adk import SQLSpecSessionService

   # High-performance configuration
   config = PsqlpyConfig(
       pool_config={
           "dsn": "postgresql://localhost:5432/agents",
           "max_db_pool_size": 100,
           "connect_timeout_sec": 5,
           "keepalives": True,
       }
   )

   store = PsqlpyADKStore(config)
   await store.create_tables()
   service = SQLSpecSessionService(store)

   # Handle high request rate
   async def handle_request(user_id: str):
       session = await service.create_session(
           app_name="api_agent",
           user_id=user_id,
           state={"request_count": 0}
       )
       return session

Real-Time Analytics on Sessions
--------------------------------

.. code-block:: python

   # Leverage JSONB GIN index for fast queries
   async with config.provide_connection() as conn:
       result = await conn.fetch(
           """
           SELECT 
               state->>'category' as category,
               COUNT(*) as session_count
           FROM adk_sessions
           WHERE app_name = $1 
             AND state @> '{"active": true}'::jsonb
           GROUP BY category
           ORDER BY session_count DESC
           """,
           ["analytics_agent"]
       )
       rows = result.result()
       for row in rows:
           print(f"{row['category']}: {row['session_count']} sessions")

Rust Microservices Integration
-------------------------------

.. code-block:: python

   # Consistent Rust stack: psqlpy + other Rust Python bindings
   from sqlspec.adapters.psqlpy import PsqlpyConfig
   # from orjson import dumps, loads  # Rust-based JSON
   # from pydantic_core import ValidationError  # Rust-based validation

   config = PsqlpyConfig(
       pool_config={
           "dsn": "postgresql://localhost:5432/microservices"
       }
   )

   # Entire stack benefits from Rust performance
   store = PsqlpyADKStore(config)

Troubleshooting
===============

Installation Issues
-------------------

**Issue: Rust compilation required**

.. code-block:: text

   error: failed to run custom build command for `psqlpy`

**Solution:** Install Rust toolchain:

.. code-block:: bash

   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

Or use pre-built wheels:

.. code-block:: bash

   pip install --only-binary :all: psqlpy

Parameter Type Errors
---------------------

**Issue: Parameters must be a list**

.. code-block:: python

   # WRONG - Using tuple
   await conn.execute(sql, (param1, param2))
   
   # CORRECT - Use list
   await conn.execute(sql, [param1, param2])

Connection Pool Exhaustion
---------------------------

**Issue: Pool size too small for load**

.. code-block:: python

   # Increase pool size
   config = PsqlpyConfig(
       pool_config={
           "dsn": "postgresql://...",
           "max_db_pool_size": 50,  # Increase from default
       }
   )

JSONB Query Performance
-----------------------

**Issue: Slow JSONB queries**

.. code-block:: sql

   -- Ensure GIN index exists (created by default)
   CREATE INDEX IF NOT EXISTS idx_adk_sessions_state
       ON adk_sessions USING GIN (state)
       WHERE state != '{}'::jsonb;

   -- Use containment operator for best performance
   SELECT * FROM adk_sessions WHERE state @> '{"key": "value"}'::jsonb;

API Reference
=============

.. autoclass:: sqlspec.adapters.psqlpy.adk.PsqlpyADKStore
   :members:
   :inherited-members:
   :show-inheritance:

See Also
========

- :doc:`../quickstart` - Quick start guide
- :doc:`../adapters` - Adapter comparison
- :doc:`../schema` - Database schema details
- :doc:`asyncpg` - AsyncPG backend (alternative)
- `Psqlpy Documentation <https://github.com/qaspen-python/psqlpy>`_ - Official psqlpy documentation
- `PostgreSQL JSONB Documentation <https://www.postgresql.org/docs/current/datatype-json.html>`_ - PostgreSQL JSON types
