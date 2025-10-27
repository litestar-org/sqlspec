================
AsyncMy Backend
================

Overview
========

AsyncMy is an async MySQL/MariaDB driver optimized for Python's asyncio ecosystem. It provides high-performance, non-blocking database operations with native connection pooling support, making it ideal for production web applications and async AI agents.

**Key Features:**

- **Native Async**: Built from the ground up for asyncio with non-blocking I/O
- **Connection Pooling**: Built-in async connection pool with configurable sizing
- **MySQL JSON Support**: Native MySQL JSON type (requires MySQL 5.7.8+ or MariaDB 10.2.7+)
- **Microsecond Timestamps**: TIMESTAMP(6) for microsecond-precision event tracking
- **InnoDB Engine**: Full ACID compliance with foreign key constraints and cascade deletes
- **PyMySQL Compatibility**: Familiar API for developers coming from PyMySQL

**Ideal Use Cases:**

- Production async web applications (FastAPI, Litestar, Starlette)
- High-concurrency AI agent deployments
- Existing MySQL/MariaDB infrastructure
- Multi-tenant applications requiring connection pooling
- Real-time conversation systems with sub-millisecond latency requirements

.. warning::

   **MySQL 5.7.8+ or MariaDB 10.2.7+ Required** for native JSON type support.
   Earlier versions do not support the JSON column type used by the ADK store.

Installation
============

Install SQLSpec with AsyncMy support:

.. code-block:: bash

   pip install sqlspec[asyncmy,adk] google-genai
   # or
   uv pip install sqlspec[asyncmy,adk] google-genai

Quick Start
===========

Basic Async Connection
-----------------------

.. code-block:: python

   from sqlspec.adapters.asyncmy import AsyncmyConfig
   from sqlspec.adapters.asyncmy.adk import AsyncmyADKStore
   from sqlspec.extensions.adk import SQLSpecSessionService

   config = AsyncmyConfig(
       pool_config={
           "host": "localhost",
           "port": 3306,
           "user": "myuser",
           "password": "mypassword",
           "database": "agent_db",
           "minsize": 5,
           "maxsize": 20,
       }
   )

   store = AsyncmyADKStore(config)
   await store.create_tables()

   service = SQLSpecSessionService(store)

   session = await service.create_session(
       app_name="customer_support",
       user_id="user_123",
       state={"conversation_context": "billing_inquiry"}
   )

Connection Pooling Configuration
---------------------------------

AsyncMy's built-in connection pool is production-ready:

.. code-block:: python

   config = AsyncmyConfig(
       pool_config={
           "host": "mysql.example.com",
           "port": 3306,
           "user": "agent_user",
           "password": "secure_password",
           "database": "ai_agents",
           "minsize": 10,           # Minimum connections maintained
           "maxsize": 50,           # Maximum concurrent connections
           "pool_recycle": 3600,    # Recycle connections every hour
           "connect_timeout": 10,   # Connection timeout in seconds
           "charset": "utf8mb4",    # Full Unicode support
           "autocommit": False,     # Explicit transaction control
       }
   )

.. tip::

   **Production Pool Sizing:**

   - **minsize**: 10-20 for steady-state workloads
   - **maxsize**: 50-100 for high-concurrency applications
   - **pool_recycle**: 3600 (1 hour) to prevent stale connections

Schema
======

The AsyncMy ADK store creates MySQL-optimized tables with InnoDB engine, JSON columns, and microsecond-precision timestamps.

Sessions Table
--------------

.. code-block:: sql

   CREATE TABLE IF NOT EXISTS adk_sessions (
       id VARCHAR(128) PRIMARY KEY,
       app_name VARCHAR(128) NOT NULL,
       user_id VARCHAR(128) NOT NULL,
       state JSON NOT NULL,                         -- Native MySQL JSON type
       create_time TIMESTAMP(6) NOT NULL
           DEFAULT CURRENT_TIMESTAMP(6),            -- Microsecond precision
       update_time TIMESTAMP(6) NOT NULL
           DEFAULT CURRENT_TIMESTAMP(6)
           ON UPDATE CURRENT_TIMESTAMP(6),          -- Auto-update on changes
       INDEX idx_adk_sessions_app_user (app_name, user_id),
       INDEX idx_adk_sessions_update_time (update_time DESC)
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

Events Table
------------

.. code-block:: sql

   CREATE TABLE IF NOT EXISTS adk_events (
       id VARCHAR(128) PRIMARY KEY,
       session_id VARCHAR(128) NOT NULL,
       app_name VARCHAR(128) NOT NULL,
       user_id VARCHAR(128) NOT NULL,
       invocation_id VARCHAR(256) NOT NULL,
       author VARCHAR(256) NOT NULL,
       actions BLOB NOT NULL,                       -- Pickled action data
       long_running_tool_ids_json TEXT,
       branch VARCHAR(256),
       timestamp TIMESTAMP(6) NOT NULL
           DEFAULT CURRENT_TIMESTAMP(6),            -- Microsecond precision
       content JSON,                                -- Native JSON type
       grounding_metadata JSON,
       custom_metadata JSON,
       partial BOOLEAN,
       turn_complete BOOLEAN,
       interrupted BOOLEAN,
       error_code VARCHAR(256),
       error_message VARCHAR(1024),
       FOREIGN KEY (session_id)
           REFERENCES adk_sessions(id)
           ON DELETE CASCADE,                       -- Auto-delete events
       INDEX idx_adk_events_session (session_id, timestamp ASC)
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

.. note::

   **Schema Design Decisions:**

   - **InnoDB Engine**: Required for foreign key support and ACID transactions
   - **utf8mb4**: Full Unicode support (4-byte characters including emoji)
   - **TIMESTAMP(6)**: Microsecond precision for event ordering
   - **JSON Type**: Native MySQL JSON (not JSONB like PostgreSQL)
   - **Cascade Delete**: Events automatically deleted when session is removed

Configuration
=============

Basic Configuration
-------------------

.. code-block:: python

   from sqlspec.adapters.asyncmy import AsyncmyConfig

   config = AsyncmyConfig(
       pool_config={
           "host": "localhost",
           "port": 3306,
           "user": "myuser",
           "password": "mypassword",
           "database": "mydb",
       }
   )

Advanced Configuration
----------------------

.. code-block:: python

   config = AsyncmyConfig(
       pool_config={
           "host": "mysql-primary.example.com",
           "port": 3306,
           "user": "agent_app",
           "password": "secure_password",
           "database": "ai_agents_prod",
           "minsize": 15,
           "maxsize": 75,
           "pool_recycle": 3600,
           "connect_timeout": 10,
           "charset": "utf8mb4",
           "autocommit": False,
           "local_infile": False,       # Security: disable local file loading
           "ssl": {                      # SSL/TLS encryption
               "ca": "/path/to/ca-cert.pem",
               "cert": "/path/to/client-cert.pem",
               "key": "/path/to/client-key.pem",
           },
           "init_command": "SET time_zone='+00:00'",  # Force UTC
       }
   )

Custom Table Names
------------------

.. code-block:: python

   store = AsyncmyADKStore(
       config,
       session_table="custom_sessions",
       events_table="custom_events"
   )

Usage Patterns
==============

MySQL JSON Operations
---------------------

MySQL's JSON type supports efficient querying and indexing:

.. code-block:: python

   # State stored as native JSON
   session = await service.create_session(
       app_name="analytics_bot",
       user_id="analyst_1",
       state={
           "dashboard": "sales",
           "filters": {
               "date_range": "last_30_days",
               "region": "EMEA"
           },
           "preferences": {
               "chart_type": "bar",
               "currency": "EUR"
           }
       }
   )

   # Query JSON fields with MySQL JSON functions
   import asyncmy

   async with config.provide_connection() as conn:
       async with conn.cursor() as cursor:
           await cursor.execute("""
               SELECT
                   id,
                   user_id,
                   JSON_EXTRACT(state, '$.dashboard') as dashboard,
                   JSON_EXTRACT(state, '$.filters.region') as region
               FROM adk_sessions
               WHERE app_name = %s
                   AND JSON_EXTRACT(state, '$.dashboard') = %s
           """, ("analytics_bot", "sales"))

           results = await cursor.fetchall()
           for row in results:
               print(f"Session {row[0]}: Dashboard={row[2]}, Region={row[3]}")

Microsecond Timestamp Handling
-------------------------------

.. code-block:: python

   from datetime import datetime, timezone

   # Get events after specific microsecond-precision time
   cutoff_time = datetime(2025, 10, 6, 12, 30, 45, 123456, tzinfo=timezone.utc)

   events = await store.get_events(
       session_id=session.id,
       after_timestamp=cutoff_time  # Microsecond precision preserved
   )

   for event in events:
       # event.timestamp is timezone-aware datetime with microseconds
       print(f"Event at {event.timestamp.isoformat()}")

Transaction Management
----------------------

.. code-block:: python

   async with config.provide_connection() as conn:
       try:
           await conn.begin()  # Start transaction

           async with conn.cursor() as cursor:
               # Multiple operations in single transaction
               await cursor.execute("INSERT INTO adk_sessions ...")
               await cursor.execute("INSERT INTO adk_events ...")

           await conn.commit()  # Commit transaction
       except Exception:
           await conn.rollback()  # Rollback on error
           raise

Performance Considerations
==========================

Connection Pool Tuning
-----------------------

**Optimal Pool Sizes:**

.. code-block:: python

   # Low traffic (< 100 concurrent users)
   pool_config = {"minsize": 5, "maxsize": 20}

   # Medium traffic (100-1000 concurrent users)
   pool_config = {"minsize": 20, "maxsize": 100}

   # High traffic (> 1000 concurrent users)
   pool_config = {"minsize": 50, "maxsize": 200}

**Connection Recycling:**

Prevent stale connections with ``pool_recycle``:

.. code-block:: python

   config = AsyncmyConfig(
       pool_config={
           "host": "mysql.example.com",
           "pool_recycle": 3600,  # Recycle after 1 hour
           # ...
       }
   )

JSON Performance
----------------

MySQL JSON queries benefit from virtual column indexing:

.. code-block:: sql

   -- Create virtual column for frequently queried JSON path
   ALTER TABLE adk_sessions
       ADD COLUMN dashboard_type VARCHAR(64)
       AS (JSON_UNQUOTE(JSON_EXTRACT(state, '$.dashboard'))) STORED;

   -- Index the virtual column
   CREATE INDEX idx_dashboard_type ON adk_sessions(dashboard_type);

   -- Now this query uses the index
   SELECT * FROM adk_sessions
   WHERE dashboard_type = 'sales';

InnoDB Optimization
-------------------

**Buffer Pool Size:**

For dedicated MySQL servers, set InnoDB buffer pool to 70-80% of RAM:

.. code-block:: ini

   # my.cnf
   [mysqld]
   innodb_buffer_pool_size = 8G
   innodb_log_file_size = 512M
   innodb_flush_log_at_trx_commit = 2  # Better performance, slight durability trade-off

**Query Cache:**

MySQL 8.0+ removed query cache. Use connection pooling instead:

.. code-block:: python

   # Proper connection pooling is more effective than query cache
   config = AsyncmyConfig(
       pool_config={"minsize": 20, "maxsize": 100}
   )

Index Usage Verification
-------------------------

.. code-block:: sql

   -- Check if queries use indexes
   EXPLAIN SELECT * FROM adk_sessions
   WHERE app_name = 'my_app' AND user_id = 'user_123';

   -- Should show:
   --   key: idx_adk_sessions_app_user
   --   type: ref

Best Practices
==============

MySQL vs MariaDB Considerations
--------------------------------

**MySQL 5.7.8+ vs MariaDB 10.2.7+:**

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Feature
     - MySQL 5.7.8+
     - MariaDB 10.2.7+
   * - JSON Type
     - Native JSON
     - Native JSON (compatible)
   * - Timestamp Precision
     - TIMESTAMP(6) (microseconds)
     - TIMESTAMP(6) (microseconds)
   * - JSON Functions
     - Extensive (JSON_EXTRACT, etc.)
     - Compatible subset
   * - Performance
     - Excellent
     - Excellent (often faster writes)

**Version Compatibility:**

.. code-block:: python

   # Check MySQL/MariaDB version
   async with config.provide_connection() as conn:
       async with conn.cursor() as cursor:
           await cursor.execute("SELECT VERSION()")
           version = await cursor.fetchone()
           print(f"Database version: {version[0]}")

           # Ensure JSON support
           if "MariaDB" in version[0]:
               assert "10.2" in version[0] or "10.3" in version[0] or "10.4" in version[0]
           else:
               assert "5.7" in version[0] or "8." in version[0]

UTF-8MB4 Character Set
----------------------

Always use ``utf8mb4`` for full Unicode support:

.. code-block:: python

   config = AsyncmyConfig(
       pool_config={
           "charset": "utf8mb4",  # NOT "utf8" (only 3 bytes)
           # ...
       }
   )

.. warning::

   **Never use ``charset='utf8'``** - it's a 3-byte encoding that cannot handle emoji
   and many international characters. Always use ``utf8mb4``.

Timezone Handling
-----------------

Force UTC timezone for consistency:

.. code-block:: python

   config = AsyncmyConfig(
       pool_config={
           "init_command": "SET time_zone='+00:00'",
           # ...
       }
   )

   # Python datetime objects should always be timezone-aware
   from datetime import datetime, timezone

   now = datetime.now(timezone.utc)  # Always use UTC

SSL/TLS Encryption
------------------

Enable SSL for production:

.. code-block:: python

   config = AsyncmyConfig(
       pool_config={
           "host": "mysql-prod.example.com",
           "ssl": {
               "ca": "/etc/ssl/certs/ca-cert.pem",
               "cert": "/etc/ssl/certs/client-cert.pem",
               "key": "/etc/ssl/keys/client-key.pem",
               "verify_mode": True,
           },
           # ...
       }
   )

Use Cases
=========

High-Concurrency Web Applications
----------------------------------

AsyncMy excels in async web frameworks:

.. code-block:: python

   # FastAPI / Litestar / Starlette integration
   from contextlib import asynccontextmanager
   from fastapi import FastAPI

   @asynccontextmanager
   async def lifespan(app: FastAPI):
       # Startup
       config = AsyncmyConfig(pool_config={...})
       await config.create_pool()
       yield
       # Shutdown
       await config.close_pool()

   app = FastAPI(lifespan=lifespan)

   @app.post("/sessions")
   async def create_session(app_name: str, user_id: str):
       store = AsyncmyADKStore(config)
       service = SQLSpecSessionService(store)
       session = await service.create_session(app_name, user_id, {})
       return {"session_id": session.id}

Multi-Tenant SaaS Applications
-------------------------------

Connection pooling with tenant isolation:

.. code-block:: python

   # Separate databases per tenant
   async def get_tenant_config(tenant_id: str) -> AsyncmyConfig:
       return AsyncmyConfig(
           pool_config={
               "host": "mysql.example.com",
               "database": f"tenant_{tenant_id}",
               "minsize": 5,
               "maxsize": 20,
           }
       )

   # Use tenant-specific store
   config = await get_tenant_config("acme_corp")
   store = AsyncmyADKStore(config)

Real-Time Conversation Systems
-------------------------------

Microsecond precision for event ordering:

.. code-block:: python

   from datetime import datetime, timezone

   # Events are stored with microsecond timestamps
   event_time = datetime.now(timezone.utc)  # Includes microseconds

   # Retrieve events with precise time filtering
   events = await store.get_events(
       session_id=session.id,
       after_timestamp=event_time,
       limit=100
   )

Existing MySQL Infrastructure
------------------------------

Leverage existing MySQL deployments:

.. code-block:: python

   # Connect to existing MySQL instance
   config = AsyncmyConfig(
       pool_config={
           "host": "existing-mysql.company.com",
           "port": 3306,
           "user": "agent_app",
           "password": "secure_password",
           "database": "ai_agents",
       }
   )

   # Use existing database, create tables if needed
   store = AsyncmyADKStore(config)
   await store.create_tables()  # Idempotent

Troubleshooting
===============

JSON Type Not Supported Error
------------------------------

.. code-block:: text

   asyncmy.errors.ProgrammingError: (1064, "You have an error in your SQL syntax...")

**Solution:** Upgrade to MySQL 5.7.8+ or MariaDB 10.2.7+:

.. code-block:: bash

   # Check version
   mysql --version

   # MySQL 5.6 or earlier -> upgrade to MySQL 5.7+ or 8.0+
   # MariaDB 10.1 or earlier -> upgrade to MariaDB 10.2+

Connection Pool Exhausted
--------------------------

.. code-block:: text

   asyncmy.errors.PoolError: Pool is full

**Solution:** Increase ``maxsize`` or check for connection leaks:

.. code-block:: python

   # Increase pool size
   config = AsyncmyConfig(
       pool_config={
           "maxsize": 100,  # Increase from default
           # ...
       }
   )

   # Always use context managers to ensure connections are released
   async with config.provide_connection() as conn:
       # Connection automatically released after this block
       ...

Timestamp Precision Loss
-------------------------

.. code-block:: text

   # Microseconds being truncated to seconds

**Solution:** Use ``TIMESTAMP(6)`` (not ``TIMESTAMP``):

.. code-block:: sql

   -- Check column definition
   SHOW CREATE TABLE adk_events;

   -- Should see: timestamp TIMESTAMP(6) NOT NULL
   -- If not, alter table:
   ALTER TABLE adk_events
       MODIFY COLUMN timestamp TIMESTAMP(6) NOT NULL;

Foreign Key Constraint Errors
------------------------------

.. code-block:: text

   asyncmy.errors.IntegrityError: (1452, 'Cannot add or update a child row...')

**Solution:** Ensure session exists before creating events:

.. code-block:: python

   # Always create session first
   session = await service.create_session("app", "user", {})

   # Then create events
   await service.append_event(session, event)

   # Verify session exists
   existing = await service.get_session("app", "user", session.id)
   if not existing:
       raise ValueError("Session not found")

Connection Timeout Errors
--------------------------

.. code-block:: text

   asyncmy.errors.OperationalError: (2003, "Can't connect to MySQL server...")

**Solution:** Check network connectivity and increase timeout:

.. code-block:: python

   config = AsyncmyConfig(
       pool_config={
           "host": "mysql.example.com",
           "connect_timeout": 30,  # Increase from default 10s
           # ...
       }
   )

UTF-8 Encoding Issues
---------------------

.. code-block:: text

   # Emoji or special characters not storing correctly

**Solution:** Always use ``utf8mb4`` charset:

.. code-block:: python

   config = AsyncmyConfig(
       pool_config={
           "charset": "utf8mb4",  # NOT "utf8"
           # ...
       }
   )

   # Verify database charset
   async with config.provide_connection() as conn:
       async with conn.cursor() as cursor:
           await cursor.execute("SHOW VARIABLES LIKE 'character_set%'")
           for row in await cursor.fetchall():
               print(row)

When to Use AsyncMy
===================

**Ideal For:**

✅ Production async web applications (FastAPI, Litestar, Starlette)

✅ High-concurrency AI agent deployments

✅ Existing MySQL/MariaDB infrastructure

✅ Multi-tenant SaaS applications

✅ Real-time conversation systems

✅ Applications requiring connection pooling

✅ Teams familiar with MySQL ecosystem

**Consider Alternatives When:**

❌ Need PostgreSQL-specific features (JSONB indexing, advanced types)

❌ Development/testing only (use DuckDB or SQLite)

❌ Analytics-heavy workloads (use DuckDB or BigQuery)

❌ Oracle-specific requirements (use OracleDB adapter)

❌ Require synchronous driver (use mysqlclient or PyMySQL)

Comparison: AsyncMy vs Other Adapters
--------------------------------------

.. list-table::
   :header-rows: 1
   :widths: 20 25 25 30

   * - Feature
     - AsyncMy (MySQL)
     - AsyncPG (PostgreSQL)
     - DuckDB
   * - Async Support
     - Native async
     - Native async
     - Sync only
   * - Connection Pool
     - Built-in
     - Built-in
     - N/A (embedded)
   * - JSON Type
     - JSON (not JSONB)
     - JSONB (indexed)
     - Native JSON
   * - Timestamp Precision
     - Microseconds (6)
     - Microseconds (6)
     - Microseconds
   * - Deployment
     - Client-server
     - Client-server
     - Embedded
   * - Best For
     - MySQL infrastructure
     - New projects, JSONB
     - Development, analytics

Example: Full Application
==========================

The AsyncMy flow mirrors :doc:`/examples/extensions/adk/basic_aiosqlite`. Replace the registry
configuration with ``AsyncmyConfig`` and use the AsyncMy ADK store to keep the rest of the code
identical (``create_session``, ``append_event``, ``list_sessions``).

API Reference
=============

.. autoclass:: sqlspec.adapters.asyncmy.adk.AsyncmyADKStore
   :members:
   :inherited-members:
   :show-inheritance:
   :no-index:

See Also
========

- :doc:`../quickstart` - Quick start guide
- :doc:`../adapters` - Adapter comparison
- :doc:`../schema` - Database schema details
- :doc:`/examples/adapters/psycopg/connect_sync` - Connection pattern reference (swap ``PsycopgConfig`` for ``AsyncmyConfig`` when targeting MySQL)
- `AsyncMy Documentation <https://github.com/long2ice/asyncmy>`_ - Official AsyncMy documentation
- `MySQL JSON Functions <https://dev.mysql.com/doc/refman/8.0/en/json-functions.html>`_ - MySQL JSON reference
- `MariaDB JSON Functions <https://mariadb.com/kb/en/json-functions/>`_ - MariaDB JSON reference
