===============
Session Stores
===============

SQLSpec provides database-backed session stores that implement the Litestar ``Store`` protocol for server-side session management.

Overview
========

Database-backed session stores enable:

- **Persistent Sessions**: Sessions survive application restarts
- **Distributed Applications**: Share sessions across multiple servers
- **Security**: Server-side storage prevents tampering
- **Scalability**: Handle millions of sessions efficiently

Available Stores
================

.. list-table::
   :header-rows: 1
   :widths: 20 20 60

   * - Adapter
     - Store Class
     - Features
   * - AsyncPG
     - ``AsyncpgStore``
     - JSONB storage, UPSERT, partial indexes
   * - Aiosqlite
     - ``AiosqliteStore``
     - JSON storage, simple schema
   * - OracleDB
     - ``OracledbStore``
     - BLOB storage, Oracle optimizations

Quick Start
===========

Basic Setup
-----------

.. code-block:: python

   from litestar import Litestar
   from litestar.middleware.session.server_side import ServerSideSessionConfig
   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.asyncpg.litestar import AsyncpgStore
   from sqlspec.extensions.litestar import SQLSpecPlugin

   # 1. Create SQLSpec instance
   spec = SQLSpec()

   # 2. Add database configuration
   db = spec.add_config(
       AsyncpgConfig(
           pool_config={"dsn": "postgresql://localhost/mydb"},
           extension_config={"litestar": {"session_table": "litestar_sessions"}},
       )
   )

   # 3. Create session store (uses the config instance)
   store = AsyncpgStore(db)

   # 4. Configure Litestar application
   app = Litestar(
       plugins=[SQLSpecPlugin(sqlspec=spec)],
       middleware=[
           ServerSideSessionConfig(store=store).middleware
       ]
   )

.. important::

   **Initialization Order**:

   1. Create ``SQLSpec()`` instance
   2. Add database config with ``spec.add_config()``
   3. Create session store with the config instance
   4. Pass ``SQLSpecPlugin(sqlspec=spec)`` to Litestar
   5. Add session middleware with the store

   The store requires a config instance returned from ``add_config()``, not the ``SQLSpec`` instance itself.

Using Sessions
==============

Store Session Data
------------------

.. code-block:: python

   from litestar import post
   from litestar.connection import ASGIConnection
   from litestar.enums import RequestEncodingType
   from litestar.params import Body

   @post("/login")
   async def login(
       data: dict = Body(media_type=RequestEncodingType.JSON),
       connection: ASGIConnection = None
   ) -> dict:
       # Validate user credentials
       user_id = authenticate(data["username"], data["password"])

       # Store in session
       connection.set_session({
           "user_id": user_id,
           "username": data["username"],
           "roles": ["user"]
       })

       return {"status": "logged in"}

Retrieve Session Data
---------------------

.. code-block:: python

   from litestar import get

   @get("/profile")
   async def profile(connection: ASGIConnection) -> dict:
       session = connection.session

       if not session.get("user_id"):
           return {"error": "Not authenticated"}, 401

       return {
           "user_id": session["user_id"],
           "username": session["username"],
           "roles": session["roles"]
       }

Clear Session
-------------

.. code-block:: python

   @post("/logout")
   async def logout(connection: ASGIConnection) -> dict:
       connection.clear_session()
       return {"status": "logged out"}

Session Expiration
==================

Configure automatic session expiration:

.. code-block:: python

   from datetime import timedelta
   from litestar.middleware.session.server_side import ServerSideSessionConfig

   config = ServerSideSessionConfig(
       store=store,
       max_age=timedelta(hours=24),  # Sessions expire after 24 hours
   )

Cleanup Expired Sessions
=========================

Manual Cleanup
--------------

.. code-block:: python

   import anyio
   from sqlspec.adapters.asyncpg.litestar import AsyncpgStore

   async def cleanup_sessions(store: AsyncpgStore):
       count = await store.delete_expired()
       print(f"Deleted {count} expired sessions")

   # Run cleanup
   anyio.run(cleanup_sessions, store)

CLI Cleanup
-----------

.. code-block:: bash

   # Using Litestar CLI
   litestar sessions delete-expired
   litestar sessions delete-expired --verbose

Scheduled Cleanup (Cron)
-------------------------

.. code-block:: bash

   # Add to crontab for hourly cleanup
   0 * * * * cd /app && litestar sessions delete-expired

Database Schema
===============

PostgreSQL (AsyncPG)
--------------------

.. code-block:: sql

   CREATE TABLE litestar_session (
       session_id TEXT PRIMARY KEY,
       data BYTEA NOT NULL,
       expires_at TIMESTAMPTZ,
       created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
       updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
   ) WITH (fillfactor = 80);

   CREATE INDEX idx_litestar_session_expires_at
   ON litestar_session(expires_at) WHERE expires_at IS NOT NULL;

Features:

- ``TIMESTAMPTZ`` for timezone-aware expiration
- Partial index on ``expires_at`` for efficient cleanup
- Fill factor 80 for HOT updates, reducing bloat
- Audit columns for debugging

SQLite (Aiosqlite)
------------------

.. code-block:: sql

   CREATE TABLE IF NOT EXISTS litestar_session (
       session_id TEXT PRIMARY KEY,
       data BLOB NOT NULL,
       expires_at INTEGER,
       created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
       updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
   );

   CREATE INDEX IF NOT EXISTS idx_litestar_session_expires_at
   ON litestar_session(expires_at) WHERE expires_at IS NOT NULL;

Oracle Database (OracleDB)
---------------------------

.. code-block:: sql

   CREATE TABLE litestar_session (
       session_id VARCHAR2(255) PRIMARY KEY,
       data BLOB NOT NULL,
       expires_at TIMESTAMP WITH TIME ZONE,
       created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
       updated_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
   );

   CREATE INDEX idx_litestar_session_expires_at
   ON litestar_session(expires_at);

Features:

- ``TIMESTAMP WITH TIME ZONE`` for global timezone awareness
- ``BLOB`` for efficient binary session data storage
- ``MERGE`` statements for atomic UPSERT operations
- Audit columns with ``SYSTIMESTAMP`` defaults

Store Configuration
===================

Custom Table Name
-----------------

Configure custom table names via ``extension_config``:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.asyncpg.litestar import AsyncpgStore

   # Create SQLSpec instance and add configuration
   spec = SQLSpec()
   config = spec.add_config(
       AsyncpgConfig(
           pool_config={"dsn": "postgresql://localhost/mydb"},
           extension_config={
               "litestar": {
                   "session_table": "custom_sessions"
               }
           }
       )
   )

   store = AsyncpgStore(config)

Oracle In-Memory Sessions (Enterprise Feature)
-----------------------------------------------

Oracle Database In-Memory Column Store can dramatically improve session lookup performance for high-traffic applications. When enabled, session tables are stored in columnar format in memory for 10-100x faster reads.

.. warning::

   **Licensing Required**: Oracle Database In-Memory is a **separately licensed option** for Oracle Database Enterprise Edition:

   - Oracle Database 12.1.0.2 or higher required
   - Oracle Database In-Memory option license ($23,000 per processor)
   - Sufficient ``INMEMORY_SIZE`` configured in the database instance

   Using ``in_memory=True`` without proper licensing will result in **ORA-00439** or **ORA-62142** errors.

Configuration
~~~~~~~~~~~~~

Enable In-Memory for Oracle session stores via ``extension_config``:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.oracledb import OracleAsyncConfig
   from sqlspec.adapters.oracledb.litestar import OracleAsyncStore
   from litestar import Litestar
   from litestar.middleware.session.server_side import ServerSideSessionConfig
   from sqlspec.extensions.litestar import SQLSpecPlugin

   # Configure Oracle with In-Memory enabled
   spec = SQLSpec()
   config = spec.add_config(
       OracleAsyncConfig(
           pool_config={
               "user": "app_user",
               "password": "secure_password",
               "dsn": "oracle.example.com:1521/XEPDB1",
               "min": 5,
               "max": 20,
           },
           extension_config={
               "litestar": {
                   "session_table": "app_sessions",
                   "in_memory": True  # Enable In-Memory Column Store
               }
           }
       )
   )

   # Create In-Memory session store
   store = OracleAsyncStore(config)

   # Configure Litestar application
   app = Litestar(
       plugins=[SQLSpecPlugin(sqlspec=spec)],
       middleware=[
           ServerSideSessionConfig(store=store).middleware
       ]
   )

**Generated DDL:**

.. code-block:: sql

   CREATE TABLE app_sessions (
       session_id VARCHAR2(255) PRIMARY KEY,
       data BLOB NOT NULL,
       expires_at TIMESTAMP WITH TIME ZONE,
       created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
       updated_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
   ) INMEMORY;

Performance Benefits
~~~~~~~~~~~~~~~~~~~~

In-Memory Column Store significantly improves session operations:

- **Session lookups**: 10-50x faster for ``get()`` operations
- **Expiration queries**: Faster ``delete_expired()`` execution
- **Concurrent reads**: Reduced I/O contention for high-traffic sites
- **Real-time analytics**: Fast session counting and user analytics

**Use Cases:**

✅ **High-traffic web applications**
   - Thousands of concurrent users
   - Frequent session read operations
   - Session-heavy workloads

✅ **Real-time session analytics**
   - Active user counting
   - Session duration tracking
   - User behavior analysis

❌ **Small applications** (< 1,000 concurrent users)
   - Overhead not justified
   - Standard indexes sufficient

❌ **Budget constraints**
   - In-Memory license costs $23K+ per processor

Database Requirements
~~~~~~~~~~~~~~~~~~~~~

**Oracle Version**: Oracle Database 12.1.0.2+ (19c+ recommended)

**Instance Configuration**: Configure ``INMEMORY_SIZE``:

.. code-block:: sql

   -- Check current setting
   SELECT value FROM v$parameter WHERE name = 'inmemory_size';

   -- Set INMEMORY_SIZE (requires restart)
   ALTER SYSTEM SET INMEMORY_SIZE=1G SCOPE=SPFILE;
   -- Restart database

**Recommended Size**: 500 MB - 2 GB for session stores.

Verification
~~~~~~~~~~~~

Verify In-Memory status after table creation:

.. code-block:: python

   from sqlspec.adapters.oracledb import OracleAsyncConfig

   config = OracleAsyncConfig(pool_config={"dsn": "..."})

   async with config.provide_connection() as conn:
       cursor = conn.cursor()

       # Check In-Memory status
       await cursor.execute("""
           SELECT table_name, inmemory
           FROM user_tables
           WHERE table_name = 'APP_SESSIONS'
       """)

       row = await cursor.fetchone()
       print(f"Table: {row[0]}, In-Memory: {row[1]}")

**Expected Output:**

.. code-block:: text

   Table: APP_SESSIONS, In-Memory: ENABLED

Troubleshooting
~~~~~~~~~~~~~~~

**ORA-00439: Feature not enabled: Database In-Memory**

**Solution**: Verify In-Memory license and configure ``INMEMORY_SIZE``:

.. code-block:: sql

   ALTER SYSTEM SET INMEMORY_SIZE=1G SCOPE=SPFILE;
   -- Restart database

**ORA-62142: INMEMORY column store not available**

**Solution**: Same as ORA-00439 - configure ``INMEMORY_SIZE`` and restart.

Implementation Differences
==========================

.. list-table::
   :header-rows: 1
   :widths: 20 20 20 40

   * - Feature
     - AsyncPG
     - Aiosqlite
     - OracleDB
   * - Storage Type
     - BYTEA
     - BLOB
     - BLOB
   * - Timestamp Type
     - TIMESTAMPTZ
     - INTEGER (Unix)
     - TIMESTAMP
   * - UPSERT
     - ON CONFLICT
     - REPLACE INTO
     - MERGE
   * - Partial Index
     - ✓
     - ✓
     - ✗ (filtered)
   * - Fill Factor
     - ✓
     - ✗
     - ✗

Best Practices
==============

Use Appropriate Max Age
------------------------

.. code-block:: python

   from datetime import timedelta

   # Short-lived sessions for sensitive operations
   auth_config = ServerSideSessionConfig(
       store=store,
       max_age=timedelta(minutes=30)
   )

   # Longer sessions for standard applications
   app_config = ServerSideSessionConfig(
       store=store,
       max_age=timedelta(days=7)
   )

Regular Cleanup
---------------

Schedule automated cleanup to prevent table bloat:

.. code-block:: bash

   # Hourly cleanup (crontab)
   0 * * * * cd /app && litestar sessions delete-expired

Secure Session Data
-------------------

.. code-block:: python

   # Don't store sensitive data in sessions
   # BAD
   connection.set_session({
       "password": user_password,  # Don't do this!
       "credit_card": card_number   # Don't do this!
   })

   # GOOD
   connection.set_session({
       "user_id": user_id,
       "username": username,
       "roles": roles
   })

Migration Management
====================

Session tables can be managed via SQLSpec migrations. The configuration must be added properly through the SQLSpec instance:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.extensions.litestar import SQLSpecPlugin

   # Configure database with extension and migration settings
   spec = SQLSpec()
   db = spec.add_config(
       AsyncpgConfig(
           pool_config={"dsn": "postgresql://localhost/mydb"},
           extension_config={
               "litestar": {"session_table": "custom_sessions"}
           },
           migration_config={
               "script_location": "migrations",
               "include_extensions": ["litestar"]
           }
       )
   )

   # Create Litestar app with plugin
   app = Litestar(
       plugins=[SQLSpecPlugin(sqlspec=spec)],
       middleware=[...]
   )

.. note::

   **Extension Migration Prefixes**: Litestar session migrations are automatically versioned with the ``ext_litestar_`` prefix (e.g., ``ext_litestar_0001``, ``ext_litestar_0002``). This prevents version conflicts with your application migrations.

   **Extension vs Application Migrations**:
   - Application migrations: ``0001_initial.py`` → version ``0001``
   - Litestar extension migrations: ``0001_create_session.py`` → version ``ext_litestar_0001``

Generate and apply migrations:

.. code-block:: bash

   # Generate migration
   litestar db migrations generate -m "add session storage"

   # Apply migrations (includes extension migrations)
   litestar db migrations upgrade

   # Check migration status
   litestar db migrations current --verbose

See Also
========

- :doc:`quickstart` - Get started with Litestar integration
- :doc:`api` - Complete API reference
- `Litestar Session Middleware <https://docs.litestar.dev/latest/usage/middleware/builtin-middleware.html#session-middleware>`_
