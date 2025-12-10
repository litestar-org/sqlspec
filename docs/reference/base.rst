====
Base
====

The ``sqlspec.base`` module contains the main entry point for SQLSpec: the ``SQLSpec`` class that manages database configurations and provides session context managers.

.. currentmodule:: sqlspec.base

SQLSpec Registry
================

.. autoclass:: SQLSpec
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

   The main SQLSpec registry that manages database configurations and provides sessions.

   **Key responsibilities:**

   - Register database configurations
   - Manage connection pool lifecycles
   - Provide database sessions via context managers
   - Support multiple databases simultaneously

   **Example:**

   .. code-block:: python

      from sqlspec import SQLSpec
      from sqlspec.adapters.asyncpg import AsyncpgConfig

      sql = SQLSpec()
      config = sql.add_config(
          AsyncpgConfig(
              pool_config={"host": "localhost", "database": "mydb"}
          )
      )

      async with sql.provide_session(config) as session:
          result = await session.execute("SELECT * FROM users")

Configuration Types
===================

All database adapter configurations inherit from base protocol classes defined in ``sqlspec.config``.

Connection Pooling
==================

Connection pooling is configured via adapter-specific TypedDicts passed to the ``pool_config`` parameter.

Session Management
==================

Session Protocols
-----------------

Sessions are provided by driver adapter classes: ``SyncDriverAdapterBase`` and ``AsyncDriverAdapterBase``.

.. autoclass:: sqlspec.protocols.AsyncSessionProtocol
   :members:
   :undoc-members:
   :show-inheritance:

   Async version of SessionProtocol with async/await methods.

Context Managers
----------------

Sessions are provided via context managers that ensure proper resource cleanup:

.. code-block:: python

   # Sync session
   with sql.provide_session(config) as session:
       result = session.execute("SELECT 1")

   # Async session
   async with sql.provide_session(config) as session:
       result = await session.execute("SELECT 1")

Lifecycle Management
====================

Connection Pool Lifecycle
-------------------------

SQLSpec automatically manages connection pool lifecycles:

1. **Startup** - Pools created when first session requested
2. **Runtime** - Pools shared across sessions
3. **Shutdown** - Pools closed when SQLSpec instance destroyed

Manual lifecycle control:

.. code-block:: python

   sql = SQLSpec()
   config = sql.add_config(AsyncpgConfig(pool_config={...}))

   # Startup pools explicitly
   # Pools created lazily on first use

   # ... application runs ...

   # Shutdown pools explicitly
   await sql.close_all_pools()

Configuration Registry
======================

Multiple Database Support
-------------------------

SQLSpec supports multiple databases simultaneously:

.. code-block:: python

   sql = SQLSpec()

   # Add PostgreSQL
   pg_db = sql.add_config(AsyncpgConfig(...))

   # Add SQLite
   sqlite_db = sql.add_config(SqliteConfig(...))

   # Get sessions for each
   async with sql.provide_session(pg_db) as pg_session:
       users = await pg_session.select("SELECT * FROM users")

   async with sql.provide_session(sqlite_db) as sqlite_session:
       cache = await sqlite_session.select("SELECT * FROM cache")

Configuration Lookup
--------------------

Sessions are provided using the config instance returned from ``add_config``:

.. code-block:: python

   # Config instance IS the handle
   config = sql.add_config(AsyncpgConfig(pool_config={...}))
   async with sql.provide_session(config) as session:
       ...

See Also
========

- :doc:`/usage/configuration` - Configuration guide
- :doc:`/usage/drivers_and_querying` - Driver usage
- :doc:`adapters` - Database adapters
- :doc:`driver` - Driver implementation details
