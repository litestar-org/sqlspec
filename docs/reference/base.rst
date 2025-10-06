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
      config = AsyncpgConfig(
          pool_config={"host": "localhost", "database": "mydb"}
      )
      sql.add_config(config)

      async with sql.provide_session(config) as session:
          result = await session.execute("SELECT * FROM users")

Configuration Types
===================

.. autoclass:: SQLConfig
   :members:
   :undoc-members:
   :show-inheritance:

   Base class for all database adapter configurations.

   **Adapter-specific configs:**

   - :class:`sqlspec.adapters.asyncpg.AsyncpgConfig` - PostgreSQL (asyncpg)
   - :class:`sqlspec.adapters.psycopg.PsycopgConfig` - PostgreSQL (psycopg)
   - :class:`sqlspec.adapters.sqlite.SqliteConfig` - SQLite (sync)
   - :class:`sqlspec.adapters.aiosqlite.AiosqliteConfig` - SQLite (async)
   - :class:`sqlspec.adapters.duckdb.DuckDBConfig` - DuckDB
   - :class:`sqlspec.adapters.asyncmy.AsyncmyConfig` - MySQL
   - :class:`sqlspec.adapters.oracledb.OracleDBConfig` - Oracle
   - :class:`sqlspec.adapters.bigquery.BigQueryConfig` - BigQuery
   - :class:`sqlspec.adapters.adbc.ADBCConfig` - ADBC

Connection Pooling
==================

.. autoclass:: ConnectionPoolConfig
   :members:
   :undoc-members:
   :show-inheritance:

   Configuration for connection pooling behavior.

   **Supported by:**

   - AsyncPG (native pooling)
   - Psycopg (psycopg_pool)
   - Other adapters (where available)

   **Example:**

   .. code-block:: python

      from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgPoolConfig

      config = AsyncpgConfig(
          pool_config=AsyncpgPoolConfig(
              dsn="postgresql://user:pass@localhost/db",
              min_size=5,
              max_size=20,
              timeout=30.0
          )
      )

Session Management
==================

Session Protocols
-----------------

.. autoclass:: sqlspec.protocols.SessionProtocol
   :members:
   :undoc-members:
   :show-inheritance:

   Protocol defining the interface for database sessions.

   **Key methods:**

   - ``execute()`` - Execute SQL and return results
   - ``execute_many()`` - Execute SQL for multiple parameter sets
   - ``select()`` - Execute SELECT and return all rows
   - ``select_one()`` - Execute SELECT and return one row (error if none)
   - ``select_one_or_none()`` - Execute SELECT and return one row or None
   - ``select_value()`` - Execute SELECT and return a scalar value

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
   sql.add_config(config)

   # Startup pools explicitly
   await sql.on_startup()

   # ... application runs ...

   # Shutdown pools explicitly
   await sql.on_shutdown()

Configuration Registry
======================

Multiple Database Support
-------------------------

SQLSpec supports multiple databases simultaneously:

.. code-block:: python

   sql = SQLSpec()

   # Add PostgreSQL
   pg_config = AsyncpgConfig(...)
   sql.add_config(pg_config)

   # Add SQLite
   sqlite_config = SqliteConfig(...)
   sql.add_config(sqlite_config)

   # Get sessions for each
   async with sql.provide_session(pg_config) as pg_session:
       users = await pg_session.select("SELECT * FROM users")

   async with sql.provide_session(sqlite_config) as sqlite_session:
       cache = await sqlite_session.select("SELECT * FROM cache")

Configuration Lookup
--------------------

Configurations can be retrieved by type or instance:

.. code-block:: python

   # By config type
   async with sql.provide_session(AsyncpgConfig) as session:
       ...

   # By config instance
   async with sql.provide_session(config) as session:
       ...

   # Get config by type
   config = sql.get_config(AsyncpgConfig)

See Also
========

- :doc:`/usage/configuration` - Configuration guide
- :doc:`/usage/drivers_and_querying` - Driver usage
- :doc:`adapters` - Database adapters
- :doc:`driver` - Driver implementation details
