=====================
Dependency Injection
=====================

The SQLSpec Litestar plugin provides automatic dependency injection for database connections, pools, and sessions into route handlers.

Overview
========

The plugin registers three types of dependencies for each database configuration:

1. **Connection** (``db_connection``) - Raw database connection from the driver
2. **Pool** (``db_pool``) - Application-level connection pool
3. **Session** (``db_session``) - SQLSpec driver instance with query capabilities

Available Dependencies
======================

Connection Dependency
---------------------

Injects the raw database connection from the underlying driver.

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConnection
   from litestar import get

   @get("/raw")
   async def raw_query(db_connection: AsyncpgConnection) -> dict:
       result = await db_connection.fetch("SELECT * FROM users")
       return {"users": [dict(row) for row in result]}

**When to use**: Driver-specific features not exposed by SQLSpec.

**Key**: Configured via ``connection_key`` (default: ``"db_connection"``)

Pool Dependency
---------------

Injects the connection pool for monitoring or custom connection management.

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgPool
   from litestar import get

   @get("/pool-stats")
   async def pool_stats(db_pool: AsyncpgPool) -> dict:
       return {
           "size": db_pool.get_size(),
           "free": db_pool.get_idle_size()
       }

**When to use**: Pool monitoring or custom connection management.

**Key**: Configured via ``pool_key`` (default: ``"db_pool"``)

Session Dependency
------------------

Injects the SQLSpec driver instance with full query capabilities (recommended).

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgDriver
   from litestar import get

   @get("/users")
   async def get_users(db_session: AsyncpgDriver) -> dict:
       result = await db_session.execute("SELECT * FROM users")
       return {"users": result.all()}

**When to use**: All standard database operations (recommended).

**Key**: Configured via ``session_key`` (default: ``"db_session"``)

Dependency Resolution
=====================

By Type Annotation
------------------

Dependencies are resolved by type annotation:

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgDriver
   from litestar import get

   @get("/users")
   async def handler(db_session: AsyncpgDriver) -> dict:
       # SQLSpec injects AsyncpgDriver instance
       result = await db_session.execute("SELECT * FROM users")
       return {"users": result.all()}

By Dependency Key
-----------------

For multi-database setups, use custom dependency keys:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver

   spec = SQLSpec()

   # Primary database
   primary = spec.add_config(
       AsyncpgConfig(
           connection_config={"dsn": "postgresql://localhost/primary"},
           extension_config={
               "litestar": {"session_key": "primary_session"}
           }
       )
   )

   # Analytics database
   analytics = spec.add_config(
       AsyncpgConfig(
           connection_config={"dsn": "postgresql://localhost/analytics"},
           extension_config={
               "litestar": {"session_key": "analytics_session"}
           }
       )
   )

   @get("/report")
   async def report(
       primary_session: AsyncpgDriver,
       analytics_session: AsyncpgDriver
   ) -> dict:
       users = await primary_session.execute("SELECT COUNT(*) FROM users")
       events = await analytics_session.execute("SELECT COUNT(*) FROM events")
       return {"users": users.scalar(), "events": events.scalar()}

Configuration
=============

Customize dependency keys via ``extension_config``:

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig

   config = AsyncpgConfig(
       connection_config={"dsn": "postgresql://localhost/mydb"},
       extension_config={
           "litestar": {
               "connection_key": "db_connection",  # Raw connection key
               "pool_key": "db_pool",              # Pool key
               "session_key": "db_session"         # Session key (recommended)
           }
       }
   )

Multi-Database Configuration
=============================

Configure multiple databases with unique dependency keys:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.duckdb import DuckDBConfig

   spec = SQLSpec()

   # Primary PostgreSQL database
   primary = spec.add_config(
       AsyncpgConfig(
           connection_config={"dsn": "postgresql://localhost/app"},
           extension_config={
               "litestar": {
                   "connection_key": "primary_connection",
                   "session_key": "primary_session"
               }
           }
       )
   )

   # Analytics DuckDB database
   analytics = spec.add_config(
       DuckDBConfig(
           extension_config={
               "litestar": {
                   "connection_key": "analytics_connection",
                   "session_key": "analytics_session"
               }
           }
       )
   )

Usage:

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgDriver
   from sqlspec.adapters.duckdb import DuckDBDriver

   @get("/combined")
   async def combined(
       primary_session: AsyncpgDriver,
       analytics_session: DuckDBDriver
   ) -> dict:
       # Query primary database
       users = await primary_session.execute("SELECT COUNT(*) FROM users")

       # Query analytics database
       events = await analytics_session.execute("SELECT COUNT(*) FROM events")

       return {
           "users": users.scalar(),
           "events": events.scalar()
       }

Type-Safe Dependencies
======================

Use specific driver types for better type checking:

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgDriver
   from sqlspec.adapters.duckdb import DuckDBDriver

   @get("/report")
   async def report(
       postgres: AsyncpgDriver,
       duckdb: DuckDBDriver
   ) -> dict:
       # IDE knows exact driver types
       pg_result = await postgres.execute("SELECT * FROM users")
       duck_result = await duckdb.execute("SELECT * FROM events")
       return {"pg": pg_result.all(), "duck": duck_result.all()}

Best Practices
==============

Use Sessions Over Connections
------------------------------

Prefer ``db_session`` for standard database operations:

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgDriver, AsyncpgConnection, AsyncpgPool

   # Recommended: Use session
   @get("/users")
   async def get_users(db_session: AsyncpgDriver) -> dict:
       result = await db_session.execute("SELECT * FROM users")
       return {"users": result.all()}

   # Advanced: Use connection only when needed
   @get("/bulk-import")
   async def bulk_import(db_connection: AsyncpgConnection) -> dict:
       # Use driver-specific features
       await db_connection.copy_records_to_table(
           table_name="users",
           records=[(1, "Alice"), (2, "Bob")]
       )
       return {"status": "imported"}

   # Advanced: Use pool for custom connection management
   @get("/custom-query")
   async def custom_query(db_pool: AsyncpgPool) -> dict:
       # Manually acquire connection from pool
       async with db_pool.acquire() as conn:
           result = await conn.fetchval("SELECT COUNT(*) FROM users")
       return {"count": result}

Unique Keys for Multiple Databases
-----------------------------------

Always use unique dependency keys for multiple databases:

.. code-block:: python

   # Good: Unique keys
   db1 = spec.add_config(
       AsyncpgConfig(
           extension_config={"litestar": {"session_key": "db1_session"}}
       )
   )
   db2 = spec.add_config(
       DuckDBConfig(
           extension_config={"litestar": {"session_key": "db2_session"}}
       )
   )

   # Bad: Same keys (will raise error)
   db1 = spec.add_config(
       AsyncpgConfig(
           extension_config={"litestar": {"session_key": "db_session"}}
       )
   )
   db2 = spec.add_config(
       DuckDBConfig(
           extension_config={"litestar": {"session_key": "db_session"}}
       )
   )

Explicit Type Annotations
--------------------------

Always provide explicit type annotations:

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgDriver

   # Good: Explicit type
   @get("/users")
   async def get_users(db_session: AsyncpgDriver) -> dict:
       ...

   # Bad: No type annotation
   @get("/users")
   async def get_users(db_session) -> dict:
       # Dependency injection won't work!
       ...

See Also
========

- :doc:`quickstart` - Get started with dependency injection
- :doc:`transactions` - Transaction management with dependencies
- :doc:`api` - Complete API reference
- :doc:`/reference/driver` - Driver API documentation
