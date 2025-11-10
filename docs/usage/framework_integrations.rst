========================
Framework Integrations
========================

SQLSpec integrates seamlessly with popular Python web frameworks through plugins and dependency injection. This guide covers integration with Litestar, FastAPI, and other frameworks.

Overview
--------

SQLSpec provides framework-specific plugins that handle:

- Connection lifecycle management
- Dependency injection
- Transaction management
- Request-scoped sessions
- Automatic cleanup

Litestar Integration
--------------------

The Litestar plugin provides first-class integration with comprehensive features.

Basic Setup
^^^^^^^^^^^

.. code-block:: python

   from litestar import Litestar, get
   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.extensions.litestar import SQLSpecPlugin

   # Configure database and create plugin
   spec = SQLSpec()
   db = spec.add_config(
       AsyncpgConfig(
           pool_config={
               "dsn": "postgresql://localhost/mydb",
               "min_size": 10,
               "max_size": 20,
           }
       )
   )
   sqlspec_plugin = SQLSpecPlugin(sqlspec=spec)

   # Create Litestar app
   app = Litestar(
       route_handlers=[...],
       plugins=[sqlspec_plugin]
   )

Using Dependency Injection
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The plugin provides dependency injection for connections, pools, and sessions:

.. code-block:: python

   from litestar import get, post
   from sqlspec.driver import AsyncDriverAdapterBase

   # Inject database session
   @get("/users/{user_id:int}")
   async def get_user(
       user_id: int,
       db_session: AsyncDriverAdapterBase
   ) -> dict:
       result = await db_session.execute(
           "SELECT id, name, email FROM users WHERE id = $1",
           user_id
       )
       return result.one()

   # Inject connection pool
   @get("/health")
   async def health_check(db_pool) -> dict:
       async with db_pool.acquire() as conn:
           result = await conn.fetchval("SELECT 1")
           return {"status": "healthy" if result == 1 else "unhealthy"}

   # Inject raw connection
   @get("/stats")
   async def stats(db_connection) -> dict:
       result = await db_connection.fetchval("SELECT COUNT(*) FROM users")
       return {"user_count": result}

Commit Modes
^^^^^^^^^^^^

The plugin supports different transaction commit strategies configured via ``extension_config``:

**Manual Commit Mode (Default)**

You control transaction boundaries explicitly:

.. code-block:: python

   from litestar import post
   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.driver import AsyncDriverAdapterBase

   spec = SQLSpec()
   db = spec.add_config(
       AsyncpgConfig(
           pool_config={"dsn": "postgresql://..."},
           extension_config={
               "litestar": {"commit_mode": "manual"}  # Default
           }
       )
   )

   @post("/users")
   async def create_user(
       data: dict,
       db_session: AsyncDriverAdapterBase
   ) -> dict:
       async with db_session.begin_transaction():
           result = await db_session.execute(
               "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
               data["name"],
               data["email"]
           )
           return result.one()

**Autocommit Mode**

Automatically commits on successful requests (2xx responses):

.. code-block:: python

   spec = SQLSpec()
   db = spec.add_config(
       AsyncpgConfig(
           pool_config={"dsn": "postgresql://..."},
           extension_config={
               "litestar": {"commit_mode": "autocommit"}  # Auto-commit on 2xx
           }
       )
   )

   @post("/users")
   async def create_user(
       data: dict,
       db_session: AsyncDriverAdapterBase
   ) -> dict:
       # Transaction begins automatically
       result = await db_session.execute(
           "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
           data["name"],
           data["email"]
       )
       # Commits automatically on success
       return result.one()

**Autocommit with Redirects**

Commits on both 2xx and 3xx responses:

.. code-block:: python

   spec = SQLSpec()
   db = spec.add_config(
       AsyncpgConfig(
           pool_config={"dsn": "postgresql://..."},
           extension_config={
               "litestar": {"commit_mode": "autocommit_include_redirect"}
           }
       )
   )

Custom Dependency Keys
^^^^^^^^^^^^^^^^^^^^^^

Customize the dependency injection keys via ``extension_config``:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.driver import AsyncDriverAdapterBase

   spec = SQLSpec()
   db = spec.add_config(
       AsyncpgConfig(
           pool_config={"dsn": "postgresql://..."},
           extension_config={
               "litestar": {
                   "connection_key": "database",    # Default: "db_connection"
                   "pool_key": "db_pool",           # Default: "db_pool"
                   "session_key": "session",        # Default: "db_session"
               }
           }
       )
   )

   @get("/users")
   async def list_users(session: AsyncDriverAdapterBase) -> list:
       result = await session.execute("SELECT * FROM users")
       return result.all()

Multiple Database Configurations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The plugin supports multiple database configurations through a single SQLSpec instance:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.driver import AsyncDriverAdapterBase
   from sqlspec.extensions.litestar import SQLSpecPlugin

   spec = SQLSpec()

   # Main database
   main_db = spec.add_config(
       AsyncpgConfig(
           pool_config={"dsn": "postgresql://localhost/main"},
           extension_config={
               "litestar": {
                   "session_key": "main_db",
                   "connection_key": "main_db_connection",
               }
           }
       )
   )

   # Analytics database
   analytics_db = spec.add_config(
       AsyncpgConfig(
           pool_config={"dsn": "postgresql://localhost/analytics"},
           extension_config={
               "litestar": {
                   "session_key": "analytics_db",
                   "connection_key": "analytics_connection",
               }
           }
       )
   )

   # Create single plugin with all configs
   app = Litestar(
       plugins=[SQLSpecPlugin(sqlspec=spec)]
   )

   # Use in handlers
   @get("/report")
   async def generate_report(
       main_db: AsyncDriverAdapterBase,
       analytics_db: AsyncDriverAdapterBase
   ) -> dict:
       users = await main_db.execute("SELECT COUNT(*) FROM users")
       events = await analytics_db.execute("SELECT COUNT(*) FROM events")
       return {
           "total_users": users.scalar(),
           "total_events": events.scalar()
       }

Session Storage Backend
^^^^^^^^^^^^^^^^^^^^^^^

Use SQLSpec as a session backend for Litestar:

.. code-block:: python

   from litestar import Litestar
   from litestar.middleware.session.server_side import ServerSideSessionConfig
   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.asyncpg.litestar import AsyncpgStore
   from sqlspec.extensions.litestar import SQLSpecPlugin

   # Configure database with session support
   spec = SQLSpec()
   db = spec.add_config(
       AsyncpgConfig(
           pool_config={"dsn": "postgresql://localhost/db"},
           extension_config={
               "litestar": {"session_table": "litestar_sessions"}
           },
           migration_config={
               "script_location": "migrations",
               "include_extensions": ["litestar"]
           }
       )
   )

   # Create session store using adapter-specific class
   store = AsyncpgStore(db)

   # Configure Litestar with plugin and session middleware
   app = Litestar(
       plugins=[SQLSpecPlugin(sqlspec=spec)],
       middleware=[
           ServerSideSessionConfig(store=store).middleware
       ]
   )

CLI Integration
^^^^^^^^^^^^^^^

The plugin provides CLI commands for database management:

.. code-block:: bash

   # Generate migration
   litestar db migrations generate -m "Add users table"

   # Apply migrations (includes extension migrations)
   litestar db migrations upgrade

   # Rollback migration
   litestar db migrations downgrade

   # Show current migration version
   litestar db migrations current

   # Show migration history (verbose)
   litestar db migrations current --verbose

.. note::

   Extension migrations (like Litestar session tables) are included automatically when ``include_extensions`` contains ``"litestar"`` in your migration config.

Correlation Middleware
^^^^^^^^^^^^^^^^^^^^^^

Enable request correlation tracking via ``extension_config``:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig

   spec = SQLSpec()
   db = spec.add_config(
       AsyncpgConfig(
           pool_config={"dsn": "postgresql://..."},
           extension_config={
               "litestar": {
                   "enable_correlation_middleware": True,  # Default: True
                   "correlation_header": "x-request-id",
                   "correlation_headers": ["x-client-trace"],
                   "auto_trace_headers": True,
                }
            }
        )
    )

   # Queries will include correlation IDs in logs (header or generated UUID)
   # Format: [correlation_id=abc123] SELECT * FROM users

FastAPI Integration
-------------------

While SQLSpec doesn't have a dedicated FastAPI plugin, integration is straightforward using dependency injection.

Basic Setup
^^^^^^^^^^^

.. code-block:: python

   from fastapi import FastAPI, Depends
   from contextlib import asynccontextmanager
   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.driver import AsyncDriverAdapterBase

   # Configure database
   spec = SQLSpec()
   db = spec.add_config(
       AsyncpgConfig(
           pool_config={
               "dsn": "postgresql://localhost/mydb",
               "min_size": 10,
               "max_size": 20,
           }
       )
   )

   # Lifespan context manager
   @asynccontextmanager
   async def lifespan(app: FastAPI):
       # Startup
       yield
       # Shutdown
       await spec.close_all_pools()

   app = FastAPI(lifespan=lifespan)

Dependency Injection
^^^^^^^^^^^^^^^^^^^^

Create a dependency function for database sessions:

.. code-block:: python

   from typing import AsyncGenerator

   async def get_db_session() -> AsyncGenerator[AsyncDriverAdapterBase, None]:
       async with spec.provide_session(config) as session:
           yield session

   # Use in route handlers
   @app.get("/users/{user_id}")
   async def get_user(
       user_id: int,
       db: AsyncDriverAdapterBase = Depends(get_db_session)
   ) -> dict:
       result = await db.execute(
           "SELECT id, name, email FROM users WHERE id = $1",
           user_id
       )
       return result.one()

Transaction Management
^^^^^^^^^^^^^^^^^^^^^^

Implement transaction handling with FastAPI:

.. code-block:: python

   @app.post("/users")
   async def create_user(
       user_data: dict,
       db: AsyncDriverAdapterBase = Depends(get_db_session)
   ) -> dict:
       async with db.begin_transaction():
           result = await db.execute(
               "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
               user_data["name"],
               user_data["email"]
           )

           user_id = result.scalar()

           # Additional operations in same transaction
           await db.execute(
               "INSERT INTO audit_log (action, user_id) VALUES ($1, $2)",
               "user_created",
               user_id
           )

           return result.one()

Multiple Databases
^^^^^^^^^^^^^^^^^^

Support multiple databases with different dependencies:

.. code-block:: python

   # Main database
   main_db = spec.add_config(AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/main"}))

   # Analytics database
   analytics_db = spec.add_config(AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/analytics"}))

   # Dependency functions
   async def get_main_db():
       async with spec.provide_session(main_db) as session:
           yield session

   async def get_analytics_db():
       async with spec.provide_session(analytics_db) as session:
           yield session

   # Use in handlers
   @app.get("/report")
   async def generate_report(
       main_db: AsyncDriverAdapterBase = Depends(get_main_db),
       analytics_db: AsyncDriverAdapterBase = Depends(get_analytics_db)
   ) -> dict:
       users = await main_db.execute("SELECT COUNT(*) FROM users")
       events = await analytics_db.execute("SELECT COUNT(*) FROM events")
       return {
           "users": users.scalar(),
           "events": events.scalar()
       }

Sanic Integration
-----------------

Integrate SQLSpec with Sanic using listeners and app context.

Basic Setup
^^^^^^^^^^^

.. code-block:: python

   from sanic import Sanic, Request, json
   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig

   app = Sanic("MyApp")

   # Initialize SQLSpec
   spec = SQLSpec()
   db = spec.add_config(AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/db"}))

   # Store in app context
   app.ctx.sqlspec = spec
   app.ctx.db_config = db

   # Cleanup on shutdown
   @app.before_server_stop
   async def close_db(app, loop):
       await app.ctx.sqlspec.close_all_pools()

Using in Route Handlers
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   @app.get("/users/<user_id:int>")
   async def get_user(request: Request, user_id: int):
       async with request.app.ctx.sqlspec.provide_session(request.app.ctx.db_config) as db:
           result = await db.execute(
               "SELECT id, name, email FROM users WHERE id = $1",
               user_id
           )
           return json(result.one())

Middleware for Automatic Sessions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   @app.middleware("request")
   async def add_db_session(request):
       request.ctx.db = await request.app.ctx.sqlspec.provide_session(
           request.app.ctx.db_config
       ).__aenter__()

   @app.middleware("response")
   async def cleanup_db_session(request, response):
       if hasattr(request.ctx, "db"):
           await request.ctx.db.__aexit__(None, None, None)

   # Use in handlers
   @app.get("/users")
   async def list_users(request: Request):
       result = await request.ctx.db.execute("SELECT * FROM users")
       return json(result.rows)

Flask Integration
-----------------

Integrate SQLSpec with Flask using synchronous drivers.

Basic Setup
^^^^^^^^^^^

.. code-block:: python

   from flask import Flask, g
   from sqlspec import SQLSpec
   from sqlspec.adapters.sqlite import SqliteConfig

   app = Flask(__name__)

   # Initialize SQLSpec
   spec = SQLSpec()
   db = spec.add_config(SqliteConfig(pool_config={"database": "app.db"}))

Using Request Context
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   def get_db():
       if 'db' not in g:
           g.db = spec.provide_session(db).__enter__()
       return g.db

   @app.teardown_appcontext
   def close_db(error):
       db = g.pop('db', None)
       if db is not None:
           db.__exit__(None, None, None)

   # Use in routes
   @app.route('/users/<int:user_id>')
   def get_user(user_id):
       db = get_db()
       result = db.execute("SELECT * FROM users WHERE id = ?", user_id)
       return result.one()

Custom Integration Patterns
----------------------------

Context Manager Pattern
^^^^^^^^^^^^^^^^^^^^^^^

For frameworks without built-in dependency injection:

.. code-block:: python

   class DatabaseSession:
       def __init__(self, spec: SQLSpec, config):
           self.spec = spec
           self.config = config
           self.session = None

       async def __aenter__(self):
           self.session = await self.spec.provide_session(self.config).__aenter__()
           return self.session

       async def __aexit__(self, exc_type, exc_val, exc_tb):
           if self.session:
               await self.session.__aexit__(exc_type, exc_val, exc_tb)

   # Usage
   async with DatabaseSession(spec, config) as db:
       result = await db.execute("SELECT * FROM users")

Request-Scoped Sessions
^^^^^^^^^^^^^^^^^^^^^^^

Implement request-scoped database sessions:

.. code-block:: python

   import asyncio
   from contextvars import ContextVar

   db_session: ContextVar = ContextVar('db_session', default=None)

   async def get_session():
       session = db_session.get()
       if session is None:
           session = await spec.provide_session(config).__aenter__()
           db_session.set(session)
       return session

   async def cleanup_session():
       session = db_session.get()
       if session:
           await session.__aexit__(None, None, None)
           db_session.set(None)

Singleton Pattern
^^^^^^^^^^^^^^^^^

For simple applications with a single database:

.. code-block:: python

   class Database:
       _instance = None
       _spec = None
       _config = None

       def __new__(cls):
           if cls._instance is None:
               cls._instance = super().__new__(cls)
               cls._spec = SQLSpec()
               cls._config = cls._spec.add_config(
                   AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/db"})
               )
           return cls._instance

       async def session(self):
           return self._spec.provide_session(self._config)

   # Usage
   db = Database()
   async with await db.session() as session:
       result = await session.execute("SELECT * FROM users")

Best Practices
--------------

**1. Use Framework-Specific Plugins When Available**

.. code-block:: python

   # Prefer Litestar plugin over manual setup
   spec = SQLSpec()
   db = spec.add_config(AsyncpgConfig(pool_config={"dsn": "postgresql://..."}))
   app = Litestar(plugins=[SQLSpecPlugin(sqlspec=spec)])

**2. Always Clean Up Pools**

.. code-block:: python

   # FastAPI
   @asynccontextmanager
   async def lifespan(app: FastAPI):
       yield
       await spec.close_all_pools()

   # Sanic
   @app.before_server_stop
   async def close_pools(app, loop):
       await spec.close_all_pools()

**3. Use Dependency Injection**

.. code-block:: python

   # Inject sessions, not global instances
   async def get_db():
       async with spec.provide_session(config) as session:
           yield session

**4. Handle Transactions Appropriately**

.. code-block:: python

   # Use autocommit for simple CRUD
   spec = SQLSpec()
   db = spec.add_config(
       AsyncpgConfig(
           pool_config={"dsn": "postgresql://..."},
           extension_config={
               "litestar": {"commit_mode": "autocommit"}
           }
       )
   )

   # Manual transactions for complex operations
   async with db_session.begin_transaction():
       # Multiple operations
       pass

**5. Separate Database Logic**

.. code-block:: python

   # Good: Separate repository layer
   class UserRepository:
       def __init__(self, db: AsyncDriverAdapterBase):
           self.db = db

       async def get_user(self, user_id: int):
           result = await self.db.execute(
               "SELECT * FROM users WHERE id = $1",
               user_id
           )
           return result.one()

   # Use in handlers
   @app.get("/users/{user_id}")
   async def get_user(
       user_id: int,
       db: AsyncDriverAdapterBase = Depends(get_db)
   ):
       repo = UserRepository(db)
       return await repo.get_user(user_id)

Testing
-------

Testing with Framework Integration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   import pytest
   from sqlspec.adapters.sqlite import SqliteConfig

   @pytest.fixture
   async def test_db():
       spec = SQLSpec()
       db = spec.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

       async with spec.provide_session(db) as session:
           # Set up test schema
           await session.execute("""
               CREATE TABLE users (
                   id INTEGER PRIMARY KEY,
                   name TEXT NOT NULL
               )
           """)
           yield session

   async def test_create_user(test_db):
       result = await test_db.execute(
           "INSERT INTO users (name) VALUES ($1) RETURNING id",
           "Test User"
       )
       assert result.scalar() == 1

Next Steps
----------

- :doc:`../examples/index` - Complete framework integration examples
- :doc:`configuration` - Configure databases for production
- :doc:`drivers_and_querying` - Execute queries in framework handlers

See Also
--------

- :doc:`../reference/extensions` - Extension API reference
- `Litestar Documentation <https://docs.litestar.dev>`_
- `FastAPI Documentation <https://fastapi.tiangolo.com>`_
