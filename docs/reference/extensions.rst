==========
Extensions
==========

SQLSpec provides integration modules for popular web frameworks and external services, enabling seamless database connectivity with dependency injection, lifecycle management, and framework-specific utilities.

.. currentmodule:: sqlspec.extensions

Overview
========

Available integrations:

**AI & ML:**

- **Google ADK** - Session and event storage for Google Agent Development Kit

**Web Frameworks:**

- **Litestar** - Modern async Python web framework
- **FastAPI** - High-performance async web framework
- **Flask** - Traditional Python web framework
- **Sanic** - Async Python web framework
- **Starlette** - Lightweight ASGI framework

**Data Tools:**

- **aiosql** - SQL file loading integration

Each extension provides:

- Configuration integration
- Dependency injection (where applicable)
- Lifecycle hooks (startup/shutdown)
- Session management
- Framework/service-specific utilities

Google ADK Integration
=======================

.. currentmodule:: sqlspec.extensions.adk

The ADK extension provides persistent session and event storage for the Google Agent Development Kit (ADK), enabling stateful AI agent applications with database-backed conversation history.

**Features:**

- Session state persistence across multiple database backends
- Event history storage with full ADK event model support
- Multi-tenant support with customizable table names
- Type-safe storage with TypedDicts
- Production-ready for PostgreSQL, MySQL, SQLite, Oracle

**Complete Documentation:**

See :doc:`/extensions/adk/index` for comprehensive documentation including:

- Installation and quickstart guides
- Complete API reference
- Database adapter details
- Schema reference
- Migration strategies
- Production examples

**Quick Example:**

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore
   from sqlspec.extensions.adk import SQLSpecSessionService

   config = AsyncpgConfig(connection_config={"dsn": "postgresql://..."})
   store = AsyncpgADKStore(config)
   await store.create_tables()

   service = SQLSpecSessionService(store)
   session = await service.create_session(
       app_name="my_agent",
       user_id="user123",
       state={"context": "initial"}
   )

Base Store Classes
------------------

.. autoclass:: BaseAsyncADKStore
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

   Abstract base class for async ADK session stores. See :doc:`/extensions/adk/api` for details.

.. autoclass:: BaseSyncADKStore
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

   Abstract base class for sync ADK session stores. See :doc:`/extensions/adk/api` for details.

Session Service
---------------

.. autoclass:: SQLSpecSessionService
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

   SQLSpec-backed implementation of Google ADK's BaseSessionService. See :doc:`/extensions/adk/api` for details.

Litestar Integration
====================

.. currentmodule:: sqlspec.extensions.litestar

The Litestar extension provides a plugin for SQLSpec integration with automatic dependency injection.

Plugin
------

.. autoclass:: SQLSpecPlugin
   :members:
   :undoc-members:
   :show-inheritance:

   Main plugin for Litestar integration.

   **Features:**

   - Automatic connection pool lifecycle
   - Dependency injection for drivers
   - Per-request session management
   - Transaction handling
   - Configuration from Litestar settings

   **Basic usage:**

   .. code-block:: python

      from litestar import Litestar, get
      from sqlspec import SQLSpec
      from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
      from sqlspec.extensions.litestar import SQLSpecPlugin

      sql = SQLSpec()
      db = sql.add_config(
          AsyncpgConfig(
              connection_config={"dsn": "postgresql://localhost/db"}
          )
      )

      plugin = SQLSpecPlugin(sqlspec=sql)

      @get("/users")
      async def get_users(db: AsyncpgDriver) -> list[dict]:
          result = await db.select("SELECT * FROM users")
          return result.all()

      app = Litestar(route_handlers=[get_users], plugins=[plugin])

Configuration
-------------



Session Backend
---------------

.. autoclass:: BaseSQLSpecStore
   :members:
   :undoc-members:
   :show-inheritance:

   Abstract base class for session storage backends.

Starlette Integration
=====================

.. currentmodule:: sqlspec.extensions.starlette

.. automodule:: sqlspec.extensions.starlette
   :members:
   :undoc-members:
   :show-inheritance:

aiosql Integration
==================

.. currentmodule:: sqlspec.extensions.aiosql

.. automodule:: sqlspec.extensions.aiosql
   :members:
   :undoc-members:
   :show-inheritance:

See Also
========

- :doc:`/usage/framework_integrations` - Framework integration guide
- :doc:`/examples/index` - Framework integration examples
- :doc:`base` - SQLSpec configuration
- :doc:`adapters` - Database adapters
