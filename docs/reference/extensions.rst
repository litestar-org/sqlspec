==========
Extensions
==========

SQLSpec provides integration modules for popular web frameworks, enabling seamless database connectivity with dependency injection, lifecycle management, and framework-specific utilities.

.. currentmodule:: sqlspec.extensions

Overview
========

Available framework integrations:

- **Litestar** - Modern async Python web framework
- **FastAPI** - High-performance async web framework
- **Flask** - Traditional Python web framework
- **Sanic** - Async Python web framework
- **Starlette** - Lightweight ASGI framework
- **aiosql** - SQL file loading integration

Each extension provides:

- Configuration integration
- Dependency injection
- Lifecycle hooks (startup/shutdown)
- Session management
- Framework-specific utilities

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
      sql.add_config(AsyncpgConfig(
          pool_config={"dsn": "postgresql://localhost/db"}
      ))

      plugin = SQLSpecPlugin(sqlspec=sql)

      @get("/users")
      async def get_users(db: AsyncpgDriver) -> list[dict]:
          result = await db.select("SELECT * FROM users")
          return result.data

      app = Litestar(route_handlers=[get_users], plugins=[plugin])

Configuration
-------------

.. autoclass:: SQLSpecConfig
   :members:
   :undoc-members:
   :show-inheritance:

   Configuration class for Litestar SQLSpec plugin.

Session Backend
---------------

.. autoclass:: SQLSpecSessionBackend
   :members:
   :undoc-members:
   :show-inheritance:

   Session backend for Litestar using SQLSpec.

FastAPI Integration
===================

.. currentmodule:: sqlspec.extensions.fastapi

.. automodule:: sqlspec.extensions.fastapi
   :members:
   :undoc-members:
   :show-inheritance:

Flask Integration
=================

.. currentmodule:: sqlspec.extensions.flask

.. automodule:: sqlspec.extensions.flask
   :members:
   :undoc-members:
   :show-inheritance:

Sanic Integration
=================

.. currentmodule:: sqlspec.extensions.sanic

.. automodule:: sqlspec.extensions.sanic
   :members:
   :undoc-members:
   :show-inheritance:

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
