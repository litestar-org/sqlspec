:orphan:

===================
Litestar Extension
===================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   quickstart
   dependency_injection
   transactions
   session_stores
   api

Database integration for the Litestar ASGI framework with dependency injection, transaction management, and session storage.

Overview
========

The SQLSpec Litestar extension transforms SQLSpec into a first-class Litestar plugin, providing seamless integration with the `Litestar <https://litestar.dev>`_ web framework. This extension handles database lifecycle, dependency injection, and transaction management automatically.

This extension implements Litestar's plugin protocol, allowing database connections to be injected into route handlers, automatic transaction management based on HTTP status codes, and database-backed server-side session storage.

Key Features
============

Production Features
-------------------

- **Dependency Injection**: Automatic injection of connections, pools, and sessions
- **Transaction Management**: Three commit modes (manual, autocommit, autocommit with redirects)
- **Connection Pooling**: Built-in connection management via SQLSpec adapters
- **Async/Sync Support**: Works with async and sync Litestar handlers

Development Features
--------------------

- **Type Safety**: Full type hints for all injected dependencies
- **Multi-Database Support**: Configure multiple databases with unique dependency keys
- **CLI Integration**: Database management commands via Litestar CLI
- **Session Storage**: Database-backed session stores for server-side sessions

Performance Features
--------------------

- **Connection Reuse**: Efficient connection pooling per request
- **Statement Caching**: Automatically caches prepared statements
- **Request Correlation**: Track database queries by request ID
- **Graceful Shutdown**: Proper cleanup of database connections

Quick Example
=============

Here's a simple example of creating a Litestar application with SQLSpec integration:

.. code-block:: python

   from litestar import Litestar, get, post
   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
   from sqlspec.extensions.litestar import SQLSpecPlugin

   @get("/users")
   async def list_users(db_session: AsyncpgDriver) -> dict:
       result = await db_session.execute("SELECT * FROM users LIMIT 10")
       return {"users": result.all()}

   @post("/users")
   async def create_user(
       data: dict,
       db_session: AsyncpgDriver
   ) -> dict:
       result = await db_session.execute(
           "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
           data["name"],
           data["email"]
       )
       return result.one()

   # 1. Create SQLSpec instance
   spec = SQLSpec()

   # 2. Configure database
   db = spec.add_config(
       AsyncpgConfig(
           connection_config={"dsn": "postgresql://localhost/mydb"},
           extension_config={
               "litestar": {"commit_mode": "autocommit"}
           }
       )
   )

   # 3. Create Litestar app with plugin
   app = Litestar(
       route_handlers=[list_users, create_user],
       plugins=[SQLSpecPlugin(sqlspec=spec)]
   )

Architecture Overview
=====================

The extension follows a layered architecture:

.. code-block:: text

   ┌─────────────────────┐
   │   Litestar App      │
   └──────────┬──────────┘
              │
   ┌──────────▼──────────┐
   │  SQLSpecPlugin      │  ← Implements Litestar Plugin Protocol
   └──────────┬──────────┘
              │
   ┌──────────▼──────────┐
   │ Dependency Provider │  ← Injects connections, pools, sessions
   └──────────┬──────────┘
              │
   ┌──────────▼──────────┐
   │  SQLSpec Config     │  ← AsyncpgConfig, SqliteConfig, etc.
   └──────────┬──────────┘
              │
   ┌──────────▼──────────┐
   │    Database         │
   └─────────────────────┘

Layers:

1. **Plugin Layer** (``SQLSpecPlugin``): Implements Litestar's plugin protocol
2. **Dependency Layer**: Provides connections, pools, and sessions to handlers
3. **Config Layer**: Database configuration and connection pooling
4. **Database Layer**: Physical database connections

Multi-Database Applications
----------------------------

Connect to multiple databases with unique dependency keys:

Next Steps
==========

.. grid:: 2
   :gutter: 3

   .. grid-item-card:: 📦 Installation
      :link: installation
      :link-type: doc

      Install the extension and Litestar

   .. grid-item-card:: 🚀 Quick Start
      :link: quickstart
      :link-type: doc

      Get up and running in 5 minutes

   .. grid-item-card:: 💉 Dependency Injection
      :link: dependency_injection
      :link-type: doc

      Inject connections, pools, and sessions

   .. grid-item-card:: 🔄 Transactions
      :link: transactions
      :link-type: doc

      Transaction management patterns

   .. grid-item-card:: 🗄️ Session Stores
      :link: session_stores
      :link-type: doc

      Database-backed session storage

   .. grid-item-card:: 📚 API Reference
      :link: api
      :link-type: doc

      Complete API documentation

See Also
========

- :doc:`/usage/framework_integrations` - Framework integration guide
- :doc:`/reference/extensions` - SQLSpec extensions reference
- :doc:`/reference/adapters` - Database adapters documentation
- `Litestar Documentation <https://docs.litestar.dev>`_
