===========
Quick Start
===========

This guide will get you up and running with the SQLSpec Litestar extension in 5 minutes.

Overview
========

In this quickstart, you'll:

1. Install SQLSpec with Litestar support
2. Configure a database connection
3. Create a Litestar application with the SQLSpec plugin
4. Use dependency injection to access the database
5. Execute queries in route handlers

Prerequisites
=============

Ensure you have installed:

- SQLSpec with a database adapter (see :doc:`installation`)
- Litestar web framework

.. code-block:: bash

   pip install sqlspec[asyncpg,litestar]

Step 1: Import Required Modules
================================

.. code-block:: python

   from litestar import Litestar, get, post
   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
   from sqlspec.extensions.litestar import SQLSpecPlugin

Step 2: Configure Database
===========================

Create a SQLSpec instance and add a database configuration:

.. code-block:: python

   spec = SQLSpec()
   db = spec.add_config(
       AsyncpgConfig(
           pool_config={
               "dsn": "postgresql://user:password@localhost:5432/mydb",
               "min_size": 5,
               "max_size": 20
           },
           extension_config={
               "litestar": {
                   "commit_mode": "autocommit"
               }
           }
       )
   )

.. note::

   Connection strings vary by database. See :doc:`dependency_injection` for examples for each database.

For local development with SQLite:

.. code-block:: python

   from sqlspec.adapters.aiosqlite import AiosqliteConfig

   db = spec.add_config(
       AiosqliteConfig(
           pool_config={"database": "./myapp.db"},
           extension_config={
               "litestar": {"commit_mode": "autocommit"}
           }
       )
   )

Step 3: Create Route Handlers
==============================

Define route handlers that use dependency injection to access the database:

.. code-block:: python

   @get("/users")
   async def list_users(db_session: AsyncpgDriver) -> dict:
       result = await db_session.execute("SELECT * FROM users LIMIT 10")
       return {"users": result.data}

   @get("/users/{user_id:int}")
   async def get_user(
       user_id: int,
       db_session: AsyncpgDriver
   ) -> dict:
       result = await db_session.execute(
           "SELECT * FROM users WHERE id = $1",
           user_id
       )
       return result.one()

   @post("/users")
   async def create_user(
       data: dict,
       db_session: AsyncpgDriver
   ) -> dict:
       result = await db_session.execute(
           "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, name, email",
           data["name"],
           data["email"]
       )
       return result.one()

Step 4: Create the Litestar App
================================

Register the SQLSpec plugin with your Litestar application:

.. code-block:: python

   plugin = SQLSpecPlugin(sqlspec=spec)

   app = Litestar(
       route_handlers=[list_users, get_user, create_user],
       plugins=[plugin]
   )

.. tip::

   The plugin automatically handles database lifecycle management including connection pooling,
   transaction management, and graceful shutdown.

Step 5: Run the Application
============================

Run your Litestar application:

.. code-block:: bash

   litestar run

You should see output similar to:

.. code-block:: text

   INFO:     Started server process [12345]
   INFO:     Waiting for application startup.
   INFO:     Application startup complete.
   INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)

Complete Example
================

Here's a complete working example:

.. code-block:: python

   from litestar import Litestar, get, post
   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
   from sqlspec.extensions.litestar import SQLSpecPlugin

   # Configure database
   spec = SQLSpec()
   db = spec.add_config(
       AsyncpgConfig(
           pool_config={
               "dsn": "postgresql://user:password@localhost:5432/mydb",
               "min_size": 5,
               "max_size": 20
           },
           extension_config={
               "litestar": {"commit_mode": "autocommit"}
           }
       )
   )

   # Route handlers
   @get("/users")
   async def list_users(db_session: AsyncpgDriver) -> dict:
       result = await db_session.execute(
           "SELECT id, name, email FROM users ORDER BY id LIMIT 10"
       )
       return {"users": result.data}

   @get("/users/{user_id:int}")
   async def get_user(
       user_id: int,
       db_session: AsyncpgDriver
   ) -> dict:
       result = await db_session.execute(
           "SELECT id, name, email FROM users WHERE id = $1",
           user_id
       )
       return result.one()

   @post("/users")
   async def create_user(
       data: dict,
       db_session: AsyncpgDriver
   ) -> dict:
       result = await db_session.execute(
           "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, name, email",
           data["name"],
           data["email"]
       )
       return result.one()

   # Create Litestar app
   plugin = SQLSpecPlugin(sqlspec=spec)
   app = Litestar(
       route_handlers=[list_users, get_user, create_user],
       plugins=[plugin]
   )

Testing the API
===============

Once your application is running, test the endpoints:

.. code-block:: bash

   # List users
   curl http://localhost:8000/users

   # Get specific user
   curl http://localhost:8000/users/1

   # Create user
   curl -X POST http://localhost:8000/users \
        -H "Content-Type: application/json" \
        -d '{"name": "Alice", "email": "alice@example.com"}'

Type-Safe Results
=================

For type-safe results, define Pydantic models:

.. code-block:: python

   from pydantic import BaseModel

   class User(BaseModel):
       id: int
       name: str
       email: str

   @get("/users/{user_id:int}")
   async def get_user(
       user_id: int,
       db_session: AsyncpgDriver
   ) -> User:
       result = await db_session.execute(
           "SELECT id, name, email FROM users WHERE id = $1",
           user_id,
           schema_type=User
       )
       return result.one()

Now your IDE provides autocomplete and type checking for the returned user!

Database Setup
==============

Create the users table:

.. code-block:: sql

   CREATE TABLE users (
       id SERIAL PRIMARY KEY,
       name TEXT NOT NULL,
       email TEXT UNIQUE NOT NULL,
       created_at TIMESTAMPTZ DEFAULT NOW()
   );

You can use Litestar CLI to manage migrations:

.. code-block:: bash

   # Generate migration
   litestar db migrations generate -m "create users table"

   # Apply migrations
   litestar db migrations upgrade

Commit Modes
============

The extension supports three transaction commit modes:

Manual Mode
-----------

Explicit transaction control (default):

.. code-block:: python

   db = spec.add_config(
       AsyncpgConfig(
           pool_config={"dsn": "postgresql://..."},
           extension_config={"litestar": {"commit_mode": "manual"}}
       )
   )

   @post("/users")
   async def create_user(
       data: dict,
       db_session: AsyncpgDriver
   ) -> dict:
       async with db_session.begin_transaction():
           result = await db_session.execute(
               "INSERT INTO users (name) VALUES ($1) RETURNING id",
               data["name"]
           )
           return result.one()

Autocommit Mode
---------------

Automatic commit on 2XX responses (recommended):

.. code-block:: python

   db = spec.add_config(
       AsyncpgConfig(
           pool_config={"dsn": "postgresql://..."},
           extension_config={"litestar": {"commit_mode": "autocommit"}}
       )
   )

   @post("/users")
   async def create_user(
       data: dict,
       db_session: AsyncpgDriver
   ) -> dict:
       # Automatically commits on success (2XX response)
       # Automatically rolls back on error (4XX/5XX response)
       result = await db_session.execute(
           "INSERT INTO users (name) VALUES ($1) RETURNING id",
           data["name"]
       )
       return result.one()

Autocommit with Redirects
--------------------------

Commits on both 2XX and 3XX responses:

.. code-block:: python

   db = spec.add_config(
       AsyncpgConfig(
           pool_config={"dsn": "postgresql://..."},
           extension_config={
               "litestar": {"commit_mode": "autocommit_include_redirect"}
           }
       )
   )

Next Steps
==========

Now that you understand the basics:

- :doc:`dependency_injection` - Learn about all dependency injection options
- :doc:`transactions` - Explore transaction management patterns
- :doc:`session_stores` - Set up database-backed session storage
- :doc:`api` - Explore the complete API reference

Common Patterns
===============

Health Check Endpoint
---------------------

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConnection

   @get("/health")
   async def health_check(db_connection: AsyncpgConnection) -> dict:
       try:
           await db_connection.fetchval("SELECT 1")
           return {"status": "healthy", "database": "connected"}
       except Exception as e:
           return {"status": "unhealthy", "error": str(e)}

Error Handling
--------------

.. code-block:: python

   from litestar import HTTPException
   from litestar.status_codes import HTTP_404_NOT_FOUND

   @get("/users/{user_id:int}")
   async def get_user(
       user_id: int,
       db_session: AsyncpgDriver
   ) -> dict:
       result = await db_session.execute(
           "SELECT * FROM users WHERE id = $1",
           user_id
       )
       user = result.one_or_none()
       if not user:
           raise HTTPException(
               status_code=HTTP_404_NOT_FOUND,
               detail=f"User {user_id} not found"
           )
       return user

See Also
========

- :doc:`installation` - Installation instructions
- :doc:`dependency_injection` - Dependency injection details
- :doc:`transactions` - Transaction management
- :doc:`/usage/framework_integrations` - Framework integration guide
