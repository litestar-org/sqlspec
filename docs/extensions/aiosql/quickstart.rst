===========
Quick Start
===========

This guide will get you up and running with aiosql-style SQL files in 5 minutes.

Overview
========

In this quickstart, you'll:

1. Create SQL files with named queries
2. Load SQL files with SQLFileLoader or aiosql adapter
3. Execute queries with SQLSpec
4. Use type-safe result mapping

Prerequisites
=============

Ensure you have installed:

- SQLSpec with a database adapter (see :doc:`installation`)

.. code-block:: bash

   pip install sqlspec[asyncpg]

SQLFileLoader Quickstart (Built-in)
====================================

Step 1: Create SQL File
------------------------

Create a SQL file with named queries:

.. code-block:: sql

   -- queries/users.sql

   -- name: get_user_by_id
   SELECT id, username, email, created_at
   FROM users
   WHERE id = :user_id;

   -- name: list_active_users
   SELECT id, username, email
   FROM users
   WHERE is_active = true
   ORDER BY username
   LIMIT :limit OFFSET :offset;

   -- name: create_user
   INSERT INTO users (username, email, password_hash)
   VALUES (:username, :email, :password_hash)
   RETURNING id, username, email, created_at;

Step 2: Load SQL Files
-----------------------

.. code-block:: python

   from sqlspec import SQLSpec

   # Create SQLSpec instance
   spec = SQLSpec()

   # Load SQL files
   spec.load_sql_files("queries/users.sql")

   # Or load entire directory
   spec.load_sql_files("queries/")

   # List loaded queries
   print(spec.list_sql_queries())

Step 3: Execute Queries
------------------------

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig

   # Set up database (continue from Step 2)
   config = spec.add_config(
       AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/mydb"})
   )

   # Execute queries
   async with spec.provide_session(config) as session:
       # Get user by ID
       result = await session.execute(spec.get_sql("get_user_by_id"), user_id=123)
       user = result.one()
       print(user)

       # Create user
       result = await session.execute(
           spec.get_sql("create_user"),
           username="alice",
           email="alice@example.com",
           password_hash="hashed"
       )
       new_user = result.one()
       print(new_user)

Step 4: Type-Safe Results
--------------------------

Add Pydantic models for type safety:

.. code-block:: python

   from pydantic import BaseModel
   from datetime import datetime

   class User(BaseModel):
       id: int
       username: str
       email: str
       created_at: datetime

   # Execute with type mapping
   async with spec.provide_session(config) as session:
       result = await session.execute(
           spec.get_sql("get_user_by_id"),
           user_id=1,
           schema_type=User
       )
       user: User = result.one()  # Fully typed!
       print(user.username)  # IDE autocomplete works!

Complete SQLFileLoader Example
-------------------------------

.. code-block:: python

   import asyncio
   from pydantic import BaseModel
   from datetime import datetime

   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig

   class User(BaseModel):
       id: int
       username: str
       email: str
       created_at: datetime

   async def main():
       # Set up database
       spec = SQLSpec()
       config = spec.add_config(
           AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/mydb"})
       )

       # Load SQL files
       spec.load_sql_files("queries/users.sql")

       async with spec.provide_session(config) as session:
           # Create user
           result = await session.execute(
               spec.get_sql("create_user"),
               username="alice",
               email="alice@example.com",
               password_hash="hashed",
               schema_type=User
           )
           user: User = result.one()
           print(f"Created: {user.username}")

           # Get user
           result = await session.execute(
               spec.get_sql("get_user_by_id"),
               user_id=user.id,
               schema_type=User
           )
           retrieved: User = result.one()
           print(f"Retrieved: {retrieved.username}")

   asyncio.run(main())

aiosql Adapter Quickstart
==========================

If you have existing aiosql SQL files or need aiosql operators, use the aiosql adapter:

Step 1: Create SQL File
------------------------

Create a SQL file with aiosql operators:

.. code-block:: sql

   -- queries/users.sql

   -- name: get_all_users
   SELECT id, username, email FROM users;

   -- name: get_user_by_id^
   SELECT id, username, email FROM users WHERE id = :user_id;

   -- name: get_user_count$
   SELECT COUNT(*) FROM users;

   -- name: create_user<!
   INSERT INTO users (username, email)
   VALUES (:username, :email);

Step 2: Use with aiosql Adapter
--------------------------------

**Async Example:**

.. code-block:: python

   import asyncio
   import aiosql
   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.extensions.aiosql import AiosqlAsyncAdapter

   async def main():
       # Set up SQLSpec
       spec = SQLSpec()
       config = spec.add_config(
           AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/mydb"})
       )

       async with spec.provide_driver(config) as driver:
           # Create aiosql adapter
           adapter = AiosqlAsyncAdapter(driver)

           # Load queries with aiosql
           queries = aiosql.from_path("queries/users.sql", adapter)

           async with spec.provide_connection(config) as conn:
               # Execute queries
               users = await queries.get_all_users(conn)
               user = await queries.get_user_by_id(conn, user_id=1)
               count = await queries.get_user_count(conn)
               await queries.create_user(
                   conn,
                   username="alice",
                   email="alice@example.com"
               )

   asyncio.run(main())

**Sync Example:**

.. code-block:: python

   import aiosql
   from sqlspec import SQLSpec
   from sqlspec.adapters.sqlite import SqliteConfig
   from sqlspec.extensions.aiosql import AiosqlSyncAdapter

   # Set up SQLSpec
   spec = SQLSpec()
   config = spec.add_config(SqliteConfig(database="app.db"))

   with spec.provide_driver(config) as driver:
       # Create aiosql adapter
       adapter = AiosqlSyncAdapter(driver)

       # Load queries
       queries = aiosql.from_path("queries/users.sql", adapter)

       with spec.provide_connection(config) as conn:
           # Execute queries
           users = queries.get_all_users(conn)
           user = queries.get_user_by_id(conn, user_id=1)
           count = queries.get_user_count(conn)
           queries.create_user(conn, username="alice", email="alice@example.com")

Key Differences
===============

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Feature
     - SQLFileLoader
     - aiosql Adapter
   * - Query Access
     - ``spec.get_sql("name")``
     - ``queries.name(conn, **params)``
   * - Parameters
     - ``execute(query, **params)``
     - ``queries.name(conn, **params)``
   * - Type Mapping
     - ``execute(..., schema_type=User)``
     - ``_sqlspec_schema_type=User``
   * - Cloud Storage
     - ✅ Supported
     - ❌ Local only
   * - Caching
     - ✅ SQL files cached after first load
     - ❌ No caching
   * - Query Operators
     - ❌ Not supported
     - ✅ Full aiosql operators

Advanced Features
=================

Cloud Storage (SQLFileLoader)
------------------------------

.. code-block:: python

   # Amazon S3
   spec.load_sql_files("s3://my-bucket/queries/")

   # Google Cloud Storage
   spec.load_sql_files("gs://my-bucket/queries/")

   # HTTP
   spec.load_sql_files("https://example.com/queries/users.sql")

Dialect-Specific Queries (SQLFileLoader)
-----------------------------------------

.. code-block:: sql

   -- name: upsert_user
   -- dialect: postgres
   INSERT INTO users (id, name) VALUES (:id, :name)
   ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;

   -- name: upsert_user
   -- dialect: sqlite
   INSERT INTO users (id, name) VALUES (:id, :name)
   ON CONFLICT(id) DO UPDATE SET name = excluded.name;

Namespaced Queries (SQLFileLoader)
-----------------------------------

.. code-block:: sql

   -- name: users.get_by_id
   SELECT * FROM users WHERE id = :id;

   -- name: users.list_all
   SELECT * FROM users;

.. code-block:: python

   # Access namespaced queries
   result = session.execute(spec.get_sql("users.get_by_id"), id=1)

Next Steps
==========

- :doc:`usage` - Learn about SQLFileLoader features in depth
- :doc:`migration` - Using aiosql files with SQLSpec
- :doc:`api` - Explore the complete API reference
- :doc:`/usage/sql_files` - Complete SQL file loader guide

See Also
========

- :doc:`installation` - Installation instructions
- :doc:`/usage/drivers_and_querying` - Query execution details
- :doc:`/reference/base` - SQLFileLoader API reference
