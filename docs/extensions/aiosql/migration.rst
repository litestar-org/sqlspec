=====================
Compatibility Guide
=====================

Using aiosql-style SQL files with SQLSpec.

Overview
========

This guide covers:

1. No changes needed for SQL files
2. Choosing the right approach for your project
3. Code examples for both approaches
4. Using both approaches together
5. Testing your integration

Step 1: No SQL File Changes
============================

Your existing aiosql SQL files work as-is with SQLSpec:

.. code-block:: sql

   -- queries/users.sql
   -- This file works with both aiosql and SQLSpec!

   -- name: get_user_by_id
   SELECT id, username, email FROM users WHERE id = :user_id;

   -- name: create_user
   INSERT INTO users (username, email) VALUES (:username, :email);

Step 2: Choose Your Approach
=============================

Option A: SQLFileLoader (SQLSpec-Native)
-----------------------------------------

Use this approach for SQLSpec-native projects or when you want SQLSpec-specific features.

**Before (vanilla aiosql):**

.. code-block:: python

   import aiosql
   import sqlite3

   queries = aiosql.from_path("queries/users.sql", "sqlite3")
   conn = sqlite3.connect("app.db")
   user = queries.get_user_by_id(conn, user_id=1)

**With SQLSpec SQLFileLoader:**

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.sqlite import SqliteConfig

   spec = SQLSpec()
   config = spec.add_config(SqliteConfig(database="app.db"))

   spec.load_sql_files("queries/users.sql")

   with spec.provide_session(config) as session:
       result = session.execute(spec.get_sql("get_user_by_id"), user_id=1)
       user = result.one()

**When to use:**

- You want cloud storage support (S3, GCS, Azure)
- You need advanced type mapping
- You're building a SQLSpec-first application

Option B: aiosql Adapter (Compatibility)
-----------------------------------------

Use this approach if you have existing aiosql code or need aiosql query operators.

**Before (vanilla aiosql):**

.. code-block:: python

   import aiosql
   import sqlite3

   queries = aiosql.from_path("queries/users.sql", "sqlite3")
   conn = sqlite3.connect("app.db")
   user = queries.get_user_by_id(conn, user_id=1)

**With SQLSpec aiosql adapter:**

.. code-block:: python

   import aiosql
   from sqlspec import SQLSpec
   from sqlspec.adapters.sqlite import SqliteConfig
   from sqlspec.extensions.aiosql import AiosqlSyncAdapter

   spec = SQLSpec()
   config = spec.add_config(SqliteConfig(database="app.db"))

   with spec.provide_driver(config) as driver:
       adapter = AiosqlSyncAdapter(driver)
       queries = aiosql.from_path("queries/users.sql", adapter)

       with spec.provide_connection(config) as conn:
           user = queries.get_user_by_id(conn, user_id=1)

**When to use:**

- You have existing aiosql code you want to keep working
- You need aiosql query operators (``^``, ``$``, ``!``, etc.)
- You want to use aiosql-style queries with databases aiosql doesn't support (DuckDB, Oracle, BigQuery)

Step 3: Code Pattern Comparison
================================

SQLFileLoader Pattern
---------------------

.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - aiosql
     - SQLSpec SQLFileLoader
   * - ``queries = aiosql.from_path("file.sql", "driver")``
     - ``spec.load_sql_files("file.sql")``
   * - ``result = queries.get_user(conn, id=1)``
     - ``query = spec.get_sql("get_user")``
       ``result = session.execute(query, id=1)``
   * - ``user = queries.get_user_by_id(conn, user_id=1)``
     - ``query = spec.get_sql("get_user_by_id")``
       ``user = session.execute(query, user_id=1).one()``

aiosql Adapter Pattern
----------------------

.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - aiosql
     - SQLSpec aiosql adapter
   * - ``queries = aiosql.from_path("file.sql", "sqlite3")``
     - ``adapter = AiosqlSyncAdapter(driver)``
       ``queries = aiosql.from_path("file.sql", adapter)``
   * - ``conn = sqlite3.connect("app.db")``
     - ``with spec.provide_connection(config) as conn:``
   * - ``user = queries.get_user(conn, id=1)``
     - ``user = queries.get_user(conn, id=1)``
       (same!)

Common Patterns
===============

Async Usage
-----------

**aiosql:**

.. code-block:: python

   import aiosql
   import asyncpg

   queries = aiosql.from_path("queries.sql", "asyncpg")
   conn = await asyncpg.connect("postgresql://...")
   user = await queries.get_user(conn, user_id=1)

**SQLSpec SQLFileLoader:**

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig

   spec = SQLSpec()
   config = spec.add_config(
       AsyncpgConfig(connection_config={"dsn": "postgresql://..."})
   )

   spec.load_sql_files("queries.sql")

   async with spec.provide_session(config) as session:
       result = await session.execute(spec.get_sql("get_user"), user_id=1)
       user = result.one()

Transaction Handling
--------------------

**aiosql:**

.. code-block:: python

   async with conn.transaction():
       await queries.create_user(conn, username="alice")
       await queries.create_profile(conn, user_id=user.id)

**SQLSpec:**

.. code-block:: python

   async with session.begin_transaction():
       user = await session.execute(
           spec.get_sql("create_user"),
           username="alice"
       ).one()

       await session.execute(
           spec.get_sql("create_profile"),
           user_id=user["id"]
       )

Using Both Approaches Together
===============================

You can use both SQLFileLoader and the aiosql adapter in the same project:

.. code-block:: python

   import aiosql
   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.extensions.aiosql import AiosqlAsyncAdapter

   spec = SQLSpec()
   config = spec.add_config(
       AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/mydb"})
   )

   # Load some SQL files with SQLFileLoader
   spec.load_sql_files("queries/reports/")

   # Use aiosql adapter for other SQL files
   async with spec.provide_driver(config) as driver:
       adapter = AiosqlAsyncAdapter(driver)
       legacy_queries = aiosql.from_path("queries/legacy/", adapter)

       async with spec.provide_session(config) as session:
           # Use SQLFileLoader queries
           report = await session.execute(
               spec.get_sql("generate_report"),
               start_date="2025-01-01"
           )

           # Use aiosql adapter queries
           async with spec.provide_connection(config) as conn:
               users = await legacy_queries.get_users(conn)

Troubleshooting
===============

Query Not Found
---------------

**Error:** ``KeyError: 'query_name'``

**Solution:** Ensure query name matches exactly:

.. code-block:: python

   # Check loaded queries
   print(spec.list_sql_queries())

   # Verify query name in SQL file
   # -- name: get_user_by_id  (not get_user)

Parameter Mismatch
------------------

**Error:** Parameter style mismatch

**Solution:** Check your database's parameter style:

.. code-block:: python

   # SQLite, Oracle use :name
   result = session.execute(spec.get_sql("get_user"), user_id=1)

   # PostgreSQL uses $1, $2
   # Update SQL file to match database

Type Mapping Issues
-------------------

**Error:** Type validation failures

**Solution:** Ensure column names match model fields:

.. code-block:: python

   class User(BaseModel):
       id: int
       username: str  # Must match column name in SELECT

   # SQL must have matching columns
   # SELECT id, username FROM users

Next Steps
==========

Learn more about using aiosql-style SQL files with SQLSpec:

- :doc:`usage` - Learn advanced features
- :doc:`api` - Explore complete API
- :doc:`/usage/sql_files` - Complete SQL file guide

See Also
========

- :doc:`quickstart` - Get started guide
- :doc:`installation` - Installation instructions
- :doc:`/usage/drivers_and_querying` - Query execution
