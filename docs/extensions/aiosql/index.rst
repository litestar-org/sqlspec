==================
aiosql Integration
==================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   quickstart
   usage
   migration
   api

SQL file organization for SQLSpec using aiosql-style named queries with native loader and compatibility adapter.

Overview
========

SQLSpec provides compatibility with aiosql-style SQL files through two approaches: a **native SQLFileLoader** for SQLSpec-first projects, and an **aiosql adapter** for existing aiosql users who want to adopt SQLSpec gradually.

aiosql is a popular Python library that enables organizing SQL queries in ``.sql`` files with named queries. SQLSpec provides seamless integration with this format, allowing you to use aiosql-style SQL files whether you're coming from aiosql or starting fresh with SQLSpec.

Key Features
============

Native SQLFileLoader
--------------------

- **Cloud Storage**: Load from S3, Google Cloud Storage, Azure, HTTP
- **Type Safety**: Direct integration with Pydantic, msgspec, attrs
- **Dialect Support**: Per-query dialect specifications
- **Namespace Support**: Organize queries with dotted names
- **Statement Cache**: SQL files cached after first load

aiosql Adapter
--------------

- **Full Compatibility**: Works with existing aiosql SQL files
- **Query Operators**: Full support for aiosql operators (``^``, ``$``, ``!``, ``*!``, ``#``)
- **Method Generation**: Automatic query method generation
- **Extended Database Support**: Use aiosql-style queries with DuckDB, Oracle, BigQuery, and other databases

When to Use This Integration
=============================

- **Already using aiosql?** Your SQL files work as-is with SQLSpec's aiosql adapter
- **Need aiosql-style queries with databases aiosql doesn't support?** Use the adapter with DuckDB, Oracle, BigQuery, etc.
- **Want to organize SQL in separate files?** Both approaches support clean SQL file organization
- **Team collaboration on SQL queries?** Version control SQL files alongside your code
- **Database-specific query variations?** Write optimized queries for different databases

Quick Comparison
================

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Feature
     - SQLFileLoader (Built-in)
     - aiosql Adapter
   * - **Cloud Storage**
     - ✅ S3, GCS, Azure, HTTP
     - ❌ Local files only
   * - **Type Mapping**
     - ✅ Pydantic, msgspec, attrs
     - ⚠️ Limited support
   * - **Transaction Support**
     - ✅ Full SQLSpec transactions
     - ✅ Via aiosql connection
   * - **Dialect Support**
     - ✅ Per-query dialect specs
     - ⚠️ Global only
   * - **Caching**
     - ✅ SQL files cached after first load
     - ❌ No caching
   * - **Query Operators**
     - ❌ Not supported
     - ✅ Full aiosql operators (``^``, ``$``, ``!``, etc.)
   * - **Setup Complexity**
     - Low (built into SQLSpec)
     - Minimal (requires aiosql package)
   * - **Best For**
     - SQLSpec-native projects
     - Existing aiosql users, unsupported databases

Both approaches are fully supported and work with the same SQL file format (with or without operators).

Quick Example
=============

SQLFileLoader (Built-in)
-------------------------

.. code-block:: sql

   -- queries/users.sql

   -- name: get_user_by_id
   SELECT id, username, email FROM users WHERE id = :user_id;

   -- name: create_user
   INSERT INTO users (username, email)
   VALUES (:username, :email)
   RETURNING id, username, email;

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig

   # Set up database
   spec = SQLSpec()
   config = spec.add_config(
       AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/mydb"})
   )

   # Load SQL files
   spec.load_sql_files("queries/users.sql")

   # Execute queries
   async with spec.provide_session(config) as session:
       # Get user
       result = await session.execute(spec.get_sql("get_user_by_id"), user_id=123)
       user = result.one()

       # Create user
       result = await session.execute(
           spec.get_sql("create_user"),
           username="alice",
           email="alice@example.com"
       )
       new_user = result.one()

aiosql Adapter (Compatibility)
-------------------------------

.. code-block:: sql

   -- queries/users.sql

   -- name: get_user_by_id^
   SELECT id, username, email FROM users WHERE id = :user_id;

   -- name: create_user!
   INSERT INTO users (username, email)
   VALUES (:username, :email);

.. code-block:: python

   import aiosql
   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.extensions.aiosql import AiosqlAsyncAdapter

   # Set up database
   spec = SQLSpec()
   config = spec.add_config(
       AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/mydb"})
   )

   # Create adapter and load queries
   async with spec.provide_driver(config) as driver:
       adapter = AiosqlAsyncAdapter(driver)
       queries = aiosql.from_path("queries/users.sql", adapter)

       async with spec.provide_connection(config) as conn:
           # Execute queries
           user = await queries.get_user_by_id(conn, user_id=123)
           await queries.create_user(conn, username="alice", email="alice@example.com")

Architecture Overview
=====================

SQLFileLoader Architecture
--------------------------

.. code-block:: text

   ┌─────────────────────┐
   │   SQL Files         │  (Local, S3, GCS, HTTP)
   └──────────┬──────────┘
              │
   ┌──────────▼──────────┐
   │  SQLFileLoader      │  ← Parses and caches SQL
   └──────────┬──────────┘
              │
   ┌──────────▼──────────┐
   │  SQL Statement      │  ← Named queries
   └──────────┬──────────┘
              │
   ┌──────────▼──────────┐
   │  SQLSpec Session    │  ← Executes queries
   └──────────┬──────────┘
              │
   ┌──────────▼──────────┐
   │    Database         │
   └─────────────────────┘

aiosql Adapter Architecture
----------------------------

.. code-block:: text

   ┌─────────────────────┐
   │   SQL Files         │  (Local only)
   └──────────┬──────────┘
              │
   ┌──────────▼──────────┐
   │  aiosql Library     │  ← Parses SQL files
   └──────────┬──────────┘
              │
   ┌──────────▼──────────┐
   │  AiosqlAdapter      │  ← Bridges aiosql to SQLSpec
   └──────────┬──────────┘
              │
   ┌──────────▼──────────┐
   │  SQLSpec Driver     │  ← Executes queries
   └──────────┬──────────┘
              │
   ┌──────────▼──────────┐
   │    Database         │
   └─────────────────────┘

Use Cases
=========

Organized SQL Management
------------------------

Keep SQL separate from Python code for better maintainability:

.. code-block:: text

   queries/
     users.sql          # User management
     products.sql       # Product catalog
     orders.sql         # Order processing
     analytics.sql      # Reports and analytics

Using Existing aiosql SQL Files
--------------------------------

If you have existing aiosql SQL files, they work directly with SQLSpec:

.. code-block:: python

   # Use aiosql adapter for existing SQL files
   adapter = AiosqlSyncAdapter(driver)
   queries = aiosql.from_path("queries/legacy/", adapter)

   # Or use SQLFileLoader for new features
   spec.load_sql_files("queries/new_features/")

   # Both work with the same SQLSpec instance

Database-Specific Queries
--------------------------

Write optimized queries for different databases:

.. code-block:: sql

   -- name: upsert_user
   -- dialect: postgres
   INSERT INTO users (id, name) VALUES (:id, :name)
   ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;

   -- name: upsert_user
   -- dialect: sqlite
   INSERT INTO users (id, name) VALUES (:id, :name)
   ON CONFLICT(id) DO UPDATE SET name = excluded.name;

Cloud Storage Loading
---------------------

Load SQL from cloud storage (SQLFileLoader only):

.. code-block:: python

   # Amazon S3
   spec.load_sql_files("s3://my-bucket/queries/")

   # Google Cloud Storage
   spec.load_sql_files("gs://my-bucket/queries/")

   # HTTP
   spec.load_sql_files("https://example.com/queries/users.sql")

Next Steps
==========

.. grid:: 2
   :gutter: 3

   .. grid-item-card:: 📦 Installation
      :link: installation
      :link-type: doc

      Install the extension and dependencies

   .. grid-item-card:: 🚀 Quick Start
      :link: quickstart
      :link-type: doc

      Get up and running in 5 minutes

   .. grid-item-card:: 📖 Usage Guide
      :link: usage
      :link-type: doc

      Learn about SQLFileLoader features

   .. grid-item-card:: 🔄 Compatibility Guide
      :link: migration
      :link-type: doc

      Using aiosql files with SQLSpec

   .. grid-item-card:: 📚 API Reference
      :link: api
      :link-type: doc

      Complete API documentation

See Also
========

- :doc:`/usage/sql_files` - Complete SQL file loader guide
- :doc:`/reference/base` - SQLFileLoader API reference
- :doc:`/usage/drivers_and_querying` - Query execution
- `aiosql Documentation <https://nackjicholson.github.io/aiosql/>`_
