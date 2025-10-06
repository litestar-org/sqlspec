=====
Usage
=====

This guide covers advanced features of the SQLFileLoader and aiosql adapter.

SQLFileLoader (Built-in)
=========================

For detailed SQLFileLoader documentation, see :doc:`/usage/sql_files`.

Key features covered here:

- SQL file format
- Cloud storage loading
- Type-safe results
- Query introspection
- Cache management

SQL File Format
---------------

Basic query definition:

.. code-block:: sql

   -- name: query_name
   SELECT * FROM users WHERE id = :user_id;

With documentation:

.. code-block:: sql

   -- name: get_active_users
   -- Retrieve all active users from the database.
   -- This query excludes soft-deleted users.
   SELECT id, username, email
   FROM users
   WHERE is_active = true AND deleted_at IS NULL
   ORDER BY username;

Dialect-specific queries:

.. code-block:: sql

   -- name: upsert_user
   -- dialect: postgres
   INSERT INTO users (id, name) VALUES (:id, :name)
   ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;

Cloud Storage Support
---------------------

Load SQL files from cloud storage:

.. code-block:: python

   from sqlspec import SQLSpec

   spec = SQLSpec()

   # Amazon S3
   spec.load_sql_files("s3://my-bucket/queries/users.sql")

   # Google Cloud Storage
   spec.load_sql_files("gs://my-bucket/queries/users.sql")

   # Azure Blob Storage
   spec.load_sql_files("az://my-container/queries/users.sql")

   # HTTP/HTTPS
   spec.load_sql_files("https://example.com/queries/users.sql")

Type-Safe Results
-----------------

Combine with schema mapping:

.. code-block:: python

   from pydantic import BaseModel

   class User(BaseModel):
       id: int
       username: str
       email: str

   async with spec.provide_session(config) as session:
       result = await session.execute(
           spec.get_sql("get_user_by_id"),
           user_id=1,
           schema_type=User
       )
       user: User = result.one()

Query Introspection
-------------------

.. code-block:: python

   # List all queries
   queries = spec.list_sql_queries()

   # Check if query exists
   if spec.has_sql_query("get_user"):
       print("Query exists!")

   # Get SQL files
   files = spec.get_sql_files()
   print(f"Loaded files: {files}")

Cache Management
----------------

.. code-block:: python

   # Clear all caches
   spec.clear_sql_cache()

   # Reload SQL files
   spec.reload_sql_files()

aiosql Adapter
==============

Query Operators
---------------

The aiosql adapter supports all aiosql query operators:

.. list-table::
   :header-rows: 1
   :widths: 15 30 55

   * - Operator
     - Meaning
     - Example
   * - (none)
     - Select many (list of rows)
     - ``-- name: get_all_users``
   * - ``^``
     - Select one (single row or None)
     - ``-- name: get_user^``
   * - ``$``
     - Select value (single value)
     - ``-- name: get_count$``
   * - ``!``
     - Insert/Update/Delete
     - ``-- name: create_user!``
   * - ``*!``
     - Insert/Update/Delete many
     - ``-- name: bulk_insert*!``
   * - ``#``
     - Script (no return)
     - ``-- name: create_schema#``

Example SQL file:

.. code-block:: sql

   -- name: get_all_users
   SELECT id, username FROM users;

   -- name: get_user_by_id^
   SELECT id, username FROM users WHERE id = :user_id;

   -- name: count_users$
   SELECT COUNT(*) FROM users;

   -- name: create_user!
   INSERT INTO users (username) VALUES (:username);

   -- name: bulk_create*!
   INSERT INTO users (username) VALUES (:username);

   -- name: create_tables#
   CREATE TABLE users (id SERIAL, username TEXT);

Usage:

.. code-block:: python

   # Select many
   users = queries.get_all_users(conn)

   # Select one
   user = queries.get_user_by_id(conn, user_id=1)

   # Select value
   count = queries.count_users(conn)

   # Insert/Update/Delete
   queries.create_user(conn, username="alice")

   # Bulk operations
   queries.bulk_create(conn, [{"username": "user1"}, {"username": "user2"}])

   # Script
   queries.create_tables(conn)

Transaction Support
-------------------

.. code-block:: python

   # Async transactions
   async with spec.provide_session(config) as session:
       async with session.begin_transaction():
           await queries.create_user(session.connection, username="alice")
           await queries.create_user(session.connection, username="bob")

Type Mapping
------------

Limited type mapping via special parameter:

.. code-block:: python

   from pydantic import BaseModel

   class User(BaseModel):
       id: int
       username: str

   result = queries.get_all_users(conn, _sqlspec_schema_type=User)

Best Practices
==============

Organize by Domain
------------------

.. code-block:: text

   queries/
     users.sql          # User management
     products.sql       # Product catalog
     orders.sql         # Order processing
     analytics.sql      # Reports

Use Clear Query Names
---------------------

.. code-block:: sql

   -- Good
   -- name: get_active_users_by_role
   -- name: create_order_with_items

   -- Bad
   -- name: query1
   -- name: get_data

Document Complex Queries
-------------------------

.. code-block:: sql

   -- name: calculate_inventory
   -- Calculate current inventory accounting for pending orders.
   -- Returns items below reorder threshold.
   SELECT
       p.id,
       p.current_stock - COALESCE(SUM(oi.quantity), 0) as available
   FROM products p
   LEFT JOIN order_items oi ON p.id = oi.product_id
   WHERE available < p.reorder_point;

Use Parameters
--------------

.. code-block:: sql

   -- Good: Safe from SQL injection
   -- name: get_user_by_email
   SELECT * FROM users WHERE email = :email;

   -- Bad: Never do this
   SELECT * FROM users WHERE email = '{email}';

See Also
========

- :doc:`/usage/sql_files` - Complete SQL file loader guide
- :doc:`quickstart` - Get started quickly
- :doc:`migration` - Using aiosql files with SQLSpec
- :doc:`api` - API reference
