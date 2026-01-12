===============
SQL File Loader
===============

SQLSpec includes a powerful SQL file loader that manages SQL statements from files using aiosql-style named queries. This is the recommended approach for organizing SQL in larger applications.

Overview
--------

The SQL file loader provides:

- **Named Queries**: Organize SQL with descriptive names
- **File-Based Organization**: Keep SQL separate from Python code
- **Caching**: 12x+ performance improvement with intelligent caching
- **Multi-Source Support**: Load from local files, URIs, or cloud storage
- **Type Safety**: Integrate loaded queries with schema mapping

Why Use SQL Files?
------------------

**Benefits**

- Separation of concerns (SQL separate from Python)
- SQL syntax highlighting in editors
- Easier SQL review and maintenance
- Database-specific dialects support
- Version control friendly
- Shared queries across application layers

**When to Use**

- Production applications with many queries
- Complex queries that benefit from SQL formatting
- Queries used in multiple places
- Team collaboration on SQL
- Database migrations and scripts

Basic Usage
-----------

Creating SQL Files
^^^^^^^^^^^^^^^^^^

Create a SQL file with named queries using ``-- name:`` comments:

.. code-block:: sql

   -- sql/users.sql

   -- name: get_user_by_id
   SELECT id, username, email, created_at
   FROM users
   WHERE id = :user_id;

   -- name: list_active_users
   SELECT id, username, email, last_login
   FROM users
   WHERE is_active = true
   ORDER BY username
   LIMIT :limit OFFSET :offset;

   -- name: create_user
   INSERT INTO users (username, email, password_hash)
   VALUES (:username, :email, :password_hash)
   RETURNING id, username, email, created_at;

Loading SQL Files
^^^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_sql_files_1.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :caption: `Loading SQL files with SQLFileLoader`
   :dedent: 2


Using Loaded Queries
^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_sql_files_2.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :caption: `Using loaded queries from SQLFileLoader`
   :dedent: 2


Query Naming Conventions
------------------------

Query Name Syntax
^^^^^^^^^^^^^^^^^

.. code-block:: sql

   -- name: query_name
   SELECT ...

**Naming Rules**

- Use descriptive, snake_case names
- Include operation prefix: ``get_``, ``list_``, ``create_``, ``update_``, ``delete_``
- Indicate plurality: ``get_user`` vs ``list_users``
- Use namespaces with dots: ``users.get_by_id``, ``orders.list_recent``

.. code-block:: sql

   -- Good names
   -- name: get_user_by_id
   -- name: list_active_users
   -- name: create_user
   -- name: update_user_email
   -- name: delete_inactive_users

   -- Namespaced names
   -- name: analytics.daily_sales
   -- name: reports.user_activity

Special Suffixes
^^^^^^^^^^^^^^^^

The loader supports special suffixes for query types:

.. code-block:: sql

   -- name: insert_user!
   -- The ! suffix indicates a modifying query
   INSERT INTO users (name) VALUES (:name);

   -- name: get_users*
   -- The * suffix indicates multiple results expected
   SELECT * FROM users;

Dialect-Specific Queries
-------------------------

Specify Database Dialect
^^^^^^^^^^^^^^^^^^^^^^^^^

Use the ``-- dialect:`` comment for database-specific SQL:

.. code-block:: sql

   -- sql/postgres_queries.sql

   -- name: get_user_with_json
   -- dialect: postgres
   SELECT id, name, data->>'email' as email
   FROM users
   WHERE id = :user_id;

   -- name: upsert_user
   -- dialect: postgres
   INSERT INTO users (id, name, email)
   VALUES (:id, :name, :email)
   ON CONFLICT (id) DO UPDATE
   SET name = EXCLUDED.name, email = EXCLUDED.email;

**Supported Dialects**

- ``postgres`` (PostgreSQL)
- ``sqlite`` (SQLite)
- ``mysql`` (MySQL/MariaDB)
- ``oracle`` (Oracle Database)
- ``mssql`` (Microsoft SQL Server)
- ``duckdb`` (DuckDB)
- ``bigquery`` (Google BigQuery)

The loader normalizes dialect names and handles common aliases (e.g., ``postgresql`` ' ``postgres``).

Advanced Features
-----------------

Adding Queries Programmatically
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_sql_files_3.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `Adding queries programmatically to SQLFileLoader`


Query Metadata
^^^^^^^^^^^^^^

Get information about loaded queries:

.. literalinclude:: ../examples/usage/usage_sql_files_4.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `Retrieving query metadata from SQLFileLoader`


Caching Behavior
^^^^^^^^^^^^^^^^

The loader implements intelligent caching with 12x+ performance improvements:

.. literalinclude:: ../examples/usage/usage_sql_files_5.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `Using caching with SQLFileLoader`


**Cache Features**

- File content checksums prevent stale data
- Statement-level caching for instant access
- Automatic invalidation on file changes (in development)
- Configurable cache size limits

Loading Directories with Mixed Files
-------------------------------------

The loader gracefully handles directories containing both named query files and raw DDL/DML scripts:

.. code-block:: text

   migrations/
      schema.sql        # Raw DDL (no -- name:) → skipped
      queries.sql       # Named queries → loaded
      seed-data.sql     # Raw DML (no -- name:) → skipped

.. literalinclude:: ../examples/usage/usage_sql_files_6.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `Loading a directory with mixed SQL files using SQLFileLoader`

**How it works:**

- Files without ``-- name:`` markers are gracefully skipped
- Skipped files are logged at DEBUG level
- Directory loading continues without errors
- Only named query files are tracked and cached

**When files are skipped:**

- Empty files
- Files with only comments
- Raw DDL scripts (CREATE TABLE, etc.)
- Raw DML scripts without named markers
- Files with ``-- dialect:`` but no ``-- name:``

**When files raise errors:**

- Duplicate query names within the same file
- Malformed ``-- name:`` markers
- Files with ``-- name:`` but no SQL content after parsing

Storage Backends
----------------

The loader supports multiple storage backends for loading SQL files.

Local Files
^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_sql_files_7.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `Loading SQL files from local filesystem using SQLFileLoader`


File URIs
^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_sql_files_8.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `Loading SQL files from file URIs using SQLFileLoader`


Cloud Storage (with fsspec)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When ``fsspec`` is installed, load from cloud storage:

.. literalinclude:: ../examples/usage/usage_sql_files_9.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `Loading SQL files from cloud storage using SQLFileLoader`


Integration with SQLSpec
-------------------------

Loader with SQLSpec Instance
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create a SQLSpec instance with an integrated loader:

.. literalinclude:: ../examples/usage/usage_sql_files_10.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `Creating a SQLSpec instance with SQLFileLoader`


Type-Safe Query Execution
^^^^^^^^^^^^^^^^^^^^^^^^^^

Combine loaded queries with schema mapping:

.. literalinclude:: ../examples/usage/usage_sql_files_11.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `Executing type-safe queries from SQLFileLoader with SQLSpec`


Practical Examples
------------------

Example 1: User Management
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: sql

   -- sql/users.sql

   -- name: get_user
   SELECT id, username, email, is_active, created_at
   FROM users
   WHERE id = :user_id;

   -- name: list_users
   SELECT id, username, email, is_active
   FROM users
   WHERE (:status IS NULL OR is_active = :status)
   ORDER BY created_at DESC
   LIMIT :limit OFFSET :offset;

   -- name: create_user
   INSERT INTO users (username, email, password_hash)
   VALUES (:username, :email, :password_hash)
   RETURNING id, username, email, created_at;

   -- name: update_user
   UPDATE users
   SET username = :username,
       email = :email,
       updated_at = CURRENT_TIMESTAMP
   WHERE id = :user_id
   RETURNING id, username, email, updated_at;

   -- name: delete_user
   DELETE FROM users WHERE id = :user_id;

.. literalinclude:: ../examples/usage/usage_sql_files_12.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `Using user management queries from SQLFileLoader`


Example 2: Analytics Queries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: sql

   -- sql/analytics.sql

   -- name: daily_sales
   -- dialect: postgres
   SELECT
       DATE(created_at) as sale_date,
       COUNT(*) as order_count,
       SUM(total_amount) as total_sales,
       AVG(total_amount) as avg_order_value
   FROM orders
   WHERE created_at >= :start_date
       AND created_at < :end_date
   GROUP BY DATE(created_at)
   ORDER BY sale_date;

   -- name: top_products
   SELECT
       p.name,
       p.category,
       COUNT(oi.id) as times_ordered,
       SUM(oi.quantity) as total_quantity,
       SUM(oi.quantity * oi.unit_price) as revenue
   FROM products p
   JOIN order_items oi ON p.id = oi.product_id
   JOIN orders o ON oi.order_id = o.id
   WHERE o.created_at >= :start_date
   GROUP BY p.id, p.name, p.category
   ORDER BY revenue DESC
   LIMIT :limit;

   -- name: customer_lifetime_value
   WITH customer_orders AS (
       SELECT
           user_id,
           COUNT(*) as order_count,
           SUM(total_amount) as total_spent,
           MIN(created_at) as first_order,
           MAX(created_at) as last_order
       FROM orders
       GROUP BY user_id
   )
   SELECT
       u.id,
       u.username,
       u.email,
       co.order_count,
       co.total_spent,
       co.first_order,
       co.last_order,
       EXTRACT(EPOCH FROM (co.last_order - co.first_order)) / 86400 as customer_days
   FROM users u
   JOIN customer_orders co ON u.id = co.user_id
   WHERE co.total_spent > :min_spent
   ORDER BY co.total_spent DESC;

.. literalinclude:: ../examples/usage/usage_sql_files_13.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `Using analytics queries from SQLFileLoader`


Example 3: Multi-Database Setup
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_sql_files_14.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `Using SQLFileLoader with multiple database dialects`


Best Practices
--------------

**1. Organize by Domain**

.. code-block:: text

   sql/
      users.sql          # User management
      products.sql       # Product catalog
      orders.sql         # Order processing
      analytics.sql      # Reports and analytics
      admin.sql          # Admin operations

**2. Use Clear Query Names**

.. code-block:: sql

   -- Good: Clear intent
   -- name: get_active_users_by_role
   -- name: create_order_with_items
   -- name: delete_expired_sessions

   -- Bad: Unclear
   -- name: query1
   -- name: get_data
   -- name: do_stuff

**3. Document Complex Queries**

.. code-block:: sql

   -- name: calculate_inventory_levels
   -- Calculate current inventory levels accounting for pending orders
   -- and returns items below reorder threshold
   SELECT
       p.id,
       p.name,
       p.current_stock,
       COALESCE(SUM(oi.quantity), 0) as pending_orders,
       p.current_stock - COALESCE(SUM(oi.quantity), 0) as available,
       p.reorder_point
   FROM products p
   LEFT JOIN order_items oi ON p.id = oi.product_id
   LEFT JOIN orders o ON oi.order_id = o.id
   WHERE o.status = 'pending' OR o.status IS NULL
   GROUP BY p.id, p.name, p.current_stock, p.reorder_point
   HAVING p.current_stock - COALESCE(SUM(oi.quantity), 0) < p.reorder_point;

**4. Use Parameters, Not String Formatting**

.. code-block:: sql

   -- Good: Parameterized
   -- name: get_user_by_email
   SELECT * FROM users WHERE email = :email;

   -- Bad: Vulnerable to injection (don't do this)
   -- name: get_user_by_email
   SELECT * FROM users WHERE email = '{email}';

**5. Version Control SQL Files**

- Commit SQL files to version control
- Review SQL changes in pull requests
- Track query performance over time

Troubleshooting
---------------

Query Not Found
^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_sql_files_15.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `Handling query not found errors with SQLFileLoader`


File Load Errors
^^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_sql_files_16.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `Handling file load errors with SQLFileLoader`


Debugging Loaded Queries
^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_sql_files_17.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `Debugging loaded queries with SQLFileLoader`


Modifying Loaded Queries
------------------------

After loading a query, you can modify it using fluent methods. Each method returns a new SQL object, preserving immutability.

Adding WHERE Conditions
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec import SQLSpec

   sqlspec = SQLSpec()
   sqlspec.load_sql_files("sql/")

   # Get base query
   query = sqlspec.get_sql("list_users")

   # Add conditions with auto-generated parameters
   filtered = (
       query
       .where_eq("status", "active")
       .where_gte("created_at", "2024-01-01")
       .where_in("role", ["admin", "moderator"])
   )

   # Execute with driver
   results = driver.select(filtered)

**Available WHERE Methods:**

- ``where_eq(column, value)`` - Equal to
- ``where_neq(column, value)`` - Not equal to
- ``where_lt(column, value)`` - Less than
- ``where_lte(column, value)`` - Less than or equal
- ``where_gt(column, value)`` - Greater than
- ``where_gte(column, value)`` - Greater than or equal
- ``where_like(column, pattern)`` - LIKE pattern match
- ``where_ilike(column, pattern)`` - Case-insensitive LIKE
- ``where_is_null(column)`` - IS NULL check
- ``where_is_not_null(column)`` - IS NOT NULL check
- ``where_in(column, values)`` - IN list
- ``where_not_in(column, values)`` - NOT IN list
- ``where_between(column, low, high)`` - BETWEEN range

Pagination
^^^^^^^^^^

.. code-block:: python

   # Add pagination
   paginated = query.limit(20).offset(40)

   # Or use the convenience method
   paginated = query.paginate(page=3, page_size=20)

Column Projection
^^^^^^^^^^^^^^^^^

Restrict the columns returned by a query:

.. code-block:: python

   # Select only specific columns
   projected = query.select_only("id", "name", "email")

Chaining Methods
^^^^^^^^^^^^^^^^

All methods can be chained fluently:

.. code-block:: python

   result = (
       sqlspec.get_sql("list_orders")
       .where_eq("customer_id", customer_id)
       .where_gte("total", 100)
       .where_in("status", ["pending", "processing"])
       .select_only("id", "total", "created_at")
       .limit(50)
       .offset(100)
   )

CTE Preservation
^^^^^^^^^^^^^^^^

Queries with Common Table Expressions (CTEs) are handled correctly:

.. code-block:: python

   # Query with CTE remains valid after modification
   complex_query = sqlspec.get_sql("analytics_with_cte")
   filtered = complex_query.where_eq("region", "us-east")
   # CTE is preserved at the top level


Next Steps
----------

- :doc:`drivers_and_querying` - Execute loaded queries
- :doc:`framework_integrations` - Use loader in web frameworks
- :doc:`../reference/base` - SQLFileLoader API reference

See Also
--------

- :doc:`query_builder` - Alternative: programmatic query construction
- :doc:`configuration` - Configure statement processing for loaded queries
- :doc:`../examples/index` - Complete examples with SQL files
