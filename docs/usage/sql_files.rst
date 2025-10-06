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

.. code-block:: python

   from sqlspec.loader import SQLFileLoader

   # Create loader
   loader = SQLFileLoader()

   # Load SQL files
   loader.load_sql("sql/users.sql", "sql/products.sql", "sql/orders.sql")

   # Or load from a directory
   loader.load_sql("sql/")

   # List available queries
   queries = loader.list_queries()
   print(queries)  # ['get_user_by_id', 'list_active_users', 'create_user', ...]

Using Loaded Queries
^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.sqlite import SqliteConfig

   # Set up database
   spec = SQLSpec()
   config = SqliteConfig()
   spec.add_config(config)

   # Get SQL with parameters
   user_query = loader.get_sql("get_user_by_id", user_id=123)

   # Execute with session
   with spec.provide_session(config) as session:
       result = session.execute(user_query)
       user = result.one()

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

.. code-block:: python

   # Add a query at runtime
   loader.add_named_sql(
       "health_check",
       "SELECT 'OK' as status, CURRENT_TIMESTAMP as timestamp"
   )

   # Add with dialect
   loader.add_named_sql(
       "postgres_version",
       "SELECT version()",
       dialect="postgres"
   )

   # Use the added query
   health_sql = loader.get_sql("health_check")

Query Metadata
^^^^^^^^^^^^^^

Get information about loaded queries:

.. code-block:: python

   # Get file info for a query
   file_info = loader.get_file_for_query("get_user_by_id")
   if file_info:
       print(f"Query from: {file_info.path}")
       print(f"Checksum: {file_info.checksum}")
       print(f"Loaded at: {file_info.loaded_at}")

   # Get all queries from a specific file
   file_obj = loader.get_file("sql/users.sql")
   if file_obj:
       print(f"Contains {len(file_obj.queries)} queries")
       for query in file_obj.queries:
           print(f"  - {query.name}")

Caching Behavior
^^^^^^^^^^^^^^^^

The loader implements intelligent caching with 12x+ performance improvements:

.. code-block:: python

   # First load - reads from disk
   loader.load_sql("sql/users.sql")

   # Second load - uses cache (file already loaded)
   loader.load_sql("sql/users.sql")

   # Clear cache
   loader.clear_cache()

   # Force reload from disk
   loader.load_sql("sql/users.sql")

**Cache Features**

- File content checksums prevent stale data
- Statement-level caching for instant access
- Automatic invalidation on file changes (in development)
- Configurable cache size limits

Storage Backends
----------------

The loader supports multiple storage backends for loading SQL files.

Local Files
^^^^^^^^^^^

.. code-block:: python

   from pathlib import Path

   # Load from Path object
   loader.load_sql(Path("sql/users.sql"))

   # Load from string path
   loader.load_sql("sql/users.sql")

   # Load directory
   loader.load_sql("sql/")

File URIs
^^^^^^^^^

.. code-block:: python

   # Load from file:// URI
   loader.load_sql("file:///absolute/path/to/queries.sql")

   # Load from relative file URI
   loader.load_sql("file://sql/users.sql")

Cloud Storage (with fsspec)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When ``fsspec`` is installed, load from cloud storage:

.. code-block:: python

   # S3
   loader.load_sql("s3://my-bucket/sql/users.sql")

   # Google Cloud Storage
   loader.load_sql("gs://my-bucket/sql/users.sql")

   # Azure Blob Storage
   loader.load_sql("az://my-container/sql/users.sql")

   # HTTP/HTTPS
   loader.load_sql("https://example.com/queries/users.sql")

Integration with SQLSpec
-------------------------

Loader with SQLSpec Instance
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create a SQLSpec instance with an integrated loader:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.loader import SQLFileLoader

   # Create loader
   loader = SQLFileLoader()
   loader.load_sql("sql/")

   # Create SQLSpec with loader
   spec = SQLSpec(loader=loader)

   # Access loader via SQLSpec
   user_query = spec.loader.get_sql("get_user_by_id", user_id=1)

Type-Safe Query Execution
^^^^^^^^^^^^^^^^^^^^^^^^^^

Combine loaded queries with schema mapping:

.. code-block:: python

   from pydantic import BaseModel
   from datetime import datetime

   class User(BaseModel):
       id: int
       username: str
       email: str
       created_at: datetime

   # Load and execute with type safety
   query = loader.get_sql("get_user_by_id", user_id=1)

   with spec.provide_session(config) as session:
       result = session.execute(query, schema_type=User)
       user: User = result.one()  # Fully typed!

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

.. code-block:: python

   # Python code
   from sqlspec import SQLSpec
   from sqlspec.loader import SQLFileLoader
   from sqlspec.adapters.sqlite import SqliteConfig

   loader = SQLFileLoader()
   loader.load_sql("sql/users.sql")

   spec = SQLSpec()
   config = SqliteConfig()
   spec.add_config(config)

   with spec.provide_session(config) as session:
       # Create user
       create_query = loader.get_sql(
           "create_user",
           username="alice",
           email="alice@example.com",
           password_hash="hashed_password"
       )
       result = session.execute(create_query)
       user = result.one()
       user_id = user['id']

       # Get user
       get_query = loader.get_sql("get_user", user_id=user_id)
       user = session.execute(get_query).one()

       # List users
       list_query = loader.get_sql("list_users", status=True, limit=10, offset=0)
       users = session.execute(list_query).data

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

.. code-block:: python

   import datetime

   # Load analytics queries
   loader.load_sql("sql/analytics.sql")

   # Run daily sales report
   sales_query = loader.get_sql(
       "daily_sales",
       start_date=datetime.date(2025, 1, 1),
       end_date=datetime.date(2025, 2, 1)
   )
   sales = session.execute(sales_query).data

   # Top products
   products_query = loader.get_sql("top_products", start_date=datetime.date(2025, 1, 1), limit=10)
   top_products = session.execute(products_query).data

Example 3: Multi-Database Setup
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Different SQL files for different databases
   loader = SQLFileLoader()
   loader.load_sql("sql/postgres/", "sql/sqlite/", "sql/shared/")

   # Queries automatically select correct dialect
   pg_query = loader.get_sql("upsert_user")  # Uses Postgres ON CONFLICT
   sqlite_query = loader.get_sql("get_user")  # Uses shared query

   # Execute on appropriate database
   async with spec.provide_session(postgres_config) as pg_session:
       await pg_session.execute(pg_query, **params)

   with spec.provide_session(sqlite_config) as sqlite_session:
       sqlite_session.execute(sqlite_query, user_id=1)

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

.. code-block:: python

   try:
       query = loader.get_sql("nonexistent_query")
   except KeyError:
       print("Query not found. Available queries:")
       print(loader.list_queries())

File Load Errors
^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.exceptions import SQLFileNotFoundError, SQLFileParseError

   try:
       loader.load_sql("sql/queries.sql")
   except SQLFileNotFoundError as e:
       print(f"File not found: {e}")
   except SQLFileParseError as e:
       print(f"Failed to parse SQL file: {e}")

Debugging Loaded Queries
^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Print query SQL
   query = loader.get_sql("get_user", user_id=1)
   print(f"SQL: {query}")
   print(f"Parameters: {query.parameters}")

   # Inspect file metadata
   file_info = loader.get_file_for_query("get_user")
   print(f"Loaded from: {file_info.path}")

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
