Drivers and Querying
=====================
Drivers and Querying
=====================

SQLSpec provides unified database drivers for multiple database systems, both synchronous and asynchronous. This guide covers all available drivers and query execution methods.

Overview
--------

SQLSpec supports 10+ database backends through adapter drivers:

.. grid:: 2

   .. grid-item-card:: PostgreSQL
      :columns: 6

      - asyncpg (async)
      - psycopg (sync/async)
      - psqlpy (async)
      - ADBC (sync/async)

   .. grid-item-card:: SQLite
      :columns: 6

      - sqlite3 (sync)
      - aiosqlite (async)
      - ADBC (sync/async)

   .. grid-item-card:: MySQL
      :columns: 6

      - asyncmy (async)

   .. grid-item-card:: Other Databases
      :columns: 6

      - DuckDB (sync)
      - Oracle (sync/async)
      - BigQuery (sync)

All drivers implement a consistent API for query execution.

Driver Architecture
-------------------

SQLSpec drivers follow a layered architecture:

1. **Config Layer**: Database connection parameters
2. **Pool Layer**: Connection pooling (where supported)
3. **Driver Layer**: Query execution and result handling
4. **Session Layer**: Transaction management

.. literalinclude:: /examples/test_drivers_and_querying_1.py
   :language: python
   :lines: 15-29
   :dedent: 2

PostgreSQL Drivers
------------------

asyncpg (Recommended for Async)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Async PostgreSQL driver with native connection pooling.

.. literalinclude:: /examples/test_drivers_and_querying_2.py
   :language: python
   :lines: 11-35
   :dedent: 2

**Features**:

- Parameter style: ``$1, $2, ...`` (numeric)
- Native prepared statements
- Binary protocol support
- Connection pooling
- Copy operations for bulk data

psycopg (Sync/Async)
^^^^^^^^^^^^^^^^^^^^

Official PostgreSQL adapter with both sync and async support.

.. literalinclude:: /examples/test_drivers_and_querying_3.py
   :language: python
   :lines: 8-32
   :dedent: 2

.. literalinclude:: /examples/test_drivers_and_querying_4.py
   :language: python
   :lines: 10-12
   :dedent: 2

**Features**:

- Parameter style: ``%s`` (format) or ``%(name)s`` (pyformat)
- Server-side cursors
- COPY operations
- Binary protocol
- Pipeline mode (psycopg 3)

psqlpy (High Performance Async)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Rust-based async PostgreSQL driver for maximum performance.

.. literalinclude:: /examples/test_drivers_and_querying_5.py
   :language: python
   :lines: 11-25
   :dedent: 2

**Features**:

- Written in Rust for performance
- Async-first design
- Connection pooling

SQLite Drivers
--------------

sqlite3 (Synchronous)
^^^^^^^^^^^^^^^^^^^^^

Python's built-in SQLite adapter.

.. literalinclude:: /examples/test_drivers_and_querying_6.py
   :language: python
   :lines: 9-27
   :dedent: 2

.. literalinclude:: /examples/test_drivers_and_querying_7.py
   :language: python
   :lines: 8-9
   :dedent: 2

**Features**:

- Parameter style: ``?`` (qmark)
- Lightweight and embedded
- Thread-local connections
- Script execution support

aiosqlite (Asynchronous)
^^^^^^^^^^^^^^^^^^^^^^^^

Async wrapper around sqlite3.

.. literalinclude:: /examples/test_drivers_and_querying_8.py
   :language: python
   :lines: 7-25
   :dedent: 2

**Features**:

- Async interface to SQLite
- Thread pool execution for blocking operations
- Same parameter style as sqlite3

MySQL Drivers
-------------

asyncmy (Asynchronous)
^^^^^^^^^^^^^^^^^^^^^^

Pure Python async MySQL/MariaDB driver.

.. literalinclude:: /examples/test_drivers_and_querying_9.py
   :language: python
   :lines: 9-38
   :dedent: 2

**Features**:

- Parameter style: ``%s`` (format)
- Connection pooling
- MySQL-specific types
- Character set support

Other Database Drivers
----------------------

DuckDB (Analytical Database)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In-process analytical database optimized for OLAP workloads.

.. literalinclude:: /examples/test_drivers_and_querying_10.py
   :language: python
   :lines: 8-34
   :dedent: 2

**Features**:

- OLAP-optimized query engine
- Parquet/CSV support
- Columnar storage
- Fast aggregations

Oracle Database
^^^^^^^^^^^^^^^

Oracle database support with python-oracledb.

.. literalinclude:: /examples/test_drivers_and_querying_11.py
   :language: python
   :lines: 8-33
   :dedent: 2

**Features**:

- Parameter style: ``:name`` (named)
- Both sync and async modes
- Connection pooling
- Oracle-specific types

BigQuery
^^^^^^^^

Google Cloud BigQuery for large-scale analytics.

.. code-block:: python

   from sqlspec.adapters.bigquery import BigQueryConfig

   config = BigQueryConfig(
       pool_config={
           "project": "my-project",
           "credentials": credentials_object,
       }
   )

   with spec.provide_session(config) as session:
       result = session.execute("""
           SELECT DATE(timestamp) as date,
                  COUNT(*) as events
           FROM `project.dataset.events`
           WHERE timestamp >= @start_date
           GROUP BY date
       """, start_date=datetime.date(2025, 1, 1))

**Features**:

- Parameter style: ``@name`` (named_at)
- Job-based execution
- Massive scale analytics
- Standard SQL support

Query Execution Methods
------------------------

All drivers support these query execution methods through sessions.

execute()
^^^^^^^^^

Execute any SQL statement and return results.

.. code-block:: python

   # SELECT query
   result = session.execute("SELECT * FROM users WHERE id = ?", 1)

   # INSERT query
   result = session.execute(
       "INSERT INTO users (name, email) VALUES (?, ?)",
       "Alice",
       "alice@example.com"
   )

   # UPDATE query
   result = session.execute(
       "UPDATE users SET email = ? WHERE id = ?",
       "newemail@example.com",
       1
   )
   print(f"Updated {result.rows_affected} rows")

   # DELETE query
   result = session.execute("DELETE FROM users WHERE id = ?", 1)

execute_many()
^^^^^^^^^^^^^^

Execute a statement with multiple parameter sets (batch insert/update).

.. code-block:: python

   # Batch insert
   session.execute_many(
       "INSERT INTO users (name, email) VALUES (?, ?)",
       [
           ("Alice", "alice@example.com"),
           ("Bob", "bob@example.com"),
           ("Charlie", "charlie@example.com"),
       ]
   )

   # Batch update
   session.execute_many(
       "UPDATE users SET status = ? WHERE id = ?",
       [
           ("active", 1),
           ("inactive", 2),
       ]
   )

select()
^^^^^^^^

Execute a SELECT query and return all rows.

.. code-block:: python

   users = session.execute("SELECT * FROM users WHERE status = ?", "active")
   # Returns list of dictionaries: [{"id": 1, "name": "Alice", ...}, ...]

select_one()
^^^^^^^^^^^^

Execute a SELECT query expecting exactly one result.

.. code-block:: python

   user = session.select_one("SELECT * FROM users WHERE id = ?", 1)
   # Returns single dictionary: {"id": 1, "name": "Alice", ...}
   # Raises NotFoundError if no results
   # Raises MultipleResultsFoundError if multiple results

select_one_or_none()
^^^^^^^^^^^^^^^^^^^^

Execute a SELECT query returning one or no results.

.. code-block:: python

   user = session.select_one_or_none("SELECT * FROM users WHERE email = ?", "nobody@example.com")
   # Returns dictionary or None
   # Raises MultipleResultsFoundError if multiple results

select_value()
^^^^^^^^^^^^^^

Execute a SELECT query returning a single scalar value.

.. code-block:: python

   count = session.select_value("SELECT COUNT(*) FROM users")
   # Returns: 42

   latest_id = session.select_value("SELECT MAX(id) FROM users")
   # Returns: 100

Working with Results
--------------------

SQLResult Object
^^^^^^^^^^^^^^^^

All queries return a ``SQLResult`` object with rich result information.

.. code-block:: python

   result = session.execute("SELECT id, name, email FROM users")

   # Access raw data
   result.data              # List of dictionaries
   result.column_names      # ["id", "name", "email"]
   result.rows_affected     # For INSERT/UPDATE/DELETE
   result.operation_type    # "SELECT", "INSERT", etc.

   # Convenience methods
   user = result.one()              # Single row (raises if not exactly 1)
   user = result.one_or_none()      # Single row or None
   value = result.scalar()          # First column of first row

Iterating Results
^^^^^^^^^^^^^^^^^

.. code-block:: python

   result = session.execute("SELECT * FROM users")

   # Get all rows and iterate
   users = result.all()
   for user in users:
       print(f"{user['name']}: {user['email']}")

   # List comprehension
   names = [user['name'] for user in result.all()]

Schema Mapping
^^^^^^^^^^^^^^

Map results to typed objects automatically.

.. code-block:: python

   from pydantic import BaseModel

   class User(BaseModel):
       id: int
       name: str
       email: str

   # Execute query
   result = session.execute("SELECT id, name, email FROM users")

   # Map results to typed User instances
   users: list[User] = result.all(schema_type=User)

   # Or get single typed result
   user_result = session.execute("SELECT id, name, email FROM users WHERE id = ?", 1)
   user: User = user_result.one(schema_type=User)

Transactions
------------

Manual Transaction Control
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   with spec.provide_session(config) as session:
       try:
           session.begin()

           session.execute("INSERT INTO users (name) VALUES (?)", "Alice")
           session.execute("INSERT INTO logs (action) VALUES (?)", "user_created")

           session.commit()
       except Exception:
           session.rollback()
           raise

Context Manager Transactions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   async with spec.provide_session(config) as session:
       async with session.begin():
           await session.execute("UPDATE accounts SET balance = balance - 100 WHERE id = ?", 1)
           await session.execute("UPDATE accounts SET balance = balance + 100 WHERE id = ?", 2)
           # Auto-commits on success, auto-rollbacks on exception

Parameter Binding
-----------------

Positional Parameters
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # SQLite, DuckDB (?)
   session.execute("SELECT * FROM users WHERE id = ?", 1)

   # PostgreSQL (asyncpg) ($1, $2, ...)
   session.execute("SELECT * FROM users WHERE id = $1 AND status = $2", 1, "active")

   # MySQL (%s)
   session.execute("SELECT * FROM users WHERE id = %s", 1)

Named Parameters
^^^^^^^^^^^^^^^^

.. code-block:: python

   # SQLite, Oracle (:name)
   session.execute(
       "SELECT * FROM users WHERE id = :id AND status = :status",
       id=1,
       status="active"
   )

   # BigQuery (@name)
   session.execute(
       "SELECT * FROM users WHERE created_at >= @start_date",
       start_date=datetime.date(2025, 1, 1)
   )

Type Coercion
^^^^^^^^^^^^^

SQLSpec automatically coerces types based on driver requirements:

.. code-block:: python

   # Booleans to integers (SQLite)
   session.execute("INSERT INTO users (is_active) VALUES (?)", True)
   # SQLite receives: 1

   # Datetime to ISO format (JSON databases)
   session.execute(
       "INSERT INTO events (timestamp) VALUES (?)",
       datetime.datetime.now()
   )

Script Execution
----------------

Execute multiple SQL statements in one call:

.. code-block:: python

   session.execute("""
       CREATE TABLE users (
           id INTEGER PRIMARY KEY,
           name TEXT NOT NULL
       );

       CREATE TABLE posts (
           id INTEGER PRIMARY KEY,
           user_id INTEGER,
           title TEXT,
           FOREIGN KEY (user_id) REFERENCES users(id)
       );

       CREATE INDEX idx_posts_user_id ON posts(user_id);
   """)

Performance Tips
----------------

**1. Use Connection Pooling**

.. code-block:: python

   config = AsyncpgConfig(
       pool_config={
           "dsn": "postgresql://localhost/db",
           "min_size": 10,
           "max_size": 20,
       }
   )

**2. Batch Operations**

Use ``execute_many()`` for bulk inserts:

.. code-block:: python

   # Fast batch insert
   session.execute_many(
       "INSERT INTO users (name) VALUES (?)",
       [(name,) for name in large_list]
   )

**3. Prepared Statements**

Drivers like asyncpg automatically prepare frequently-used statements.

**4. Use Appropriate Methods**

.. code-block:: python

   # Instead of:
   result = session.execute("SELECT COUNT(*) FROM users")
   count = result.scalar()

   # Use:
   count = session.select_value("SELECT COUNT(*) FROM users")

Driver Selection Guide
----------------------

Choose the right driver for your use case:

.. list-table::
   :header-rows: 1
   :widths: 20 20 60

   * - Database
     - Recommended Driver
     - Use Case
   * - PostgreSQL (Async)
     - asyncpg
     - Async applications with connection pooling
   * - PostgreSQL (Sync)
     - psycopg
     - Traditional sync applications
   * - SQLite (Async)
     - aiosqlite
     - Async web applications with SQLite
   * - SQLite (Sync)
     - sqlite3
     - Simple applications, testing
   * - MySQL (Async)
     - asyncmy
     - Async MySQL applications
   * - Analytics
     - DuckDB
     - OLAP, data analysis, reporting
   * - Cloud Analytics
     - BigQuery
     - Large-scale cloud data warehousing

Next Steps
----------

- :doc:`query_builder` - Build queries programmatically
- :doc:`sql_files` - Load queries from SQL files
- :doc:`../reference/driver` - Detailed driver API reference

See Also
--------

- :doc:`configuration` - Configure database connections
- :doc:`data_flow` - Understanding query execution
- :doc:`framework_integrations` - Framework-specific usage
