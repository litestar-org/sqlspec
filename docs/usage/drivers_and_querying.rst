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

.. literalinclude:: /examples/usage/usage_drivers_and_querying_1.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `driver architecture`

PostgreSQL Drivers
------------------

asyncpg (Recommended for Async)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Async PostgreSQL driver with native connection pooling.

.. literalinclude:: /examples/usage/usage_drivers_and_querying_2.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `asyncpg`

**Features**:

- Parameter style: ``$1, $2, ...`` (numeric)
- Native prepared statements
- Binary protocol support
- Connection pooling
- Copy operations for bulk data

psycopg (Sync/Async)
^^^^^^^^^^^^^^^^^^^^

Official PostgreSQL adapter with both sync and async support.

.. tab-set::

   .. tab-item:: Sync

      .. literalinclude:: /examples/usage/usage_drivers_and_querying_3.py
         :language: python
         :start-after: # start-example
         :end-before: # end-example
         :dedent: 2
         :caption: `psycopg sync`

   .. tab-item:: Async

      .. literalinclude:: /examples/usage/usage_drivers_and_querying_4.py
         :language: python
         :start-after: # start-example
         :end-before: # end-example
         :dedent: 2
         :caption: `psycopg async`

**Features**:

- Parameter style: ``%s`` (format) or ``%(name)s`` (pyformat)
- Server-side cursors
- COPY operations
- Binary protocol
- Pipeline mode (psycopg 3)

psqlpy (High Performance Async)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Rust-based async PostgreSQL driver for maximum performance.

.. literalinclude:: /examples/usage/usage_drivers_and_querying_5.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `psqlpy`

**Features**:

- Written in Rust for performance
- Async-first design
- Connection pooling

SQLite Drivers
--------------

sqlite3 (Synchronous)
^^^^^^^^^^^^^^^^^^^^^

Python's built-in SQLite adapter.

.. literalinclude:: /examples/usage/usage_drivers_and_querying_6.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `sqlite config`

.. literalinclude:: /examples/usage/usage_drivers_and_querying_7.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `sqlite`

**Features**:

- Parameter style: ``?`` (qmark)
- Lightweight and embedded
- Thread-local connections
- Script execution support

aiosqlite (Asynchronous)
^^^^^^^^^^^^^^^^^^^^^^^^

Async wrapper around sqlite3.

.. literalinclude:: /examples/usage/usage_drivers_and_querying_8.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `aiosqlite`

**Features**:

- Async interface to SQLite
- Thread pool execution for blocking operations
- Same parameter style as sqlite3

MySQL Drivers
-------------

asyncmy (Asynchronous)
^^^^^^^^^^^^^^^^^^^^^^

Pure Python async MySQL/MariaDB driver.

.. literalinclude:: /examples/usage/usage_drivers_and_querying_9.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `asyncmy`

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

.. literalinclude:: /examples/usage/usage_drivers_and_querying_10.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `duckdb`

**Features**:

- OLAP-optimized query engine
- Parquet/CSV support
- Columnar storage
- Fast aggregations

Oracle Database
^^^^^^^^^^^^^^^

Oracle database support with python-oracledb.

.. literalinclude:: /examples/usage/usage_drivers_and_querying_11.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `oracle`

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

.. literalinclude:: /examples/usage/usage_drivers_and_querying_13.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2
   :caption: `execute`

execute_many()
^^^^^^^^^^^^^^

Execute a statement with multiple parameter sets (batch insert/update).

.. literalinclude:: /examples/usage/usage_drivers_and_querying_14.py
   :language: python
   :start-after: # start-example-1
   :end-before: # end-example-1
   :dedent: 2
   :caption: `execute_many`


select()
^^^^^^^^

Execute a SELECT query and return all rows.

.. literalinclude:: /examples/usage/usage_drivers_and_querying_14.py
   :language: python
   :start-after: # start-example-2
   :end-before: # end-example-2
   :dedent: 4
   :caption: `select`

select_one()
^^^^^^^^^^^^

Execute a SELECT query expecting exactly one result.

.. literalinclude:: /examples/usage/usage_drivers_and_querying_14.py
   :language: python
   :start-after: # start-example-3
   :end-before: # end-example-3
   :dedent: 4
   :caption: `select_one`

select_one_or_none()
^^^^^^^^^^^^^^^^^^^^

Execute a SELECT query returning one or no results.

.. literalinclude:: /examples/usage/usage_drivers_and_querying_14.py
   :language: python
   :start-after: # start-example-4
   :end-before: # end-example-4
   :dedent: 4
   :caption: `select_one_or_none`

select_value()
^^^^^^^^^^^^^^

Execute a SELECT query returning a single scalar value.

.. literalinclude:: /examples/usage/usage_drivers_and_querying_14.py
   :language: python
   :start-after: # start-example-5
   :end-before: # end-example-5
   :dedent: 4
   :caption: `select_value`

Working with Results
--------------------

SQLResult Object
^^^^^^^^^^^^^^^^

All queries return a ``SQLResult`` object with rich result information.

.. literalinclude:: /examples/usage/usage_drivers_and_querying_14.py
   :language: python
   :start-after: # start-example-6
   :end-before: # end-example-6
   :dedent: 4
   :caption: `SQLResult object`

Iterating Results
^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/usage_drivers_and_querying_14.py
   :language: python
   :start-after: # start-example-7
   :end-before: # end-example-7
   :dedent: 4
   :caption: `iterating results`

Schema Mapping
^^^^^^^^^^^^^^

Map results to typed objects automatically.# end-example

.. literalinclude:: /examples/usage/usage_drivers_and_querying_14.py
   :language: python
   :start-after: # start-example-8
   :end-before: # end-example-8
   :dedent: 4
   :caption: `schema mapping`

Transactions
------------

Manual Transaction Control
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/usage_drivers_and_querying_15.py
   :language: python
   :caption: `manual transaction control`
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4


Context Manager Transactions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``session.begin()`` returns a coroutine, so wrap it in your own helper if you
prefer context manager semantics.

.. literalinclude:: /examples/usage/usage_drivers_and_querying_16.py
   :language: python
   :caption: ``async transaction helper``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 2

Parameter Binding
-----------------

Positional Parameters
^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/usage_drivers_and_querying_17.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :caption: ``positional parameters``
   :dedent: 4

Named Parameters
^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/usage_drivers_and_querying_18.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :caption: ``named parameters``
   :dedent: 4

Type Coercion
^^^^^^^^^^^^^

SQLSpec automatically coerces types based on driver requirements:

.. literalinclude:: /examples/usage/usage_drivers_and_querying_19.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :caption: ``type coercion``
   :dedent: 4

Script Execution
----------------

Execute multiple SQL statements in one call:

.. literalinclude:: /examples/usage/usage_drivers_and_querying_20.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :caption: ``script execution``
   :dedent: 4

Performance Tips
----------------

**1. Use Connection Pooling**

.. literalinclude:: /examples/usage/usage_drivers_and_querying_21.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :caption: ``asyncpg connection pooling``
   :dedent: 4

**2. Batch Operations**

Use ``execute_many()`` for bulk inserts:

.. literalinclude:: /examples/usage/usage_drivers_and_querying_22.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example
   :caption: ``batch inserts``
   :dedent: 4

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
