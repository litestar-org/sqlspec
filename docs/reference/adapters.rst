========
Adapters
========

Database adapters provide SQLSpec with the ability to connect to and interact with different database systems. Each adapter consists of configuration classes and driver implementations tailored to specific databases.

.. currentmodule:: sqlspec.adapters

Overview
========

SQLSpec includes adapters for:

- **PostgreSQL**: asyncpg, psycopg (sync/async), psqlpy
- **SQLite**: sqlite3, aiosqlite
- **MySQL**: asyncmy
- **DuckDB**: duckdb
- **Oracle**: oracledb (sync/async)
- **BigQuery**: google-cloud-bigquery
- **ADBC**: Arrow Database Connectivity

Each adapter provides:

1. **Config class** - Database connection configuration
2. **Driver class** - Query execution and transaction management
3. **Type mappings** - Database-specific type conversions
4. **Parameter style** - Proper parameter binding (?, $1, :name, etc.)

PostgreSQL Adapters
===================

AsyncPG (Async, High Performance)
----------------------------------

.. currentmodule:: sqlspec.adapters.asyncpg

.. autoclass:: AsyncpgConfig
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: AsyncpgDriver
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: AsyncpgPoolConfig
   :members:
   :undoc-members:
   :show-inheritance:

**Features:**

- Native async/await support
- Connection pooling with asyncpg.Pool
- Fast binary protocol
- Best performance for async PostgreSQL

**Parameter style:** ``$1, $2, $3`` (PostgreSQL positional)

**Example:**

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgPoolConfig

   sql = SQLSpec()
   config = AsyncpgConfig(
       pool_config=AsyncpgPoolConfig(
           dsn="postgresql://user:pass@localhost/db",
           min_size=5,
           max_size=20
       )
   )
   sql.add_config(config)

Psycopg (Sync/Async, Modern)
-----------------------------

.. currentmodule:: sqlspec.adapters.psycopg

.. autoclass:: PsycopgConfig
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: PsycopgSyncDriver
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: PsycopgAsyncDriver
   :members:
   :undoc-members:
   :show-inheritance:

**Features:**

- Both sync and async support
- Modern psycopg3 API
- Connection pooling (psycopg_pool)
- Server-side cursors

**Parameter style:** ``%s, %s, %s`` (PostgreSQL format style)

**Example:**

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.psycopg import PsycopgConfig

   # Async usage
   config = PsycopgConfig(
       pool_config={"conninfo": "postgresql://user:pass@localhost/db"},
       is_async=True
   )

   # Sync usage
   config_sync = PsycopgConfig(
       pool_config={"conninfo": "postgresql://user:pass@localhost/db"},
       is_async=False
   )

psqlpy (Rust-based, Ultra High Performance)
--------------------------------------------

.. currentmodule:: sqlspec.adapters.psqlpy

.. autoclass:: PsqlpyConfig
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: PsqlpyDriver
   :members:
   :undoc-members:
   :show-inheritance:

**Features:**

- Rust-based for maximum performance
- Zero-copy result handling
- Connection pooling
- Async-only

**Parameter style:** ``$1, $2, $3`` (PostgreSQL positional)

SQLite Adapters
===============

SQLite (Sync, Built-in)
-----------------------

.. currentmodule:: sqlspec.adapters.sqlite

.. autoclass:: SqliteConfig
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: SqliteDriver
   :members:
   :undoc-members:
   :show-inheritance:

**Features:**

- Uses Python's built-in sqlite3
- File-based or in-memory databases
- Simple setup for development
- Thread-safe connections

**Parameter style:** ``?, ?, ?`` (SQLite positional)

**Example:**

.. code-block:: python

   from sqlspec.adapters.sqlite import SqliteConfig

   # In-memory database
   config = SqliteConfig(database=":memory:")

   # File-based database
   config = SqliteConfig(database="/path/to/db.sqlite")

aiosqlite (Async)
-----------------

.. currentmodule:: sqlspec.adapters.aiosqlite

.. autoclass:: AiosqliteConfig
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: AiosqliteDriver
   :members:
   :undoc-members:
   :show-inheritance:

**Features:**

- Async wrapper around sqlite3
- Same SQLite features as sync version
- Non-blocking operations

**Parameter style:** ``?, ?, ?`` (SQLite positional)

DuckDB Adapter
==============

.. currentmodule:: sqlspec.adapters.duckdb

.. autoclass:: DuckDBConfig
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: DuckDBDriver
   :members:
   :undoc-members:
   :show-inheritance:

**Features:**

- Embedded analytical database
- Parquet/CSV support
- OLAP optimized
- In-memory or persistent

**Parameter style:** ``?, ?, ?`` (positional)

**Example:**

.. code-block:: python

   from sqlspec.adapters.duckdb import DuckDBConfig

   # In-memory analytics
   config = DuckDBConfig(database=":memory:")

   # Read from Parquet
   with sql.provide_session(config) as session:
       session.execute("CREATE TABLE users AS SELECT * FROM 'data.parquet'")

MySQL Adapter
=============

.. currentmodule:: sqlspec.adapters.asyncmy

.. autoclass:: AsyncmyConfig
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: AsyncmyDriver
   :members:
   :undoc-members:
   :show-inheritance:

**Features:**

- Async MySQL client
- Connection pooling
- Full MySQL feature support

**Parameter style:** ``%s, %s, %s`` (MySQL format)

**Example:**

.. code-block:: python

   from sqlspec.adapters.asyncmy import AsyncmyConfig

   config = AsyncmyConfig(
       pool_config={
           "host": "localhost",
           "user": "root",
           "password": "password",
           "database": "mydb"
       }
   )

Oracle Adapter
==============

.. currentmodule:: sqlspec.adapters.oracledb

.. autoclass:: OracleDBConfig
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: OracleDBSyncDriver
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: OracleDBAsyncDriver
   :members:
   :undoc-members:
   :show-inheritance:

**Features:**

- Both sync and async support
- Oracle-specific types
- Thick/thin client modes

**Parameter style:** ``:1, :2, :3`` (Oracle positional)

**Example:**

.. code-block:: python

   from sqlspec.adapters.oracledb import OracleDBConfig

   config = OracleDBConfig(
       pool_config={
           "user": "system",
           "password": "oracle",
           "dsn": "localhost:1521/xe"
       },
       is_async=True
   )

BigQuery Adapter
================

.. currentmodule:: sqlspec.adapters.bigquery

.. autoclass:: BigQueryConfig
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: BigQueryDriver
   :members:
   :undoc-members:
   :show-inheritance:

**Features:**

- Google Cloud BigQuery integration
- Serverless analytics
- Petabyte-scale queries

**Parameter style:** ``@param1, @param2`` (named parameters)

**Example:**

.. code-block:: python

   from sqlspec.adapters.bigquery import BigQueryConfig

   config = BigQueryConfig(
       pool_config={
           "project": "my-project",
           "credentials": credentials_obj
       }
   )

ADBC Adapter
============

.. currentmodule:: sqlspec.adapters.adbc

.. autoclass:: ADBCConfig
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: ADBCDriver
   :members:
   :undoc-members:
   :show-inheritance:

**Features:**

- Arrow Database Connectivity standard
- Zero-copy data transfer with Arrow
- Multiple backend support (PostgreSQL, SQLite, FlightSQL)

**Example:**

.. code-block:: python

   from sqlspec.adapters.adbc import ADBCConfig

   # PostgreSQL via ADBC
   config = ADBCConfig(
       driver="adbc_driver_postgresql",
       pool_config={"uri": "postgresql://localhost/db"}
   )

Adapter Architecture
====================

Common Pattern
--------------

All adapters follow a consistent pattern:

1. **Configuration** (``*Config`` class)

   - Connection parameters
   - Pool configuration
   - Adapter-specific options

2. **Driver** (``*Driver`` class)

   - Inherits from ``BaseSyncDriver`` or ``BaseAsyncDriver``
   - Implements query execution
   - Handles transactions

3. **Type System** (``_types.py``)

   - Database-specific type mappings
   - Result serialization

Parameter Binding
-----------------

Each adapter converts SQLSpec's parameter format to database-specific style:

.. list-table::
   :header-rows: 1
   :widths: 20 30 50

   * - Database
     - Style
     - Example
   * - PostgreSQL (asyncpg, psqlpy)
     - Positional ($1, $2)
     - ``SELECT * FROM users WHERE id = $1``
   * - PostgreSQL (psycopg)
     - Format (%s)
     - ``SELECT * FROM users WHERE id = %s``
   * - SQLite
     - Positional (?)
     - ``SELECT * FROM users WHERE id = ?``
   * - MySQL
     - Format (%s)
     - ``SELECT * FROM users WHERE id = %s``
   * - Oracle
     - Positional (:1, :2)
     - ``SELECT * FROM users WHERE id = :1``
   * - BigQuery
     - Named (@param)
     - ``SELECT * FROM users WHERE id = @user_id``

Choosing an Adapter
===================

**For PostgreSQL:**

- **asyncpg** - Best async performance, native pooling
- **psycopg** - Modern API, sync/async flexibility
- **psqlpy** - Maximum performance, Rust-based

**For SQLite:**

- **sqlite** - Simple sync operations, development
- **aiosqlite** - Async operations, async web apps

**For Analytics:**

- **DuckDB** - Embedded analytics, Parquet support
- **BigQuery** - Cloud analytics, large datasets

**For MySQL:**

- **asyncmy** - Async MySQL operations

**For Oracle:**

- **oracledb** - Official Oracle driver, sync/async

**For Cross-Database:**

- **ADBC** - Standard interface, Arrow integration

See Also
========

- :doc:`/usage/configuration` - Configuration guide
- :doc:`/examples/index` - Adapter examples
- :doc:`driver` - Driver implementation
- :doc:`base` - SQLSpec registry
