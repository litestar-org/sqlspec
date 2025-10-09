========
Adapters
========

SQLSpec provides database adapters that enable connections to various database systems. Each adapter consists of configuration classes and driver implementations tailored to specific databases, providing a consistent interface while respecting database-specific capabilities.

.. currentmodule:: sqlspec.adapters

Overview
========

Available adapters:

- **PostgreSQL**: asyncpg, psycopg, psqlpy
- **SQLite**: sqlite, aiosqlite
- **MySQL**: asyncmy
- **DuckDB**: duckdb
- **Oracle**: oracledb
- **BigQuery**: bigquery
- **Cross-Database**: ADBC (Arrow Database Connectivity)

Each adapter implementation includes:

1. **Configuration class** - Connection and pool settings
2. **Driver class** - Query execution and transaction management
3. **Type mappings** - Database-specific type conversions
4. **Parameter binding** - Automatic parameter style conversion

PostgreSQL
==========

AsyncPG
-------

.. currentmodule:: sqlspec.adapters.asyncpg

**Homepage**: https://github.com/MagicStack/asyncpg

**PyPI**: https://pypi.org/project/asyncpg/

**Concurrency**: Async-only

**Connection Pooling**: Native pooling via ``asyncpg.Pool``

**Parameter Style**: ``$1, $2, $3`` (PostgreSQL positional placeholders)

**Special Features**:

- Binary protocol for efficient data transfer
- pgvector support (automatic registration when ``pgvector`` is installed)
- Native JSON/JSONB type codecs
- Statement caching
- Prepared statements

**Known Limitations**:

- Async-only (no synchronous support)
- Requires async/await syntax throughout application

**Installation**:

.. code-block:: bash

   uv add sqlspec[asyncpg]

**Configuration**:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgPoolConfig

   sql = SQLSpec()
   db = sql.add_config(
       AsyncpgConfig(
           pool_config=AsyncpgPoolConfig(
               dsn="postgresql://user:password@localhost:5432/mydb",
               min_size=5,
               max_size=20,
               command_timeout=60.0
           )
       )
   )

   async with sql.provide_session(db) as session:
       result = await session.execute("SELECT * FROM users WHERE id = $1", [1])
       user = result.one()

**API Reference**:

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

Psycopg
-------

.. currentmodule:: sqlspec.adapters.psycopg

**Homepage**: https://github.com/psycopg/psycopg

**PyPI**: https://pypi.org/project/psycopg/

**Concurrency**: Sync and async

**Connection Pooling**: Native pooling via ``psycopg_pool``

**Parameter Style**: ``%s, %s, %s`` (PostgreSQL format style)

**Special Features**:

- Both synchronous and asynchronous support
- Modern psycopg3 API
- pgvector support (automatic registration when ``pgvector`` is installed)
- Server-side cursors
- Pipeline mode for batch operations

**Known Limitations**:

- Separate driver classes for sync (``PsycopgSyncDriver``) and async (``PsycopgAsyncDriver``)

**Installation**:

.. code-block:: bash

   uv add sqlspec[psycopg]

**Configuration (Async)**:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.psycopg import PsycopgAsyncConfig

   sql = SQLSpec()
   db = sql.add_config(
       PsycopgAsyncConfig(
           pool_config={
               "conninfo": "postgresql://user:password@localhost:5432/mydb",
               "min_size": 5,
               "max_size": 20
           }
       )
   )

   async with sql.provide_session(db) as session:
       result = await session.execute("SELECT * FROM users WHERE id = %s", [1])

**Configuration (Sync)**:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.psycopg import PsycopgSyncConfig

   sql = SQLSpec()
   db = sql.add_config(
       PsycopgSyncConfig(
           pool_config={
               "conninfo": "postgresql://user:password@localhost:5432/mydb",
               "min_size": 5,
               "max_size": 20
           }
       )
   )

   with sql.provide_session(db) as session:
       result = session.execute("SELECT * FROM users WHERE id = %s", [1])

**API Reference**:

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

psqlpy
------

.. currentmodule:: sqlspec.adapters.psqlpy

**Homepage**: https://github.com/psqlpy-python/psqlpy

**PyPI**: https://pypi.org/project/psqlpy/

**Concurrency**: Async-only

**Connection Pooling**: Native pooling (Rust-based implementation)

**Parameter Style**: ``$1, $2, $3`` (PostgreSQL positional placeholders)

**Special Features**:

- Rust-based driver for memory efficiency
- Connection pooling
- Transaction support
- Type conversion

**Known Limitations**:

- Async-only (no synchronous support)
- Smaller ecosystem than asyncpg or psycopg

**Installation**:

.. code-block:: bash

   uv add sqlspec[psqlpy]

**Configuration**:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.psqlpy import PsqlpyConfig

   sql = SQLSpec()
   db = sql.add_config(
       PsqlpyConfig(
           pool_config={
               "dsn": "postgresql://user:password@localhost:5432/mydb",
               "max_pool_size": 20
           }
       )
   )

   async with sql.provide_session(db) as session:
       result = await session.execute("SELECT * FROM users WHERE id = $1", [1])

**API Reference**:

.. autoclass:: PsqlpyConfig
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: PsqlpyDriver
   :members:
   :undoc-members:
   :show-inheritance:

SQLite
======

sqlite
------

.. currentmodule:: sqlspec.adapters.sqlite

**Homepage**: Built-in Python module

**PyPI**: N/A (included with Python)

**Concurrency**: Sync-only

**Connection Pooling**: Custom thread-local pooling

**Parameter Style**: ``?, ?, ?`` (SQLite positional placeholders)

**Special Features**:

- No external dependencies
- File-based or in-memory databases
- Thread-safe connections
- Custom connection pooling for concurrency

**Known Limitations**:

- Synchronous only (no async support)
- Limited concurrent write performance
- Thread-local connection management

**Installation**:

.. code-block:: bash

   # No installation required - built into Python
   uv add sqlspec

**Configuration (In-Memory)**:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.sqlite import SqliteConfig

   sql = SQLSpec()
   db = sql.add_config(SqliteConfig(database=":memory:"))

   with sql.provide_session(db) as session:
       session.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
       session.execute("INSERT INTO users (name) VALUES (?)", ["Alice"])

**Configuration (File-Based)**:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.sqlite import SqliteConfig

   sql = SQLSpec()
   db = sql.add_config(
       SqliteConfig(
           pool_config={
               "database": "/path/to/database.db",
               "timeout": 30.0,
               "check_same_thread": False
           }
       )
   )

**API Reference**:

.. autoclass:: SqliteConfig
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: SqliteDriver
   :members:
   :undoc-members:
   :show-inheritance:

aiosqlite
---------

.. currentmodule:: sqlspec.adapters.aiosqlite

**Homepage**: https://github.com/omnilib/aiosqlite

**PyPI**: https://pypi.org/project/aiosqlite/

**Concurrency**: Async-only

**Connection Pooling**: Custom pooling

**Parameter Style**: ``?, ?, ?`` (SQLite positional placeholders)

**Special Features**:

- Async wrapper around Python's sqlite3 module
- Same SQLite features as synchronous version
- Compatible with async frameworks

**Known Limitations**:

- Async operations run on thread pool (not true async I/O)
- Limited concurrent write performance (SQLite limitation)

**Installation**:

.. code-block:: bash

   uv add sqlspec[aiosqlite]

**Configuration**:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.aiosqlite import AiosqliteConfig

   sql = SQLSpec()
   db = sql.add_config(
       AiosqliteConfig(
           pool_config={
               "database": "/path/to/database.db",
               "timeout": 30.0
           }
       )
   )

   async with sql.provide_session(db) as session:
       await session.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
       await session.execute("INSERT INTO users (name) VALUES (?)", ["Alice"])

**API Reference**:

.. autoclass:: AiosqliteConfig
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: AiosqliteDriver
   :members:
   :undoc-members:
   :show-inheritance:

MySQL
=====

asyncmy
-------

.. currentmodule:: sqlspec.adapters.asyncmy

**Homepage**: https://github.com/long2ice/asyncmy

**PyPI**: https://pypi.org/project/asyncmy/

**Concurrency**: Async-only

**Connection Pooling**: Native pooling

**Parameter Style**: ``%s, %s, %s`` (MySQL format style)

**Special Features**:

- Cython-based implementation
- Connection pooling
- MySQL protocol support
- Prepared statements

**Known Limitations**:

- Async-only (no synchronous support)
- Requires Cython build tools during installation

**Installation**:

.. code-block:: bash

   uv add sqlspec[asyncmy]

**Configuration**:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncmy import AsyncmyConfig

   sql = SQLSpec()
   db = sql.add_config(
       AsyncmyConfig(
           pool_config={
               "host": "localhost",
               "port": 3306,
               "user": "root",
               "password": "password",
               "database": "mydb",
               "minsize": 5,
               "maxsize": 20
           }
       )
   )

   async with sql.provide_session(db) as session:
       result = await session.execute("SELECT * FROM users WHERE id = %s", [1])

**API Reference**:

.. autoclass:: AsyncmyConfig
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: AsyncmyDriver
   :members:
   :undoc-members:
   :show-inheritance:

DuckDB
======

duckdb
------

.. currentmodule:: sqlspec.adapters.duckdb

**Homepage**: https://github.com/duckdb/duckdb

**PyPI**: https://pypi.org/project/duckdb/

**Concurrency**: Sync-only

**Connection Pooling**: Custom pooling

**Parameter Style**: ``?, ?, ?`` (positional placeholders)

**Special Features**:

- Embedded analytical database (OLAP)
- Native Parquet and CSV support
- Extension management (auto-install/load extensions)
- Secrets management for API integrations
- Arrow-native data transfer
- Direct file querying without import
- Shared memory databases for concurrency

**Known Limitations**:

- Synchronous only (no async support)
- Embedded database (not client-server)

**Installation**:

.. code-block:: bash

   uv add sqlspec[duckdb]

**Configuration (In-Memory)**:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.duckdb import DuckDBConfig

   sql = SQLSpec()
   db = sql.add_config(DuckDBConfig())  # Defaults to :memory:shared_db

   with sql.provide_session(db) as session:
       # Query Parquet file directly
       result = session.execute("SELECT * FROM 'data.parquet' LIMIT 10")

**Configuration (Persistent with Extensions)**:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBExtensionConfig

   sql = SQLSpec()
   db = sql.add_config(
       DuckDBConfig(
           pool_config={
               "database": "/path/to/analytics.db",
               "threads": 4,
               "memory_limit": "4GB"
           },
           driver_features={
               "extensions": [
                   DuckDBExtensionConfig(name="httpfs"),
                   DuckDBExtensionConfig(name="parquet")
               ]
           }
       )
   )

   with sql.provide_session(db) as session:
       # Query remote Parquet file
       result = session.execute(
           "SELECT * FROM 'https://example.com/data.parquet' LIMIT 10"
       )

**API Reference**:

.. autoclass:: DuckDBConfig
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: DuckDBDriver
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: DuckDBExtensionConfig
   :members:
   :undoc-members:

.. autoclass:: DuckDBSecretConfig
   :members:
   :undoc-members:

BigQuery
========

bigquery
--------

.. currentmodule:: sqlspec.adapters.bigquery

**Homepage**: https://github.com/googleapis/python-bigquery

**PyPI**: https://pypi.org/project/google-cloud-bigquery/

**Concurrency**: Sync-only

**Connection Pooling**: None (stateless HTTP API)

**Parameter Style**: ``@param1, @param2`` (named parameters)

**Special Features**:

- Google Cloud BigQuery integration
- Serverless query execution
- Standard SQL dialect
- Automatic result pagination

**Known Limitations**:

- Synchronous only (no async support)
- Requires Google Cloud credentials
- Query costs based on data scanned
- No connection pooling (stateless API)

**Installation**:

.. code-block:: bash

   uv add sqlspec[bigquery]

**Configuration**:

.. code-block:: python

   from google.oauth2 import service_account
   from sqlspec import SQLSpec
   from sqlspec.adapters.bigquery import BigQueryConfig

   credentials = service_account.Credentials.from_service_account_file(
       "/path/to/credentials.json"
   )

   sql = SQLSpec()
   db = sql.add_config(
       BigQueryConfig(
           pool_config={
               "project": "my-project-id",
               "credentials": credentials
           }
       )
   )

   with sql.provide_session(db) as session:
       result = session.execute(
           "SELECT * FROM `project.dataset.table` WHERE id = @user_id",
           {"user_id": 1}
       )

**API Reference**:

.. autoclass:: BigQueryConfig
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: BigQueryDriver
   :members:
   :undoc-members:
   :show-inheritance:

Oracle
======

oracledb
--------

.. currentmodule:: sqlspec.adapters.oracledb

**Homepage**: https://github.com/oracle/python-oracledb

**PyPI**: https://pypi.org/project/oracledb/

**Concurrency**: Sync and async

**Connection Pooling**: Native pooling

**Parameter Style**: ``:1, :2, :3`` (Oracle positional placeholders)

**Special Features**:

- Both synchronous and asynchronous support
- Thin mode (pure Python) and thick mode (Oracle Client)
- Oracle-specific data types (NUMBER, CLOB, BLOB, etc.)
- Connection pooling
- Two-phase commit support

**Known Limitations**:

- Separate configuration classes for sync and async
- Thick mode requires Oracle Instant Client installation

**Installation**:

.. code-block:: bash

   uv add sqlspec[oracledb]

**Configuration (Async, Thin Mode)**:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.oracledb import OracleAsyncConfig

   sql = SQLSpec()
   db = sql.add_config(
       OracleAsyncConfig(
           pool_config={
               "user": "system",
               "password": "oracle",
               "dsn": "localhost:1521/XE",
               "min": 5,
               "max": 20
           }
       )
   )

   async with sql.provide_session(db) as session:
       result = await session.execute("SELECT * FROM users WHERE id = :1", [1])

**Configuration (Sync, Thin Mode)**:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.oracledb import OracleSyncConfig

   sql = SQLSpec()
   db = sql.add_config(
       OracleSyncConfig(
           pool_config={
               "user": "system",
               "password": "oracle",
               "dsn": "localhost:1521/XE"
           }
       )
   )

   with sql.provide_session(db) as session:
       result = session.execute("SELECT * FROM users WHERE id = :1", [1])

**API Reference**:

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

Cross-Database
==============

ADBC (Arrow Database Connectivity)
-----------------------------------

.. currentmodule:: sqlspec.adapters.adbc

**Homepage**: https://github.com/apache/arrow-adbc

**PyPI**: Various (backend-specific):

- PostgreSQL: https://pypi.org/project/adbc-driver-postgresql/
- SQLite: https://pypi.org/project/adbc-driver-sqlite/
- DuckDB: https://pypi.org/project/adbc-driver-duckdb/
- BigQuery: https://pypi.org/project/adbc-driver-bigquery/
- Snowflake: https://pypi.org/project/adbc-driver-snowflake/

**Concurrency**: Sync-only

**Connection Pooling**: None (stateless connections)

**Parameter Style**: Varies by backend

- PostgreSQL: ``$1, $2, $3`` (numeric)
- SQLite: ``?, ?, ?`` (qmark)
- DuckDB: ``?, ?, ?`` (qmark)
- BigQuery: ``@param1, @param2`` (named_at)
- Snowflake: ``?, ?, ?`` (qmark)

**Special Features**:

- Arrow-native data transfer (zero-copy)
- Multi-backend support with unified interface
- Direct Arrow table output
- Automatic driver detection from URI or driver name
- Efficient bulk data operations

**Known Limitations**:

- Synchronous only (no async support)
- No connection pooling
- Backend-specific driver packages required
- Parameter style varies by backend

**Installation**:

.. code-block:: bash

   # PostgreSQL backend
   uv add sqlspec[adbc-postgresql]

   # SQLite backend
   uv add sqlspec[adbc-sqlite]

   # DuckDB backend
   uv add sqlspec[adbc-duckdb]

   # BigQuery backend
   uv add sqlspec[adbc-bigquery]

   # Snowflake backend
   uv add sqlspec[adbc-snowflake]

**Configuration (PostgreSQL)**:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.adbc import AdbcConfig

   sql = SQLSpec()
   db = sql.add_config(
       AdbcConfig(
           connection_config={
               "driver_name": "postgresql",
               "uri": "postgresql://user:password@localhost:5432/mydb"
           }
       )
   )

   with sql.provide_session(db) as session:
       result = session.execute("SELECT * FROM users WHERE id = $1", [1])
       # Get Arrow table directly
       arrow_table = result.arrow()

**Configuration (SQLite)**:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.adbc import AdbcConfig

   sql = SQLSpec()
   db = sql.add_config(
       AdbcConfig(
           connection_config={
               "driver_name": "sqlite",
               "uri": "/path/to/database.db"
           }
       )
   )

   with sql.provide_session(db) as session:
       result = session.execute("SELECT * FROM users WHERE id = ?", [1])

**Configuration (DuckDB)**:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.adbc import AdbcConfig

   sql = SQLSpec()
   db = sql.add_config(
       AdbcConfig(
           connection_config={
               "driver_name": "duckdb",
               "uri": "/path/to/analytics.db"
           }
       )
   )

   with sql.provide_session(db) as session:
       result = session.execute("SELECT * FROM 'data.parquet' LIMIT 10")

**API Reference**:

.. autoclass:: AdbcConfig
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: AdbcDriver
   :members:
   :undoc-members:
   :show-inheritance:

Adapter Architecture
====================

Common Patterns
---------------

All SQLSpec adapters follow consistent architectural patterns:

**Configuration Class**

Each adapter provides a configuration class that inherits from either:

- ``AsyncDatabaseConfig`` - For async adapters
- ``SyncDatabaseConfig`` - For sync adapters
- ``NoPoolSyncConfig`` - For stateless adapters (ADBC, BigQuery)

Configuration classes handle:

- Connection parameters
- Pool settings (when applicable)
- Driver-specific features
- Statement configuration
- Migration settings

**Driver Class**

Driver classes inherit from:

- ``BaseAsyncDriver`` - For async query execution
- ``BaseSyncDriver`` - For sync query execution

Driver classes provide:

- Query execution methods (``execute``, ``select_one``, ``select_all``, etc.)
- Transaction management (``begin``, ``commit``, ``rollback``)
- Result processing and type mapping
- Parameter binding

**Type Mappings**

Each adapter includes database-specific type mappings in ``_types.py``:

- Python to database type conversions
- Database to Python type conversions
- Custom type handlers (JSON, UUID, arrays, etc.)

Parameter Binding
-----------------

SQLSpec automatically converts parameter placeholders to database-specific styles:

.. list-table::
   :header-rows: 1
   :widths: 20 20 30 30

   * - Database
     - Adapter
     - Style
     - Example
   * - PostgreSQL
     - asyncpg, psqlpy
     - ``$1, $2``
     - ``SELECT * FROM users WHERE id = $1``
   * - PostgreSQL
     - psycopg
     - ``%s``
     - ``SELECT * FROM users WHERE id = %s``
   * - SQLite
     - sqlite, aiosqlite
     - ``?``
     - ``SELECT * FROM users WHERE id = ?``
   * - MySQL
     - asyncmy
     - ``%s``
     - ``SELECT * FROM users WHERE id = %s``
   * - DuckDB
     - duckdb
     - ``?``
     - ``SELECT * FROM users WHERE id = ?``
   * - Oracle
     - oracledb
     - ``:1, :2``
     - ``SELECT * FROM users WHERE id = :1``
   * - BigQuery
     - bigquery
     - ``@param``
     - ``SELECT * FROM users WHERE id = @user_id``
   * - ADBC
     - adbc (varies)
     - Backend-specific
     - See backend documentation

Connection Pooling Types
-------------------------

**Native Pooling**

Adapters with native pooling use database driver's built-in pool:

- asyncpg (``asyncpg.Pool``)
- psycopg (``psycopg_pool``)
- asyncmy (native pooling)
- oracledb (``oracledb.create_pool``)

**Custom Pooling**

SQLSpec provides custom pooling for adapters without native support:

- sqlite (thread-local pooling)
- aiosqlite (async pooling)
- duckdb (connection pooling)

**No Pooling**

Stateless adapters create connections per request:

- ADBC (stateless Arrow connections)
- BigQuery (stateless HTTP API)

Creating Custom Adapters
=========================

For information on creating custom database adapters, see:

- :doc:`/contributing/creating_adapters` - Adapter development guide
- :doc:`driver` - Driver implementation details
- :doc:`base` - SQLSpec configuration system

See Also
========

- :doc:`/usage/configuration` - Configuration guide
- :doc:`/usage/drivers_and_querying` - Query execution
- :doc:`/examples/index` - Usage examples
- :doc:`driver` - Driver reference
- :doc:`base` - SQLSpec registry
