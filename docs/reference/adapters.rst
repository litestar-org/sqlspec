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
- **Spanner**: spanner
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

**Driver Features**:

- ``json_serializer`` – Override JSON parameter serialization for ``dict``/``list``/``tuple`` inputs. Defaults to
  :func:`sqlspec.utils.serializers.to_json` and is invoked for every JSON parameter passed to AsyncMy.
- ``json_deserializer`` – Customize JSON result decoding. Defaults to
  :func:`sqlspec.utils.serializers.from_json` and automatically converts JSON columns into Python objects during fetches.

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

**Community Extensions**:

DuckDBConfig accepts the runtime flags DuckDB expects for community/unsigned extensions via
``pool_config`` (for example ``allow_community_extensions=True``,
``allow_unsigned_extensions=True``, ``enable_external_access=True``). SQLSpec applies those
options with ``SET`` statements immediately after establishing each connection, so even older
DuckDB builds that do not recognize the options during ``duckdb.connect()`` will still enable the
required permissions before extensions are installed.

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

Spanner
=======

spanner
-------

.. currentmodule:: sqlspec.adapters.spanner

**Homepage**: https://github.com/googleapis/python-spanner

**PyPI**: https://pypi.org/project/google-cloud-spanner/

**Concurrency**: Sync-only

**Connection Pooling**: Native session pooling via ``google.cloud.spanner_v1.pool``

**Parameter Style**: ``@param1, @param2`` (named parameters)

**Special Features**:

- Google Cloud Spanner integration
- Full ACID transactions
- Interleaved tables for co-location
- Row-level TTL policies
- Custom SQLglot dialect (GoogleSQL and PostgreSQL modes)
- Native Arrow support via conversion
- UUID auto-conversion
- JSON type handling

**Known Limitations**:

- Synchronous only (no async support in current implementation)
- DDL operations require separate admin API calls
- Requires Google Cloud credentials
- 20,000 mutation limit per transaction

**Installation**:

.. code-block:: bash

   uv add sqlspec[spanner]

**Configuration**:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.spanner import SpannerSyncConfig

   sql = SQLSpec()
   db = sql.add_config(
       SpannerSyncConfig(
           pool_config={
               "project": "my-project-id",
               "instance_id": "my-instance",
               "database_id": "my-database",
               "min_sessions": 5,
               "max_sessions": 20
           }
       )
   )

   # Read-only snapshot (default)
   with sql.provide_session(db) as session:
       result = session.select(
           "SELECT * FROM users WHERE id = @user_id",
           {"user_id": 1}
       )

   # Write transaction
   with sql.provide_session(db, transaction=True) as session:
       session.execute(
           "UPDATE users SET active = TRUE WHERE id = @user_id",
           {"user_id": 1}
       )

**API Reference**:

.. autoclass:: SpannerSyncConfig
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: SpannerSyncDriver
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
- **NumPy VECTOR support** (Oracle 23ai+) - automatic conversion for AI/ML embeddings

**Known Limitations**:

- Separate configuration classes for sync and async
- Thick mode requires Oracle Instant Client installation
- VECTOR data type requires Oracle Database 23ai or higher

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

**Configuration (with NumPy VECTOR Support - Oracle 23ai+)**:

.. code-block:: python

   import numpy as np
   from sqlspec import SQLSpec
   from sqlspec.adapters.oracledb import OracleAsyncConfig

   sql = SQLSpec()
   db = sql.add_config(
       OracleAsyncConfig(
           pool_config={
               "user": "system",
               "password": "oracle",
               "dsn": "localhost:1521/FREEPDB1"
           },
           driver_features={
               "enable_numpy_vectors": True  # Enable automatic NumPy conversion
           }
       )
   )

   async with sql.provide_session(db) as session:
       # Create table with VECTOR column
       await session.execute("""
           CREATE TABLE embeddings (
               id NUMBER PRIMARY KEY,
               text VARCHAR2(4000),
               embedding VECTOR(768, FLOAT32)
           )
       """)

       # Insert NumPy array - automatically converted to Oracle VECTOR
       vector = np.random.rand(768).astype(np.float32)
       await session.execute(
           "INSERT INTO embeddings VALUES (:1, :2, :3)",
           (1, "sample text", vector)
       )

       # Retrieve - automatically converted back to NumPy array
       result = await session.select_one(
           "SELECT * FROM embeddings WHERE id = :1",
           (1,)
       )
       embedding = result["EMBEDDING"]
       assert isinstance(embedding, np.ndarray)
       assert embedding.dtype == np.float32

**NumPy VECTOR Support Details**:

Oracle Database 23ai introduces the VECTOR data type for AI/ML embeddings and similarity search. SQLSpec provides seamless NumPy integration for automatic bidirectional conversion.

**Supported NumPy dtypes**:

- ``float32`` → ``VECTOR(*, FLOAT32)`` - General embeddings (recommended)
- ``float64`` → ``VECTOR(*, FLOAT64)`` - High-precision embeddings
- ``int8`` → ``VECTOR(*, INT8)`` - Quantized embeddings
- ``uint8`` → ``VECTOR(*, BINARY)`` - Binary/hash vectors

**Requirements**:

- Oracle Database 23ai or higher
- NumPy installed (``pip install numpy``)
- ``enable_numpy_vectors=True`` in ``driver_features`` (opt-in)

**Manual Conversion API**:

For advanced use cases, use the type converter directly:

.. code-block:: python

   from sqlspec.adapters.oracledb.type_converter import OracleTypeConverter

   converter = OracleTypeConverter()

   # NumPy → Oracle VECTOR
   oracle_array = converter.convert_numpy_to_vector(numpy_array)

   # Oracle VECTOR → NumPy
   numpy_array = converter.convert_vector_to_numpy(oracle_array)

**Vector Similarity Search Example**:

.. code-block:: python

   query_vector = np.random.rand(768).astype(np.float32)

   results = await session.select_all("""
       SELECT id, text,
              VECTOR_DISTANCE(embedding, :1, COSINE) as distance
       FROM embeddings
       ORDER BY distance
       FETCH FIRST 5 ROWS ONLY
   """, (query_vector,))

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
    uv add sqlspec[adbc] adbc-driver-postgresql

    # SQLite backend
    uv add sqlspec[adbc] adbc-driver-sqlite

    # DuckDB backend
    uv add sqlspec[adbc] adbc-driver-duckdb

    # BigQuery backend
    uv add sqlspec[adbc] adbc-driver-bigquery

    # Snowflake backend
    uv add sqlspec[adbc] adbc-driver-snowflake

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

Type Handler Pattern
====================

SQLSpec supports optional type handlers for database-specific features that require external dependencies. Type handlers enable automatic type conversion at the database driver level.

Overview
--------

Type handlers differ from type converters:

**Type Converters** (``type_converter.py``):
  - Transform data after retrieval or before insertion
  - Pure Python transformations
  - Examples: JSON detection, datetime formatting

**Type Handlers** (``_<feature>_handlers.py``):
  - Register with database driver for automatic conversion
  - Require optional dependencies
  - Examples: pgvector, NumPy arrays

Graceful Degradation
--------------------

Type handlers gracefully degrade when optional dependencies are not installed:

- Detection via ``sqlspec._typing`` constants (``NUMPY_INSTALLED``, ``PGVECTOR_INSTALLED``)
- Auto-enable in ``driver_features`` when dependency available
- No errors if dependency missing - simply returns unconverted values

Optional Type Support Matrix
------------------------------

.. list-table::
   :header-rows: 1
   :widths: 20 20 30 30

   * - Adapter
     - Feature
     - Optional Dependency
     - Configuration
   * - oracledb
     - NumPy VECTOR
     - ``numpy``
     - ``driver_features["enable_numpy_vectors"]``
   * - asyncpg
     - pgvector
     - ``pgvector``
     - Auto-enabled (``driver_features["enable_pgvector"]``)
   * - psycopg
     - pgvector
     - ``pgvector``
     - Auto-enabled (``driver_features["enable_pgvector"]``)

Configuring Type Handlers
---------------------------

Type handlers are configured via the ``driver_features`` parameter:

**Automatic Detection** (Recommended):

.. code-block:: python

   # NumPy vectors - auto-enabled when numpy installed
   from sqlspec import SQLSpec
   from sqlspec.adapters.oracledb import OracleAsyncConfig

   sql = SQLSpec()
   db = sql.add_config(OracleAsyncConfig(
       pool_config={"dsn": "localhost:1521/FREEPDB1"}
       # enable_numpy_vectors automatically set to True if numpy installed
   ))

**Explicit Configuration**:

.. code-block:: python

   # Explicitly disable optional feature
   db = sql.add_config(OracleAsyncConfig(
       pool_config={"dsn": "localhost:1521/FREEPDB1"},
       driver_features={"enable_numpy_vectors": False}  # Force disable
   ))

**Check Feature Status**:

.. code-block:: python

   from sqlspec._typing import NUMPY_INSTALLED, PGVECTOR_INSTALLED

   print(f"NumPy available: {NUMPY_INSTALLED}")
   print(f"pgvector available: {PGVECTOR_INSTALLED}")

NumPy VECTOR Support (Oracle)
-------------------------------

Oracle Database 23ai introduces the VECTOR data type for AI/ML embeddings. SQLSpec provides seamless NumPy integration:

**Requirements**:
- Oracle Database 23ai or higher
- ``numpy`` package installed
- ``driver_features["enable_numpy_vectors"]=True`` (auto-enabled)

**Supported dtypes**:
- ``float32`` → ``VECTOR(*, FLOAT32)`` - General embeddings (recommended)
- ``float64`` → ``VECTOR(*, FLOAT64)`` - High-precision embeddings
- ``int8`` → ``VECTOR(*, INT8)`` - Quantized embeddings
- ``uint8`` → ``VECTOR(*, BINARY)`` - Binary/hash vectors

**Example Usage**:

See the Oracle adapter documentation above for complete examples.

pgvector Support (PostgreSQL)
-------------------------------

PostgreSQL's pgvector extension enables vector similarity search. SQLSpec automatically registers pgvector support when available:

**Requirements**:
- PostgreSQL with pgvector extension installed
- ``pgvector`` Python package installed
- Automatic registration (no configuration needed)
- Extension registration failures are downgraded to debug logs, so missing pgvector keeps the connection usable without vector support

**Adapters with pgvector support**:
- ``asyncpg`` - Async PostgreSQL driver
- ``psycopg`` - Sync and async PostgreSQL driver

**Example Usage**:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig

   sql = SQLSpec()
   db = sql.add_config(AsyncpgConfig(
       pool_config={"dsn": "postgresql://localhost/mydb"}
       # pgvector automatically registered if available
   ))

   async with sql.provide_session(db) as session:
       # Enable pgvector extension
       await session.execute("CREATE EXTENSION IF NOT EXISTS vector")

       # Create table with vector column
       await session.execute("""
           CREATE TABLE items (
               id SERIAL PRIMARY KEY,
               embedding vector(384)
           )
       """)

       # Insert vector (NumPy array or list)
       embedding = [0.1, 0.2, 0.3] * 128  # 384 dimensions
       await session.execute(
           "INSERT INTO items (embedding) VALUES ($1)",
           [embedding]
       )

       # Vector similarity search
       results = await session.select_all("""
           SELECT id, embedding <-> $1 as distance
           FROM items
           ORDER BY distance
           LIMIT 5
       """, [embedding])

Implementing Custom Type Handlers
-----------------------------------

To add type handlers for a new optional feature:

1. **Define detection constant** in ``sqlspec/_typing.py``:

   .. code-block:: python

      try:
          import optional_package
          OPTIONAL_PACKAGE_INSTALLED = True
      except ImportError:
          OPTIONAL_PACKAGE_INSTALLED = False

2. **Create handler module** (``adapters/<adapter>/_feature_handlers.py``):

   .. code-block:: python

      from sqlspec._typing import OPTIONAL_PACKAGE_INSTALLED

      def register_handlers(connection):
          if not OPTIONAL_PACKAGE_INSTALLED:
              logger.debug("Optional package not installed")
              return
          # Register handlers with connection

3. **Update config** to auto-detect and initialize:

   .. code-block:: python

      from sqlspec._typing import OPTIONAL_PACKAGE_INSTALLED

      class Config(AsyncDatabaseConfig):
          def __init__(self, *, driver_features=None, **kwargs):
              if driver_features is None:
                  driver_features = {}
              if "enable_feature" not in driver_features:
                  driver_features["enable_feature"] = OPTIONAL_PACKAGE_INSTALLED
              super().__init__(driver_features=driver_features, **kwargs)

          async def _init_connection(self, connection):
              if self.driver_features.get("enable_feature"):
                  from ._feature_handlers import register_handlers
                  register_handlers(connection)

4. **Add tests** (unit and integration):

   .. code-block:: python

      import pytest
      from sqlspec._typing import OPTIONAL_PACKAGE_INSTALLED

      @pytest.mark.skipif(not OPTIONAL_PACKAGE_INSTALLED, reason="Package not installed")
      async def test_feature_roundtrip(session):
          # Test with optional package
          pass

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
