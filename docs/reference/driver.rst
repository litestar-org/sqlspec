======
Driver
======

The driver module defines sync and async driver adapters, transaction helpers,
and the shared data dictionary mixins.

.. currentmodule:: sqlspec.driver

Example
=======

.. literalinclude:: /examples/reference/driver_api.py
   :language: python
   :caption: ``driver usage``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

<<<<<<< HEAD
Transaction Blocks
==================

``transaction()`` wraps a block in a transaction on the driver's connection.
Entering the block calls ``begin()`` and yields the same driver. A normal exit
commits; an exception rolls back and propagates to the caller. If the commit
itself fails, the block attempts a rollback and then raises the commit error.

The block calls the adapter's own ``begin()``, ``commit()``, and ``rollback()``,
so it follows each database's transaction model. When the connection already has
an open transaction, whether started by ``begin()`` or implicitly by an earlier
statement as SQLite does in its default mode or a connection with autocommit
disabled does, the block joins it instead of calling ``begin()``. Exiting the block
commits or rolls back that whole transaction, including work done before the block.

.. code-block:: python

    async with config.provide_session() as session:
        async with session.transaction():
            await session.execute("INSERT INTO users (name) VALUES (?)", "Ada")
            await session.execute("INSERT INTO audit (action) VALUES (?)", "user-created")

.. code-block:: python

    with config.provide_session() as session:
        with session.transaction():
            session.execute("INSERT INTO users (name) VALUES (?)", "Ada")
            session.execute("INSERT INTO audit (action) VALUES (?)", "user-created")

Nested blocks
-------------

A ``transaction()`` block entered inside another ``transaction()`` block on the
same driver, or inside a service's ``begin_transaction()`` block that uses the
driver, does not begin or commit. It runs in a savepoint instead: a normal exit
releases the savepoint, and an exception rolls back to it and propagates. The
enclosing block stays open and decides whether the work is committed. A service
``begin_transaction()`` block inside a ``transaction()`` block nests the same way.

.. code-block:: python

    from sqlspec.exceptions import UniqueViolationError

    with session.transaction():
        session.execute("INSERT INTO users (name) VALUES (?)", "Ada")
        try:
            with session.transaction():
                session.execute("INSERT INTO users (name) VALUES (?)", "Ada")
        except UniqueViolationError:
            pass

Nesting needs savepoint support. When the adapter cannot create a savepoint,
entering a nested block raises ``ImproperConfigurationError`` and the enclosing
block stays usable. DuckDB and ADBC connections to DuckDB, BigQuery, or Snowflake
report missing savepoint support. BigQuery has no transactions, so its
``begin()``, ``commit()``, and ``rollback()`` do nothing, and Spanner commits or
rolls back only sessions opened for writes; neither supports savepoints, so nested
blocks are not supported on either.

Isolation settings
------------------

``transaction()`` takes no isolation-level argument. Apply isolation or other
transaction settings with ``execute_script`` as the first statement inside the
block, using the syntax your database supports:

.. code-block:: python

    async with session.transaction():
        await session.execute_script("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        await session.execute("UPDATE accounts SET balance = balance - ? WHERE id = ?", 10, 1)

Driver Adapter Protocol and Base Classes
========================================

SQLSpec does not define standalone ``DriverProtocol``, ``AsyncDriverProtocol``, or
``SessionProtocol`` classes. Instead, database drivers and sessions are instances
of :class:`SyncDriverAdapterBase` or :class:`AsyncDriverAdapterBase`. The type alias
:data:`DriverAdapterProtocol` unifies synchronous and asynchronous driver adapters
for generic annotations.

.. autodata:: DriverAdapterProtocol

Synchronous Driver Adapter
--------------------------

.. autoclass:: SyncDriverAdapterBase
   :members:
   :undoc-members:
   :show-inheritance:

Asynchronous Driver Adapter
---------------------------

.. autoclass:: AsyncDriverAdapterBase
   :members:
   :undoc-members:
   :show-inheritance:

Connection Context and Session Factories
========================================

Context managers that manage pool connection and session lifecycles for driver adapters.

.. autoclass:: SyncPoolConnectionContext
   :members:
   :show-inheritance:

.. autoclass:: AsyncPoolConnectionContext
   :members:
   :show-inheritance:

.. autoclass:: SyncPoolSessionFactory
   :members:
   :show-inheritance:

.. autoclass:: AsyncPoolSessionFactory
   :members:
   :show-inheritance:

Row Streaming and Execution Results
===================================

.. autoclass:: SyncRowStream
   :members:
   :show-inheritance:

.. autoclass:: AsyncRowStream
   :members:
   :show-inheritance:

.. autoclass:: ExecutionResult
   :members:
   :show-inheritance:

Exception Handlers
==================

.. autoclass:: BaseSyncExceptionHandler
   :members:
   :show-inheritance:

.. autoclass:: BaseAsyncExceptionHandler
   :members:
   :show-inheritance:

Data Dictionary
===============

The shared data dictionary base classes define the metadata
contract used by adapter-local dictionaries. User-facing examples and the
support matrix live in :doc:`../usage/data_dictionary`. In short:

- Structural metadata returns ``MetadataResult`` envelopes.
- DDL lookups return ``DDLResult`` objects with fidelity and warning metadata.
- Dependency ordering uses typed dependency edges rather than only table names.
- System and performance metadata uses ``SystemMetadataRequest`` and
  ``SystemMetadataResult`` in a separate opt-in namespace.

.. autoclass:: DataDictionaryMixin
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: AsyncDataDictionaryBase
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: SyncDataDictionaryBase
   :members:
   :undoc-members:
   :show-inheritance:

Adapter Data Dictionary Classes
===============================

Adapter data dictionaries remain public at their adapter-local import paths, and
drivers continue to expose them through ``driver.data_dictionary``. Shared helper
modules under ``sqlspec.data_dictionary.dialects`` are internal implementation
details used to keep repeated dialect rules consistent. Performance builds
compile the shared helper modules, while adapter-local data-dictionary classes
stay in their driver packages because they still own real adapter overrides.

When changing data-dictionary behavior, review these shared-dialect groups
together:

- PostgreSQL: ``sqlspec.adapters.adbc.data_dictionary.AdbcDataDictionary`` when
  the driver dialect is Postgres, plus
  ``sqlspec.adapters.asyncpg.data_dictionary.AsyncpgDataDictionary``,
  ``sqlspec.adapters.psqlpy.data_dictionary.PsqlpyDataDictionary``,
  ``sqlspec.adapters.psycopg.data_dictionary.PsycopgSyncDataDictionary``, and
  ``sqlspec.adapters.psycopg.data_dictionary.PsycopgAsyncDataDictionary``.
- SQLite: ``sqlspec.adapters.sqlite.data_dictionary.SqliteDataDictionary``,
  ``sqlspec.adapters.aiosqlite.data_dictionary.AiosqliteDataDictionary``, and
  ``sqlspec.adapters.adbc.data_dictionary.AdbcDataDictionary`` when the driver
  dialect is SQLite.
- MySQL and MariaDB:
  ``sqlspec.adapters.mysqlconnector.data_dictionary.MysqlConnectorSyncDataDictionary``,
  ``sqlspec.adapters.mysqlconnector.data_dictionary.MysqlConnectorAsyncDataDictionary``,
  ``sqlspec.adapters.pymysql.data_dictionary.PyMysqlDataDictionary``,
  ``sqlspec.adapters.aiomysql.data_dictionary.AiomysqlDataDictionary``,
  ``sqlspec.adapters.asyncmy.data_dictionary.AsyncmyDataDictionary``, and
  ``sqlspec.adapters.adbc.data_dictionary.AdbcDataDictionary`` when the driver
  dialect is MySQL or MariaDB.
- CockroachDB:
  ``sqlspec.adapters.cockroach_asyncpg.data_dictionary.CockroachAsyncpgDataDictionary``,
  ``sqlspec.adapters.cockroach_psycopg.data_dictionary.CockroachPsycopgSyncDataDictionary``,
  ``sqlspec.adapters.cockroach_psycopg.data_dictionary.CockroachPsycopgAsyncDataDictionary``,
  and ``sqlspec.adapters.adbc.data_dictionary.AdbcDataDictionary`` when the
  driver dialect is CockroachDB.
- Oracle:
  ``sqlspec.adapters.oracledb.data_dictionary.OracledbSyncDataDictionary`` and
  ``sqlspec.adapters.oracledb.data_dictionary.OracledbAsyncDataDictionary``.
  Oracle ADK and event stores that construct these dictionaries directly should
  be reviewed with the same changes.
- BigQuery:
  ``sqlspec.adapters.bigquery.data_dictionary.BigQueryDataDictionary`` and
  ``sqlspec.adapters.adbc.data_dictionary.AdbcDataDictionary`` when the driver
  dialect is BigQuery.
- DuckDB and Spanner currently have native-only adapter dictionaries:
  ``sqlspec.adapters.duckdb.data_dictionary.DuckDBDataDictionary`` and
  ``sqlspec.adapters.spanner.data_dictionary.SpannerDataDictionary``.

Feature Flag Types
==================

.. currentmodule:: sqlspec.data_dictionary

.. autoclass:: FeatureFlags
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

.. autoclass:: FeatureVersions
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:
