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

Transaction Blocks
==================

``transaction()`` wraps a block in a transaction on the driver's connection.
Entering the block calls ``begin()`` and yields the same driver. A normal exit
commits; an exception rolls back and propagates to the caller. The block works
the same way on every adapter.

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

``transaction()`` takes no isolation-level argument. Apply isolation or other
transaction settings with ``execute_script`` as the first statement inside the
block, using the syntax your database supports:

.. code-block:: python

    async with session.transaction():
        await session.execute_script("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        await session.execute("UPDATE accounts SET balance = balance - ? WHERE id = ?", 10, 1)

Base Driver Classes
===================

Synchronous Driver
------------------

.. autoclass:: SyncDriverAdapterBase
   :members:
   :undoc-members:
   :show-inheritance:

Asynchronous Driver
-------------------

.. autoclass:: AsyncDriverAdapterBase
   :members:
   :undoc-members:
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
