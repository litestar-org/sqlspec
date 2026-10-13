========
Adapters
========

ADK stores use the same adapters as the rest of SQLSpec. Configure your database
with a standard config class, then pass it to the ADK store.

Choosing an Adapter
===================

Use async adapters for best performance with ADK runners:

- **PostgreSQL** (recommended): ``asyncpg``, ``psycopg`` (async mode), ``psqlpy``
- **CockroachDB**: ``cockroach_asyncpg``, ``cockroach_psycopg`` (full FTS support)
- **MySQL/MariaDB**: ``aiomysql``, ``asyncmy``
- **SQLite**: ``aiosqlite`` (development and single-process)
- **Oracle**: ``oracledb``
- **IBM Db2**: ``db2`` (native ``ibm_db`` sync and async modes)
- **DuckDB**: ``duckdb`` (analytics; reduced-scope for ADK)
- **ADBC**: ``adbc`` (Arrow-native portability across PostgreSQL, SQLite, DuckDB, BigQuery, Snowflake, Oracle, IBM Db2, and SQL Server)
- **Spanner**: ``spanner`` (Google Cloud, globally distributed; supports both async and sync modes)
- **SQL Server**: ``mssql_python`` (native Arrow and BulkCopy), ``pymssql`` (FreeTDS), ``arrow_odbc`` (Arrow over Microsoft ODBC Driver 18)

Sync adapters (``psycopg`` sync mode, ``spanner`` sync mode, ``db2`` sync mode, ``sqlite``, ``mysqlconnector``,
``pymysql``, ``mssql_python``, ``pymssql``, ``arrow_odbc``, ``adbc``) work with ADK and use ``anyio`` worker-thread dispatch for async
ADK runners.

Each Adapter Provides
=====================

Every adapter with ADK support ships session/event and memory stores:

- **Session store** (e.g., ``AsyncpgADKStore``) -- sessions and events.
- **Memory store** (e.g., ``AsyncpgADKMemoryStore``) -- long-term memory with FTS.

Import from the adapter's ``adk`` subpackage:

.. code-block:: python

   from sqlspec.adapters.asyncpg.adk import (
       AsyncpgADKStore,
       AsyncpgADKMemoryStore,
   )

Artifact service base classes live under ``sqlspec.extensions.adk.artifact``.
Google Cloud Spanner also provides concrete artifact metadata stores
(:class:`~sqlspec.adapters.spanner.adk.SpannerSyncADKArtifactStore` and
:class:`~sqlspec.adapters.spanner.adk.SpannerAsyncADKArtifactStore`) in
``sqlspec.adapters.spanner.adk``.

Example
=======

.. literalinclude:: /examples/extensions/adk/backend_config.py
   :language: python
   :caption: ``adk backend config``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

See Also
========

- :doc:`backends` for the full support matrix and backend-specific notes.
- :doc:`/usage/drivers_and_querying` for adapter configuration patterns.
- :doc:`/reference/adapters/index` for the complete adapter API.
