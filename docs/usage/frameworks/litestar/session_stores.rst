===============
Session Stores
===============

SQLSpec provides session store backends for Litestar's session middleware. Store user
sessions in your database instead of cookies or external caches.

Why SQL-backed Sessions?
------------------------

- **No external dependencies** - use your existing database instead of Redis or Memcached.
- **Persistence** - sessions survive server restarts.
- **Querying** - inspect or clean up sessions with standard SQL.

Basic Setup
-----------

Pass a SQLSpec store to Litestar's ``SessionMiddleware``:

.. literalinclude:: /examples/frameworks/litestar/session_stores.py
   :language: python
   :caption: ``session store``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Available Stores
----------------

SQLSpec provides stores for async adapters:

- ``AsyncpgStore`` - PostgreSQL via asyncpg
- ``AiosqliteStore`` - SQLite via aiosqlite
- ``ArrowOdbcStore`` - SQL Server via arrow-odbc and Microsoft ODBC Driver 18

Each store can create its session table. It can also add new columns from its
own DDL. Set this behavior under ``extension_config["litestar"]``:

.. code-block:: python

   config = AsyncpgConfig(
       connection_config={"dsn": "postgresql://localhost/app"},
       extension_config={
           "litestar": {
               "manage_schema": True,
               "create_schema": True,
           }
       },
   )

Set ``manage_schema=False`` when another tool owns the table. Set
``create_schema=False`` to add columns without creating a missing table. Use a
versioned migration for renames, drops, or type changes.

Database-specific storage options
---------------------------------

Storage tuning belongs in the same ``extension_config["litestar"]`` mapping as
the schema settings. SQLSpec validates that mapping for the selected adapter.
Unknown keys and options from another database family raise
``ImproperConfigurationError`` instead of being silently ignored.

The available options are:

* PostgreSQL (``asyncpg``, ``psycopg``, and ``psqlpy``): ``fillfactor``,
  ``autovacuum_vacuum_scale_factor``, and
  ``autovacuum_analyze_scale_factor``. The established session-table
  ``fillfactor=80`` default is retained.
* CockroachDB: ``enable_hash_sharded_indexes``,
  ``hash_shard_bucket_count``, and
  ``ttl_expiration_expression="expires_at"``.
* BigQuery: ``partitioning``, ``partition_expiration_days``, and
  ``require_partition_filter``. The partition column is ``expires_at``.
* SQLite and AioSQLite: opt-in ``pragma_profile`` and ``pragma_overrides``.
  PRAGMAs are applied during schema preparation, not for each session-store
  operation.
* MySQL and MariaDB: ``table_options`` and ``index_options`` for reviewed
  backend-specific DDL clauses.
* Spanner: ``shard_count``, ``table_options``, and ``index_options``.
* Oracle Database: ``compression``, ``partitioning``, ``in_memory``, and
  ``table_options``. See :ref:`oracledb-extension-storage-options` for the
  capability and licensing behavior.

All newly supported options are opt-in except PostgreSQL's existing
``fillfactor`` default, so upgrading does not otherwise change session DDL.

For example, enable the SQLite profile and override only the values needed by
the application:

.. code-block:: python

   from sqlspec.adapters.sqlite import SqliteConfig

   config = SqliteConfig(
       connection_config={"database": "sessions.db"},
       extension_config={
           "litestar": {
               "pragma_profile": True,
               "pragma_overrides": {"busy_timeout": 10_000},
           }
       },
   )

Session Expiry
--------------

Configure session lifetime through Litestar's ``SessionMiddleware`` settings. Expired
sessions are cleaned up automatically based on the ``max_age`` parameter.
