==========
SQL Server
==========

This recipe collection demonstrates how to use Microsoft SQL Server with SQLSpec across common production patterns.
It covers driver selection, connection management, transactional workflows, and ecosystem integrations including Google ADK, durable event queues, and Litestar.

.. note::

   SQL Server connectivity requires either the ``sqlspec[mssql-python]`` or ``sqlspec[pymssql]`` installation extra.

Choosing a Driver
=================

SQLSpec offers three drivers for Microsoft SQL Server depending on workload requirements and platform dependencies:

.. list-table::
   :header-rows: 1
   :widths: 20 25 25 30

   * - Driver
     - Parameter style
     - Bulk load
     - Notes
   * - ``mssql-python``
     - qmark (``?``)
     - Native BulkCopy and Arrow
     - Microsoft's driver with BulkCopy and SQLSpec Arrow loading.
   * - ``pymssql``
     - qmark (``?``) / pyformat (``%(name)s``)
     - ``execute_many`` only
     - FreeTDS-based driver.
   * - ``arrow-odbc``
     - qmark (``?``)
     - Arrow-first ODBC streaming
     - Microsoft ODBC Driver 18; columnar data processing and analytics.

.. code-block:: python

   from sqlspec.adapters.mssql_python import MssqlPythonConfig
   from sqlspec.adapters.pymssql import PymssqlConfig

   mssql_config = MssqlPythonConfig(
       connection_config={
           "server": "localhost",
           "port": 1433,
           "database": "app_db",
           "user": "sa",
           "password": "SecretPassword123!",
       }
   )
   pymssql_config = PymssqlConfig(
       connection_config={
           "server": "localhost",
           "port": 1433,
           "database": "app_db",
           "user": "sa",
           "password": "SecretPassword123!",
       }
   )

Connecting and Pooling
======================

Configurations define connection parameters and built-in connection pool settings.
The ``provide_session()`` context manager acquires a driver instance from the pool, and ``close_pool()`` cleanly releases all pool resources.

.. code-block:: python

   from sqlspec.adapters.mssql_python import MssqlPythonConfig
   from sqlspec.adapters.pymssql import PymssqlConfig

   mssql_config = MssqlPythonConfig(
       connection_config={
           "server": "localhost",
           "port": 1433,
           "database": "app_db",
           "user": "sa",
           "password": "SecretPassword123!",
           "encrypt": False,
           "trust_server_certificate": True,
           "pool_size": 10,
           "pool_idle_timeout": 30,
           "pool_enabled": True,
       }
   )

   pymssql_config = PymssqlConfig(
       connection_config={
           "server": "localhost",
           "port": 1433,
           "database": "app_db",
           "user": "sa",
           "password": "SecretPassword123!",
           "charset": "utf8",
           "pool_recycle_seconds": 3600,
       }
   )

   with mssql_config.provide_session() as driver:
       rows = driver.select("SELECT 1 AS ready")

   mssql_config.close_pool()
   pymssql_config.close_pool()

Parameter Styles
================

The ``mssql-python`` adapter strictly uses positional question-mark (``?``) placeholders.
The ``pymssql`` adapter accepts question-mark (``?``) placeholders as well as named pyformat placeholders (``%(name)s``).
Positional and named parameter styles must never be mixed within the same query.

.. code-block:: python

   from sqlspec.adapters.mssql_python import MssqlPythonConfig
   from sqlspec.adapters.pymssql import PymssqlConfig

   mssql_config = MssqlPythonConfig(
       connection_config={
           "server": "localhost",
           "port": 1433,
           "database": "app_db",
           "user": "sa",
           "password": "SecretPassword123!",
       }
   )
   pymssql_config = PymssqlConfig(
       connection_config={
           "server": "localhost",
           "port": 1433,
           "database": "app_db",
           "user": "sa",
           "password": "SecretPassword123!",
       }
   )

   with mssql_config.provide_session() as driver:
       user = driver.select_one_or_none(
           "SELECT id, username, email FROM users WHERE id = ?",
           (42,),
       )

   with pymssql_config.provide_session() as driver:
       user_qmark = driver.select_one_or_none(
           "SELECT id, username, email FROM users WHERE id = ?",
           (42,),
       )

       user_named = driver.select_one_or_none(
           "SELECT id, username, email FROM users WHERE username = %(name)s",
           {"name": "ada"},
       )

Running Multi-Batch Scripts with GO
===================================

SQL Server scripts and DDL statements often use the ``GO`` batch separator.
The ``execute_script`` method splits input text on ``GO`` separators and runs each statement batch in sequence.

.. code-block:: python

   from sqlspec.adapters.mssql_python import MssqlPythonConfig

   config = MssqlPythonConfig(
       connection_config={
           "server": "localhost",
           "port": 1433,
           "database": "app_db",
           "user": "sa",
           "password": "SecretPassword123!",
       }
   )

   script = """
   CREATE TABLE dbo.audit_log (
       id INT IDENTITY(1,1) PRIMARY KEY,
       event_type NVARCHAR(50) NOT NULL,
       created_at DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME()
   )
   GO
   CREATE NONCLUSTERED INDEX ix_audit_log_event_type ON dbo.audit_log (event_type)
   GO
   """

   with config.provide_session() as driver:
       driver.execute_script(script)
       driver.commit()

Transactions and Savepoints
===========================

Transactions are managed using ``begin()``, ``commit()``, and ``rollback()``.
When a connection is initialized with ``autocommit=True``, committing or rolling back a transaction automatically restores the autocommit state.
Named savepoints are supported through ``create_savepoint()``, ``release_savepoint()``, and ``rollback_to_savepoint()``.

.. code-block:: python

   from sqlspec.adapters.mssql_python import MssqlPythonConfig

   config = MssqlPythonConfig(
       connection_config={
           "server": "localhost",
           "port": 1433,
           "database": "app_db",
           "user": "sa",
           "password": "SecretPassword123!",
           "autocommit": True,
       }
   )

   with config.provide_session() as driver:
       driver.begin()
       try:
           driver.execute(
               "INSERT INTO accounts (id, balance) VALUES (?, ?)",
               (1, 1000),
           )
           driver.create_savepoint("s1")
           driver.execute(
               "INSERT INTO audit_trail (account_id, action) VALUES (?, ?)",
               (1, "deposit_pending"),
           )
           driver.rollback_to_savepoint("s1")
           driver.commit()
       except Exception:
           driver.rollback()
           raise

Bulk Loading with BulkCopy and Arrow
====================================

The ``mssql-python`` adapter provides high-throughput bulk insertion via Microsoft BulkCopy and Apache Arrow streaming.
In contrast, ``pymssql`` does not support native BulkCopy and uses batched ``execute_many`` operations.

The three examples below use separate, pre-created tables with ``id``,
``event_type``, and ``created_at`` columns.

.. code-block:: python

   import pyarrow as pa
   from sqlspec.adapters.mssql_python import MssqlPythonConfig
   from sqlspec.adapters.pymssql import PymssqlConfig

   mssql_config = MssqlPythonConfig(
       connection_config={
           "server": "localhost",
           "port": 1433,
           "database": "app_db",
           "user": "sa",
           "password": "SecretPassword123!",
       }
   )
   pymssql_config = PymssqlConfig(
       connection_config={
           "server": "localhost",
           "port": 1433,
           "database": "app_db",
           "user": "sa",
           "password": "SecretPassword123!",
       }
   )

   rows = [
       (1, "login", "2026-01-01T00:00:00"),
       (2, "logout", "2026-01-01T00:01:00"),
   ]

   arrow_table = pa.Table.from_arrays(
       [
           pa.array([1, 2]),
           pa.array(["login", "logout"]),
           pa.array(["2026-01-01T00:00:00", "2026-01-01T00:01:00"]),
       ],
       names=["id", "event_type", "created_at"],
   )

   with mssql_config.provide_session() as driver:
       driver.bulk_copy(
           "dbo.events",
           rows,
           batch_size=10000,
           table_lock=True,
       )
       driver.commit()

   # Arrow loading is an alternative to bulk_copy; use a separate target.
   with mssql_config.provide_session() as driver:
       driver.load_from_arrow("dbo.events_arrow", arrow_table)
       driver.commit()

   with pymssql_config.provide_session() as driver:
       driver.execute_many(
           "INSERT INTO dbo.events_pymssql (id, event_type, created_at) VALUES (?, ?, ?)",
           rows,
       )
       driver.commit()

Migrations
==========

Database migrations are executed with ``SyncMigrationCommands``.
SQL Server resolves unqualified table references against the login user's default schema (typically ``dbo``), unless configured otherwise.

.. code-block:: python

   from sqlspec.adapters.mssql_python import MssqlPythonConfig
   from sqlspec.migrations.commands import SyncMigrationCommands

   config = MssqlPythonConfig(
       connection_config={
           "server": "localhost",
           "port": 1433,
           "database": "app_db",
           "user": "sa",
           "password": "SecretPassword123!",
       },
       migration_config={
           "script_location": "migrations",
           "version_table_name": "schema_migrations",
       },
   )

   commands = SyncMigrationCommands(config)
   commands.upgrade()

Google ADK Session and Memory Stores
====================================

SQL Server supports Google ADK session storage via ``MssqlPythonADKStore`` and ``PymssqlADKStore``, as well as memory storage via ``MssqlPythonADKMemoryStore`` and ``PymssqlADKMemoryStore``.
Setting ``native_json: False`` configures ``NVARCHAR(MAX)`` columns for JSON payloads.
Call ``ensure_tables()`` to provision tables. Existing migration-managed
installations should run the ADK extension upgrade: migration ``0002`` creates
the newly supported mssql-python memory table and indexes if missing. Downgrading
that repair preserves memory data; a full downgrade of ``0001`` removes it.
Because both SQL Server drivers are synchronous, asynchronous ADK runners should wrap store operations using ``anyio.to_thread.run_sync``.

.. code-block:: python

   import anyio
   from sqlspec.adapters.mssql_python import MssqlPythonConfig
   from sqlspec.adapters.mssql_python.adk import (
       MssqlPythonADKMemoryStore,
       MssqlPythonADKStore,
   )

   config = MssqlPythonConfig(
       connection_config={
           "server": "localhost",
           "port": 1433,
           "database": "app_db",
           "user": "sa",
           "password": "SecretPassword123!",
       },
       extension_config={"adk": {"native_json": False}},
   )

   session_store = MssqlPythonADKStore(config)
   memory_store = MssqlPythonADKMemoryStore(config)

   session_store.ensure_tables()
   memory_store.ensure_tables()


   async def run_agent() -> None:
       session = await anyio.to_thread.run_sync(
           session_store.get_session,
           "app",
           "user-123",
           "session-123",
       )

Durable Event Queue
===================

SQLSpec provides a transactional event queue backed by a SQL Server table.
The queue table is created by running migrations with ``"include_extensions": ["events"]``.
The channel publishes, consumes, and acknowledges events using the ``poll_queue`` strategy.

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.mssql_python import MssqlPythonConfig
   from sqlspec.migrations.commands import SyncMigrationCommands

   config = MssqlPythonConfig(
       connection_config={
           "server": "localhost",
           "port": 1433,
           "database": "app_db",
           "user": "sa",
           "password": "SecretPassword123!",
       },
       extension_config={"events": {"queue_table": "app_events"}},
       migration_config={
           "script_location": "migrations",
           "version_table_name": "schema_migrations",
           "include_extensions": ["events"],
       },
   )

   SyncMigrationCommands(config).upgrade()

   spec = SQLSpec()
   spec.add_config(config)
   channel = spec.event_channel(config)

   channel.publish("notifications", {"user_id": 42, "event": "order_placed"})

   event = channel.consume("notifications")
   if event is not None:
       channel.ack(event.id)

Litestar Plugin and Session Store
=================================

The Litestar integration enables dependency injection of ``MssqlPythonDriver`` or ``PymssqlDriver`` into route handlers via ``SQLSpecPlugin``.
Server-side session storage is provided by ``MssqlPythonStore`` and ``PymssqlStore`` for use with Litestar's ``SessionMiddleware``.

.. code-block:: python

   from litestar import Litestar, get
   from litestar.middleware.session.server_side import ServerSideSessionConfig
   from sqlspec import SQLSpec
   from sqlspec.adapters.mssql_python import MssqlPythonConfig, MssqlPythonDriver
   from sqlspec.adapters.mssql_python.litestar import MssqlPythonStore
   from sqlspec.extensions.litestar import SQLSpecPlugin

   config = MssqlPythonConfig(
       connection_config={
           "server": "localhost",
           "port": 1433,
           "database": "app_db",
           "user": "sa",
           "password": "SecretPassword123!",
       },
       extension_config={"litestar": {"session_table": "app_sessions"}},
   )

   spec = SQLSpec()
   spec.add_config(config)
   plugin = SQLSpecPlugin(sqlspec=spec)

   session_store = MssqlPythonStore(config)
   session_config = ServerSideSessionConfig()


   async def prepare_sessions() -> None:
       await session_store.create_table()


   @get("/users", sync_to_thread=True)
   def get_users(db_session: MssqlPythonDriver) -> list[dict[str, object]]:
       return db_session.select("SELECT id, username FROM users")


   app = Litestar(
       route_handlers=[get_users],
       plugins=[plugin],
       on_startup=[prepare_sessions],
       middleware=[session_config.middleware],
       stores={"sessions": session_store},
   )

See Also
========

* :doc:`dishka`
* :doc:`/reference/adapters/mssql_python`
* :doc:`/reference/adapters/pymssql`
* :doc:`/reference/adapters/arrow_odbc`
* :doc:`/usage/bulk_ingest`
* :doc:`/extensions/adk/index`
