============
mssql-python
============

``mssql_python`` is the default SQL Server adapter for SQLSpec. It provides sync
and async SQLSpec drivers over Microsoft's ``mssql-python`` package, including
T-SQL migrations, data dictionary support, Google ADK persistence, Litestar
session storage, native Arrow reads, and BulkCopy.

Install
=======

Install the optional dependency:

.. code-block:: bash

   uv add "sqlspec[mssql-python]"

Linux deployments also need the platform libraries required by
``mssql-python``. Common Debian/Ubuntu packages include ``libltdl7``,
``libkrb5-3``, and ``libgssapi-krb5-2``. The driver bundles Microsoft's DDBC
runtime, so a separate ODBC driver install is not required for the
``mssql_python`` adapter.

Quick Start: Sync
=================

.. code-block:: python

   from sqlspec.adapters.mssql_python import MssqlPythonConfig

   config = MssqlPythonConfig(
       connection_config={
           "server": "localhost,1433",
           "database": "app",
           "uid": "sa",
           "pwd": "change-me",
           "trust_server_certificate": True,
       }
   )

   with config.provide_session() as session:
       rows = session.select("SELECT id, name FROM users WHERE active = ?", (True,))

Quick Start: Async
==================

The async config wraps the same driver through ``asyncio.to_thread``. Use it in
async applications when you need SQLSpec's async driver contract.

.. code-block:: python

   from sqlspec.adapters.mssql_python import MssqlPythonAsyncConfig

   config = MssqlPythonAsyncConfig(
       connection_config={
           "server": "localhost,1433",
           "database": "app",
           "uid": "sa",
           "pwd": "change-me",
           "trust_server_certificate": True,
       }
   )

   async with config.provide_session() as session:
       user = await session.select_one_or_none("SELECT id, name FROM users WHERE id = ?", (1,))

Connection Strings
==================

You can pass a complete connection string:

.. code-block:: python

   config = MssqlPythonConfig(
       connection_config={
           "connection_string": (
               "Server=tcp:example.database.windows.net,1433;"
               "Database=app;"
               "UID=app_user;"
               "PWD=secret;"
               "Encrypt=yes;"
               "TrustServerCertificate=no;"
           )
       }
   )

Or let SQLSpec build it from fields:

.. code-block:: python

   config = MssqlPythonConfig(
       connection_config={
           "server": "tcp:example.database.windows.net",
           "port": 1433,
           "database": "app",
           "authentication": "ActiveDirectoryDefault",
           "encrypt": True,
           "trust_server_certificate": False,
       }
   )

Common authentication values include ``ActiveDirectoryDefault``,
``ActiveDirectoryInteractive``, ``ActiveDirectoryServicePrincipal``, and
``ActiveDirectoryMSI``. Azure SQL deployments should use encrypted connections
with ``TrustServerCertificate=no`` outside local development.

Parameter Styles
================

The adapter registers the ``mssql_python`` parameter profile:

- qmark parameters: ``WHERE id = ?``
- pyformat parameters: ``WHERE id = %(id)s``

Do not mix parameter styles in one statement. Qmark parameters are the simplest
choice for new code:

.. code-block:: python

   row = session.select_one_or_none("SELECT id FROM users WHERE email = ?", ("a@example.com",))

Pooling
=======

SQLSpec exposes ``mssql-python`` driver-level pooling through the config. The
default pool size is ``100`` and the default idle timeout is ``600`` seconds.

.. code-block:: python

   config = MssqlPythonConfig(
       connection_config={
           "server": "localhost,1433",
           "database": "app",
           "uid": "sa",
           "pwd": "change-me",
           "pool_size": 25,
           "pool_idle_timeout": 300,
       },
       driver_features={"use_pool": True},
   )

Disable driver-level pooling with ``driver_features={"use_pool": False}`` or
``connection_config={"pool_enabled": False}``.

Transactions and Savepoints
===========================

Use the SQLSpec driver transaction methods:

.. code-block:: python

   with config.provide_session() as session:
       session.begin()
       try:
           session.execute("UPDATE accounts SET balance = balance - ? WHERE id = ?", (100, 1))
           session.execute("UPDATE accounts SET balance = balance + ? WHERE id = ?", (100, 2))
           session.commit()
       except Exception:
           session.rollback()
           raise

Savepoints use SQL Server's ``SAVE TRANSACTION`` and ``ROLLBACK TRANSACTION``
statements internally.

Type Handling
=============

Prefer SQL Server-native types for new schema:

- ``UNIQUEIDENTIFIER`` for ``uuid.UUID`` values.
- ``DATETIME2`` for naive UTC timestamps.
- ``DATETIMEOFFSET`` when an offset must be preserved.
- ``NVARCHAR(MAX)`` for JSON on SQL Server 2022 and earlier.
- Native ``JSON`` for SQL Server versions that expose it.
- ``VARBINARY(MAX)`` for arbitrary bytes.

The agent-facing chapter ``.agents/knowledge/mssql_type_handling.md`` contains
the full type matrix and operational gotchas.

Arrow Fetch
===========

``mssql-python`` exposes native Arrow reads through cursor methods. SQLSpec uses
the shared ``select_to_arrow`` API for table, reader, batch, and batches return
formats:

.. code-block:: python

   result = session.select_to_arrow("SELECT id, created_at FROM audit_log")
   table = result.get_data()

   result = session.select_to_arrow("SELECT * FROM audit_log", return_format="batches", batch_size=64_000)
   for batch in result.get_data():
       process(batch)

BulkCopy
========

Use ``bulk_copy`` for high-volume inserts:

.. code-block:: python

   rows = [(1, "Ada"), (2, "Grace")]
   inserted = session.bulk_copy(
       "dbo.users",
       rows,
       batch_size=64_000,
       table_lock=True,
       keep_identity=False,
   )

``bulk_copy`` forwards the SQL Server options exposed by ``mssql-python``:
``column_mappings``, ``keep_identity``, ``check_constraints``, ``table_lock``,
``keep_nulls``, ``fire_triggers``, and ``use_internal_transaction``.

Migrations
==========

``MssqlPythonConfig`` and ``MssqlPythonAsyncConfig`` use T-SQL migration
trackers. The migration table is created with SQL Server idempotent DDL and
``DATETIME2(6)`` timestamp columns.

.. code-block:: python

   config.migrate_up()

Google ADK
==========

The adapter includes ADK session and event queue stores:

.. code-block:: python

   from sqlspec.adapters.mssql_python.adk import MssqlPythonAsyncADKStore

   store = MssqlPythonAsyncADKStore(config)
   await store.create_session("session", "app", "user", {"theme": "dark"})

Litestar
========

Use the generic SQLSpec plugin for dependency injection and
``MssqlPythonStore`` for Litestar server-side sessions:

.. code-block:: python

   from litestar import Litestar
   from litestar.middleware.session.server_side import ServerSideSessionConfig

   from sqlspec import SQLSpec
   from sqlspec.adapters.mssql_python import MssqlPythonAsyncConfig
   from sqlspec.adapters.mssql_python.litestar import MssqlPythonStore
   from sqlspec.extensions.litestar import SQLSpecPlugin

   config = MssqlPythonAsyncConfig(connection_config={"connection_string": "..."})
   sqlspec = SQLSpec()
   sqlspec.add_config(config)

   session_store = MssqlPythonStore(config)
   session_config = ServerSideSessionConfig(store="sessions")

   app = Litestar(
       route_handlers=[],
       plugins=[SQLSpecPlugin(sqlspec=sqlspec)],
       middleware=[session_config.middleware],
       stores={"sessions": session_store},
       on_startup=[session_store.create_table],
   )

Compatibility
=============

The adapter targets Python 3.10+, SQL Server 2016+, Azure SQL Database, Azure
SQL Managed Instance, and SQL database in Fabric. ``mssql-python`` publishes
platform wheels for common Linux, macOS, and Windows targets; verify your target
architecture before deploying to less common platforms.
