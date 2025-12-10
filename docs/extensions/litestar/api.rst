=============
API Reference
=============

Complete API reference for the SQLSpec Litestar extension.

SQLSpecPlugin
=============

.. autoclass:: sqlspec.extensions.litestar.SQLSpecPlugin
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

Configuration
=============

Litestar Configuration
----------------------

Configure the plugin via ``extension_config`` in database configuration:

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig

   config = AsyncpgConfig(
       connection_config={"dsn": "postgresql://localhost/mydb"},
       extension_config={
           "litestar": {
               "connection_key": "db_connection",
               "pool_key": "db_pool",
               "session_key": "db_session",
               "commit_mode": "autocommit",
               "extra_commit_statuses": {201, 204},
               "extra_rollback_statuses": {409},
               "enable_correlation_middleware": True,
               "correlation_header": "x-correlation-id",
           }
       }
   )

Configuration Options
---------------------

.. list-table::
   :header-rows: 1
   :widths: 25 15 15 45

   * - Option
     - Type
     - Default
     - Description
   * - ``connection_key``
     - ``str``
     - ``"db_connection"``
     - Dependency injection key for connections
   * - ``pool_key``
     - ``str``
     - ``"db_pool"``
     - Dependency injection key for connection pool
   * - ``session_key``
     - ``str``
     - ``"db_session"``
     - Dependency injection key for driver sessions
   * - ``commit_mode``
     - ``str``
     - ``"manual"``
     - Transaction handling mode: ``"manual"``, ``"autocommit"``, ``"autocommit_include_redirect"``
   * - ``extra_commit_statuses``
     - ``set[int]``
     - ``None``
     - Additional HTTP status codes that trigger commits
   * - ``extra_rollback_statuses``
     - ``set[int]``
     - ``None``
     - Additional HTTP status codes that trigger rollbacks
  * - ``enable_correlation_middleware``
     - ``bool``
     - ``True``
     - Enable request correlation tracking
  * - ``correlation_header``
     - ``str``
     - ``"X-Request-ID"``
     - HTTP header to read when populating the correlation ID middleware
   * - ``correlation_headers``
     - ``list[str]``
     - ``[]``
     - Additional headers to consider (auto-detected headers are appended unless disabled)
   * - ``auto_trace_headers``
     - ``bool``
     - ``True``
     - Toggle automatic detection of standard tracing headers (`Traceparent`, `X-Cloud-Trace-Context`, etc.)

Session Stores
==============

AsyncpgStore
------------

.. autoclass:: sqlspec.adapters.asyncpg.litestar.AsyncpgStore
   :members:
   :undoc-members:
   :show-inheritance:

AiosqliteStore
--------------

.. autoclass:: sqlspec.adapters.aiosqlite.litestar.AiosqliteStore
   :members:
   :undoc-members:
   :show-inheritance:

OracledbStore
-------------

.. autoclass:: sqlspec.adapters.oracledb.litestar.OracledbStore
   :members:
   :undoc-members:
   :show-inheritance:

Commit Modes
============

Manual Mode
-----------

Explicit transaction control:

- No automatic commits or rollbacks
- Use ``async with session.begin_transaction()``
- Full control over transaction boundaries

Autocommit Mode
---------------

Automatic commit based on HTTP status:

**Commits on**:

- HTTP 200-299 (success)
- Any status in ``extra_commit_statuses``

**Rolls back on**:

- HTTP 300+ (redirects and errors)
- Any status in ``extra_rollback_statuses``
- Exceptions during request handling

Autocommit with Redirects
--------------------------

Commits on success and redirects:

**Commits on**:

- HTTP 200-399 (success + redirects)
- Any status in ``extra_commit_statuses``

**Rolls back on**:

- HTTP 400+ (errors)
- Any status in ``extra_rollback_statuses``
- Exceptions during request handling

Type Aliases
============

Common type annotations for dependency injection:

.. code-block:: python

   from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

   # Async drivers
   AsyncDriverAdapterBase  # Base class for all async drivers

   # Sync drivers
   SyncDriverAdapterBase   # Base class for all sync drivers

   # Specific driver types
   from sqlspec.adapters.asyncpg import AsyncpgDriver
   from sqlspec.adapters.aiosqlite import AiosqliteDriver
   from sqlspec.adapters.sqlite import SqliteDriver

Usage Examples
==============

Basic Plugin Setup
------------------

.. code-block:: python

   from litestar import Litestar
   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.extensions.litestar import SQLSpecPlugin

   spec = SQLSpec()
   db = spec.add_config(
       AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/mydb"})
   )

   plugin = SQLSpecPlugin(sqlspec=spec)
   app = Litestar(route_handlers=[...], plugins=[plugin])

Multi-Database Setup
--------------------

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.duckdb import DuckDBConfig
   from sqlspec.extensions.litestar import SQLSpecPlugin
   from litestar import Litestar

   spec = SQLSpec()

   primary = spec.add_config(
       AsyncpgConfig(
           connection_config={"dsn": "postgresql://localhost/primary"},
           extension_config={
               "litestar": {"session_key": "primary_session"}
           }
       )
   )

   analytics = spec.add_config(
       DuckDBConfig(
           extension_config={
               "litestar": {"session_key": "analytics_session"}
           }
       )
   )

   plugin = SQLSpecPlugin(sqlspec=spec)
   app = Litestar(route_handlers=[...], plugins=[plugin])

Session Store Setup
-------------------

.. code-block:: python

   from litestar import Litestar
   from litestar.middleware.session.server_side import ServerSideSessionConfig
   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.asyncpg.litestar import AsyncpgStore
   from sqlspec.extensions.litestar import SQLSpecPlugin

   # Create SQLSpec instance
   spec = SQLSpec()

   # Add database configuration
   config = spec.add_config(
       AsyncpgConfig(
           connection_config={"dsn": "postgresql://localhost/mydb"},
           extension_config={"litestar": {"session_table": "litestar_sessions"}},
       )
   )

   # Create session store
   store = AsyncpgStore(config)

   # Configure Litestar application
   app = Litestar(
       plugins=[SQLSpecPlugin(sqlspec=spec)],
       middleware=[
           ServerSideSessionConfig(store=store).middleware
       ]
   )

See Also
========

- :doc:`quickstart` - Get started guide
- :doc:`dependency_injection` - Dependency injection details
- :doc:`transactions` - Transaction management
- :doc:`session_stores` - Session storage
- :doc:`/reference/extensions` - Extensions reference
