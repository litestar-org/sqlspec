======================
Dependency Injection
======================

The SQLSpec plugin integrates with Litestar's dependency injection system. By default,
it provides a session under the key ``db_session``. You can customize this key or register
multiple databases with distinct keys.

Default Injection
-----------------

When you add ``SQLSpecPlugin`` to your app, handlers can request ``db_session`` to receive
a session scoped to the request:

.. code-block:: python

   from sqlspec.adapters.aiosqlite import AiosqliteDriver

   @get("/users")
   async def list_users(db_session: AiosqliteDriver) -> list[User]:
       result = await db_session.execute("SELECT * FROM users")
       return result.all(schema_type=User)

.. note::

   Use the driver type that matches your config. For example, ``SqliteDriver`` for
   ``SqliteConfig``, ``AiosqliteDriver`` for ``AiosqliteConfig``, or ``AsyncpgDriver``
   for ``AsyncpgConfig``.

Custom Keys
-----------

Use ``extension_config`` to customize the dependency injection keys for each database.
Set ``session_key``, ``connection_key``, and ``pool_key`` to unique values when using multiple databases.

.. literalinclude:: /examples/extensions/litestar/dependency_keys.py
   :language: python
   :caption: ``dependency keys``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Multiple Databases
------------------

Configure each database with unique keys in its ``extension_config``, then use a single plugin.
You can mix async and sync adapters - for example, an async PostgreSQL primary database with a
sync DuckDB for ETL operations:

.. code-block:: python

   from litestar import Litestar, get
   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
   from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
   from sqlspec.extensions.litestar import SQLSpecPlugin

   sqlspec = SQLSpec()

   # Primary async PostgreSQL database
   sqlspec.add_config(
       AsyncpgConfig(
           connection_config={
               "host": "localhost",
               "port": 5432,
               "database": "app",
               "user": "app",
               "password": "secret",
           },
           extension_config={"litestar": {"session_key": "db"}}
       )
   )

   # ETL sync DuckDB database with custom keys
   sqlspec.add_config(
       DuckDBConfig(
           connection_config={"database": "/tmp/etl.db"},
           extension_config={
               "litestar": {
                   "session_key": "etl_db",
                   "connection_key": "etl_connection",
                   "pool_key": "etl_pool",
               }
           }
       )
   )

   @get("/report")
   async def report(db: AsyncpgDriver, etl_db: DuckDBDriver) -> dict:
       # Async query to primary PostgreSQL
       users = await db.select("SELECT * FROM users")
       # Sync query to DuckDB ETL database
       metrics = etl_db.select("SELECT * FROM analytics")
       return {"users": users.all(), "metrics": metrics.all()}

   app = Litestar(
       route_handlers=[report],
       plugins=[SQLSpecPlugin(sqlspec=sqlspec)]  # Single plugin handles all configs
   )

Advanced DuckDB Configuration
-----------------------------

DuckDB supports extensions and connection hooks for advanced use cases like attaching
external PostgreSQL databases. Use ``driver_features`` to configure extensions and
``on_connection_create`` for custom connection initialization:

.. code-block:: python

   from typing import Any
   from sqlspec import SQLSpec
   from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBExtensionConfig

   def on_connection_create(connection: Any) -> None:
       """Configure DuckDB connection with PostgreSQL attachment."""
       # Load postgres extension and attach external database
       connection.execute("LOAD postgres")
       connection.execute(
           "ATTACH 'dbname=app user=app password=secret host=localhost' "
           "AS pg (TYPE POSTGRES, SCHEMA 'public')"
       )

   sqlspec = SQLSpec()
   sqlspec.add_config(
       DuckDBConfig(
           connection_config={
               "database": "/tmp/analytics.db",
               "temp_directory": "/tmp",
           },
           driver_features={
               "extensions": [
                   DuckDBExtensionConfig(name="postgres"),
                   DuckDBExtensionConfig(name="encodings"),
               ],
               "on_connection_create": on_connection_create,
           },
           extension_config={
               "litestar": {
                   "session_key": "etl_db",
                   "connection_key": "etl_connection",
               }
           }
       )
   )

This pattern enables querying PostgreSQL tables directly from DuckDB SQL:

.. code-block:: python

   @get("/sync-users")
   def sync_users(etl_db: DuckDBDriver) -> dict:
       # Query PostgreSQL via DuckDB's postgres extension
       result = etl_db.execute(
           "INSERT INTO local_users SELECT * FROM pg.users RETURNING *"
       )
       return {"synced": result.rowcount}
