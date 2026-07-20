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

Session Expiry
--------------

Configure session lifetime through Litestar's ``SessionMiddleware`` settings. Expired
sessions are cleaned up automatically based on the ``max_age`` parameter.
