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

Each store automatically creates its session table on first use if it doesn't exist.

Session Expiry
--------------

Configure session lifetime through Litestar's ``SessionMiddleware`` settings. Expired
sessions are cleaned up automatically based on the ``max_age`` parameter.
