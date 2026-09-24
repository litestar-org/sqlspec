===
Db2
===

Sync and async IBM Db2 adapter built on `ibm_db <https://pypi.org/project/ibm_db/>`_.
It binds positional parameters (``?``), pools connections, reflects the Db2
catalog, and compiles SQL through SQLSpec's built-in ``db2`` SQLGlot dialect.

Supported Databases
===================

* **Db2 for Linux, UNIX and Windows (LUW) 11.5 and later.** Catalog queries read the
  ``SYSCAT`` views, and feature detection assumes the 11.5 SQL level.
* **Db2 for z/OS and Db2 for IBM i are not supported.** They expose different
  catalogs and SQL, and SQLSpec does not test against them.

Installation
============

.. code-block:: bash

   pip install "sqlspec[db2]"

The extra installs ``ibm_db`` 3.3.0 or later, which bundles the IBM CLI driver
(``clidriver``). Wheels are published for CPython 3.9 through 3.14. Linux wheels
are ``manylinux_2_34`` x86_64 because the bundled CLI driver needs glibc 2.34 or
newer. On other platforms ``pip`` builds ``ibm_db`` from source: set
``CLIDRIVER_VERSION=v11.5.9`` to download a compatible CLI driver, or point
``IBM_DB_HOME`` at an existing Db2 client installation.

Quick Start
===========

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.db2 import Db2SyncConfig

   spec = SQLSpec()
   db = spec.add_config(
       Db2SyncConfig(
           connection_config={
               "database": "SAMPLE",
               "hostname": "db2.example.com",
               "port": 50000,
               "user": "db2inst1",
               "password": "secret",
           }
       )
   )

   with spec.provide_session(db) as session:
       rows = session.select("SELECT id, name FROM users WHERE active = ?", 1)

Use ``Db2AsyncConfig`` with ``async with`` sessions for asyncio applications. It
runs on ``ibm_db_dbi.AsyncConnection`` and accepts the same connection parameters
plus the async pool settings below.

.. code-block:: python

   from sqlspec.adapters.db2 import Db2AsyncConfig

   config = Db2AsyncConfig(
       connection_config={
           "database": "SAMPLE",
           "hostname": "db2.example.com",
           "user": "db2inst1",
           "password": "secret",
           "max_size": 10,
           "acquire_timeout": 30.0,
       }
   )


   async def active_users() -> list[dict]:
       async with config.provide_session() as session:
           return await session.select("SELECT id, name FROM users WHERE active = ?", 1)

Connection Parameters
=====================

Each parameter renders as one IBM CLI connection keyword. Unknown keys raise
``ImproperConfigurationError`` when the config is created.

.. list-table::
   :header-rows: 1

   * - Parameter
     - CLI keyword
     - Notes
   * - ``database``
     - ``DATABASE``
     - Required, directly or through ``dsn``.
   * - ``hostname``
     - ``HOSTNAME``
     - Omit it to connect to a cataloged database alias.
   * - ``port``
     - ``PORT``
     - Defaults to ``50000`` when ``hostname`` is set.
   * - ``protocol``
     - ``PROTOCOL``
     - Defaults to ``TCPIP`` when ``hostname`` is set.
   * - ``user`` / ``password``
     - ``UID`` / ``PWD``
     -
   * - ``current_schema``
     - ``CURRENTSCHEMA``
     - Default schema for unqualified names.
   * - ``security``
     - ``SECURITY``
     - ``"SSL"`` enables TLS.
   * - ``ssl_server_certificate``
     - ``SSLSERVERCERTIFICATE``
     - Path to the server certificate (ARM or PEM file).
   * - ``authentication``
     - ``AUTHENTICATION``
     - For example ``"SERVER_ENCRYPT"``.
   * - ``connect_timeout``
     - ``CONNECTTIMEOUT``
     - Seconds.
   * - ``autocommit``
     - (none)
     - Autocommit mode new connections open in. Defaults to ``True``.
   * - ``dsn``
     - (parsed)
     - ``KEY=VALUE;...`` string or ``db2://user:password@host:port/database?Key=Value``
       URL. Explicit parameters override values parsed from it.
   * - ``extra``
     - (verbatim)
     - Additional CLI keywords, for example ``{"ClientApplName": "billing"}``.

Values that contain ``;`` or ``{``, or that start or end with whitespace, are
wrapped in braces when the connection string is rendered, so passwords such as
``"pa;ss"`` work as written. CLI connection strings cannot represent ``}``;
a value containing it raises ``ImproperConfigurationError``.

TLS example:

.. code-block:: python

   from sqlspec.adapters.db2 import Db2SyncConfig

   config = Db2SyncConfig(
       connection_config={
           "database": "SAMPLE",
           "hostname": "db2.example.com",
           "port": 50001,
           "user": "db2inst1",
           "password": "secret",
           "security": "SSL",
           "ssl_server_certificate": "/etc/db2/server.arm",
           "current_schema": "APP",
       }
   )

``config.get_connection_string()`` returns the rendered CLI connection string.

Connection Pooling
==================

``Db2SyncConfig`` keeps **one connection per thread** that uses the config,
including worker threads that run ``async_()`` wrappers. The pool has no maximum
size: the number of open connections equals the number of threads that have used
it. Connections are replaced after ``pool_recycle_seconds`` (default 86400) and
pinged with ``SELECT 1 FROM SYSIBM.SYSDUMMY1`` before reuse once idle for
``health_check_interval`` seconds (default 30).

``Db2AsyncConfig`` holds **at most** ``max_size`` connections (default 10). A
session that cannot get a connection within ``acquire_timeout`` seconds (default
30) raises ``ConnectionTimeoutError``. Recycling and health checks use the same
two settings as the sync pool.

Transactions
============

Connections open in **autocommit mode** by default, so each statement outside a
transaction commits immediately.

* ``session.begin()`` turns autocommit off for the unit of work; ``commit()`` and
  ``rollback()`` end it and turn autocommit back on.
* ``session.transaction()`` wraps a block in ``begin()``/``commit()`` and rolls
  back when the block raises. Nested blocks use savepoints.
* ``provide_session(transaction=True)`` starts the session inside a transaction.
* A session that exits with an unfinished transaction rolls it back and restores
  the connection's autocommit mode before the connection returns to the pool.

.. code-block:: python

   with config.provide_session() as session:
       with session.transaction():
           session.execute("INSERT INTO users (id, name) VALUES (?, ?)", 101, "Ada")
           session.execute("UPDATE accounts SET owner_id = ? WHERE id = ?", 101, 7)

Set ``"autocommit": False`` in ``connection_config`` to make every session a
single unit of work that you commit explicitly.

Result Column Names
===================

Db2 folds unquoted identifiers to uppercase, so ``SELECT id FROM users`` reports a
column named ``ID``. With ``enable_lowercase_column_names`` (default ``True``),
names that consist only of uppercase letters, digits, and underscores are
lowercased in result rows, so ``row["id"]`` works. Quoted mixed-case names such as
``"MixedCase"`` are kept as written. Set
``driver_features={"enable_lowercase_column_names": False}`` to keep the names
Db2 reports.

The Db2 Dialect
===============

SQLSpec registers a ``db2`` SQLGlot dialect. Use it to transpile SQL written for
other databases or with the query builder (``sql.select(..., dialect="db2")``):

.. code-block:: python

   import sqlglot

   import sqlspec.dialects.db2  # noqa: F401

   print(sqlglot.transpile("SELECT id FROM users ORDER BY id LIMIT 10 OFFSET 20", read="postgres", write="db2")[0])
   # SELECT id FROM users ORDER BY id OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY
   print(sqlglot.transpile("SELECT CURRENT_TIMESTAMP + INTERVAL '1 day'", read="postgres", write="db2")[0])
   # SELECT CURRENT TIMESTAMP + 1 DAY FROM SYSIBM.SYSDUMMY1

The dialect covers:

* ``FETCH FIRST n ROWS ONLY`` and ``OFFSET m ROWS FETCH NEXT n ROWS ONLY`` paging.
* ``SYSIBM.SYSDUMMY1`` for ``SELECT`` statements without a ``FROM`` clause, and
  ``VALUES`` statements.
* Special registers (``CURRENT TIMESTAMP``, ``CURRENT DATE``, ``CURRENT SCHEMA``,
  ``CURRENT SERVER``, ``CURRENT TIMEZONE``) and labeled durations
  (``CURRENT DATE + 1 DAYS - 2 MONTHS``).
* Isolation and lock clauses (``WITH UR``, ``WITH CS``, ``WITH RS``, ``WITH RR``,
  ``USE AND KEEP ... LOCKS``), which round-trip unchanged.
* Db2 types such as ``DECFLOAT``, ``GRAPHIC``, ``VARGRAPHIC``, ``DBCLOB``, ``CLOB``
  and ``BLOB``.
* ``MERGE`` statements for builder upserts (``sql.upsert(..., dialect="db2")``).

Row locks from the query builder translate to Db2 isolation clauses:

* ``for_update()`` renders ``WITH RS USE AND KEEP UPDATE LOCKS``.
* ``for_share()`` renders ``WITH RS USE AND KEEP SHARE LOCKS``.
* ``skip_locked=True`` appends ``SKIP LOCKED DATA``.
* ``nowait=True`` and ``of=...`` raise ``SQLBuilderError``. Db2 has no ``NOWAIT``;
  set ``CURRENT LOCK TIMEOUT`` on the session instead.

.. code-block:: python

   from sqlspec import sql

   query = (
       sql.select("id", "payload", dialect="db2")
       .from_("jobs")
       .where("status = 'new'")
       .order_by("id")
       .limit(5)
       .for_update(skip_locked=True)
   )
   print(query.build().sql)
   # ... FETCH FIRST 5 ROWS ONLY WITH RS USE AND KEEP UPDATE LOCKS SKIP LOCKED DATA

.. warning::

   IBM's ``db2-sqlglot-dialect`` package registers the same ``db2`` entry point in
   ``sqlglot.dialects`` and pins ``sqlglot<30.10``, which conflicts with SQLSpec's
   ``sqlglot>=30.13`` requirement. Do not install both packages in one
   environment.

Data Dictionary
===============

``session.data_dictionary`` reads the ``SYSCAT`` catalog views. Unqualified
lookups use ``CURRENT SCHEMA``.

.. code-block:: python

   with config.provide_session() as session:
       tables = session.data_dictionary.get_tables(session)
       columns = session.data_dictionary.get_columns(session, table="USERS")
       foreign_keys = session.data_dictionary.get_foreign_keys(session, table="ORDERS")

Arrow
=====

``session.select_to_arrow()`` converts result rows into a ``pyarrow.Table``. The
conversion runs in Python; for columnar reads straight from the database, use the
:doc:`arrow-odbc adapter <arrow_odbc>` with the Db2 ODBC driver (see its
:ref:`IBM Db2 section <arrow-odbc-db2>`).

Extensions
==========

Both configs provide stores for the Litestar session backend, the events queue,
and the Google ADK session and memory services. Stores create their tables on
first use, and every timestamp they write is stored in UTC.

.. list-table::
   :header-rows: 1

   * - Extension
     - Sync config
     - Async config
   * - Litestar sessions
     - ``sqlspec.adapters.db2.litestar.Db2SyncStore``
     - ``sqlspec.adapters.db2.litestar.Db2AsyncStore``
   * - Events queue
     - ``sqlspec.adapters.db2.events.Db2SyncEventQueueStore``
     - ``sqlspec.adapters.db2.events.Db2AsyncEventQueueStore``
   * - ADK sessions
     - ``sqlspec.adapters.db2.adk.Db2SyncADKStore``
     - ``sqlspec.adapters.db2.adk.Db2AsyncADKStore``
   * - ADK memory
     - ``sqlspec.adapters.db2.adk.Db2SyncADKMemoryStore``
     - ``sqlspec.adapters.db2.adk.Db2AsyncADKMemoryStore``

SQLSpec migrations run on both configs.

API Reference
=============

Configuration
-------------

.. autoclass:: sqlspec.adapters.db2.Db2SyncConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.db2.Db2AsyncConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.db2.Db2ConnectionParams
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.db2.Db2PoolParams
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.db2.Db2AsyncPoolParams
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.db2.Db2DriverFeatures
   :members:
   :show-inheritance:

Drivers
-------

.. autoclass:: sqlspec.adapters.db2.Db2SyncDriver
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.db2.Db2AsyncDriver
   :members:
   :show-inheritance:

Connection Pools
----------------

.. autoclass:: sqlspec.adapters.db2.pool.Db2SyncConnectionPool
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.db2.pool.Db2AsyncConnectionPool
   :members:
   :show-inheritance:

Data Dictionaries
-----------------

.. autoclass:: sqlspec.adapters.db2.Db2SyncDataDictionary
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.db2.Db2AsyncDataDictionary
   :members:
   :show-inheritance:
