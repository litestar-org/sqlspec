==========
arrow-odbc
==========

Sync Arrow-over-ODBC adapter built on `arrow-odbc <https://pypi.org/project/arrow-odbc/>`_.
Streams ``pyarrow.RecordBatchReader`` results from any ODBC-compliant driver,
making it a good fit for read-heavy analytical transfer between SQL Server,
PostgreSQL, MySQL, or other ODBC sources and the Arrow ecosystem.

SQL Server coverage is exercised in CI against SQL Server 2022 through
``pytest-databases`` and Microsoft ODBC Driver 18. The shared contract matrix
verifies native Arrow reads, Arrow reader/batch output, and Arrow bulk ingest
for this adapter. Row-oriented ``execute_many()`` is intentionally unsupported;
use ``load_from_arrow()`` for bulk writes.

The adapter exports a table-backed events queue store, a Litestar session
store, and Google ADK session/event and memory stores. They support SQL Server
connections through Microsoft ODBC Driver 18 and IBM Db2 LUW connections through
the IBM CLI/ODBC driver (see :ref:`arrow-odbc-db2`).

.. _arrow-odbc-db2:

IBM Db2
=======

arrow-odbc reads Db2 LUW 11.5 and later through the IBM CLI/ODBC driver. Db2 for
z/OS and Db2 for IBM i are not supported. For row-oriented work, pooling, and
async access use the :doc:`Db2 adapter <db2>`.

**Dialect detection.** The adapter picks the SQL dialect from the ODBC driver
name only: the ``Driver`` value of the connection string, or its ``DSN`` value
when there is no ``Driver``. Names containing ``db2``, ``IBM Data Server Driver``,
``clidriver``, or ``libdb2o`` select ``db2``. Database, host, and user names are
never used. When the driver is reached through a DSN whose name does not say
Db2, set ``driver_features={"dbms_name": "DB2"}``.

**Connection keywords.** For Db2, ``host`` (or ``server``) and ``port`` render as
the IBM CLI keywords ``Hostname`` and ``Port``, and ``Protocol=TCPIP`` is added
when a host is set. SQL Server options such as ``encrypt``,
``trust_server_certificate``, and ``trusted_connection`` raise
``ImproperConfigurationError``.

The ``ibm_db`` package (installed by ``sqlspec[db2]``) bundles the CLI driver, so
no separate ODBC driver installation is needed; unixODBC must be present on Linux.
Point ``Driver`` at the bundled ``libdb2o.so``:

.. code-block:: python

   from pathlib import Path

   import ibm_db
   from arrow_odbc import TextEncoding

   from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig

   package_dir = Path(ibm_db.__file__).resolve().parent
   driver = next(package_dir.rglob("clidriver/lib/libdb2o.so"), package_dir / "clidriver/lib/libdb2o.so")

   config = ArrowOdbcConfig(
       connection_config={
           "connection_string": f"Driver={driver};LongDataCompat=1;",
           "host": "db2.example.com",
           "port": 50000,
           "database": "SAMPLE",
           "uid": "db2inst1",
           "pwd": "secret",
           "autocommit": False,
       },
       driver_features={"payload_text_encoding": TextEncoding.UTF16},
   )

   print(config.statement_config.dialect)
   # db2

**Transactions.** Db2 has no ``BEGIN`` statement. ``begin()`` and
``transaction()`` require a connection opened with autocommit off, so set
``connection_config={"autocommit": False}`` for transactional work; on an
autocommit connection they raise ``ImproperConfigurationError``. Savepoints use
Db2 syntax.

**Large objects.** Add ``LongDataCompat=1`` to the connection string so ``BLOB``
columns arrive as binary rather than hexadecimal text and ``CLOB`` columns as
text.

**Text encoding.** For non-ASCII text, either set the ``DB2CODEPAGE=1208``
environment variable before connecting or set
``driver_features={"payload_text_encoding": TextEncoding.UTF16}`` (``TextEncoding``
comes from the ``arrow_odbc`` package). Text parameters are
always bound as text.

**Results.** Implicitly uppercase column names are lowercased (disable with
``driver_features={"enable_lowercase_column_names": False}``); quoted mixed-case
names are kept. Timezone-aware datetime parameters are bound as UTC. Db2
``TIMESTAMP(12)`` values are truncated to nanoseconds, and ``DECFLOAT``, ``XML``,
and ``BOOLEAN`` columns arrive as UTF-8 text.

Configuration
=============

.. autoclass:: sqlspec.adapters.arrow_odbc.ArrowOdbcConfig
   :members:
   :show-inheritance:

Connection Parameters
=====================

.. autoclass:: sqlspec.adapters.arrow_odbc.ArrowOdbcConnectionParams
   :members:
   :show-inheritance:

Driver Features
===============

.. autoclass:: sqlspec.adapters.arrow_odbc.ArrowOdbcDriverFeatures
   :members:
   :show-inheritance:

Driver
======

.. autoclass:: sqlspec.adapters.arrow_odbc.ArrowOdbcDriver
   :members:
   :show-inheritance:

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.arrow_odbc.data_dictionary.ArrowOdbcDataDictionary
   :members:
   :show-inheritance:

Extensions
==========

.. autoclass:: sqlspec.adapters.arrow_odbc.events.ArrowOdbcEventQueueStore
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.arrow_odbc.litestar.ArrowOdbcStore
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.arrow_odbc.adk.ArrowOdbcADKStore
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.arrow_odbc.adk.ArrowOdbcADKMemoryStore
   :members:
   :show-inheritance:

Schema Discovery
================

``ArrowOdbcDataDictionary.get_columns`` first uses bundled dialect catalog
queries. When no query exists for the detected dialect (or it returns no
rows) and a table name is given, the driver issues a zero-row probe
(``SELECT * FROM "schema"."table" WHERE 1=0``) and derives column names,
ordering, nullability, and SQL type names from the Arrow reader schema.
Arrow-derived type names are approximations (for example ``VARCHAR`` for any
string column); ``mssql_python`` and other ODBC adapters without native
metadata APIs remain SQL-only.

Extension Settings
==================

Use these types inside ``extension_config["adk"]``.

.. autoclass:: sqlspec.adapters.arrow_odbc.adk.ArrowOdbcADKConfig
   :members:
   :show-inheritance:
