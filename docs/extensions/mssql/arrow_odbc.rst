==========
arrow-odbc
==========

``arrow_odbc`` is a generic Arrow-over-ODBC adapter. Use it when your primary
goal is bulk Arrow transfer and the database is reachable through an ODBC
driver. For full SQL Server application support, prefer :doc:`mssql_python`.

Why This Adapter Exists
=======================

Apache Arrow's ADBC ecosystem does not provide first-party drivers for every
database. ``arrow-odbc`` fills that gap for SQL Server, Oracle, MySQL, Db2,
HANA, Teradata, Vertica, Sybase, Informix, Firebird, Impala, Hive, and other
ODBC sources.

The SQLSpec adapter is intentionally sync-only and Arrow-first:

- ``select_to_arrow(..., return_format="table")`` materializes a ``pyarrow.Table``.
- ``select_to_arrow(..., return_format="batches")`` returns Arrow record batches.
- ``bulk_insert_arrow`` inserts a ``pyarrow.Table`` or reader into a table.
- Row-oriented ``executemany`` is not implemented; use Arrow import APIs.

Install
=======

Install SQLSpec with the optional dependency:

.. code-block:: bash

   uv add "sqlspec[arrow-odbc]"

Install a vendor ODBC driver separately:

.. code-block:: bash

   # SQL Server on Debian/Ubuntu
   sudo apt-get install msodbcsql18 unixodbc

   # MySQL on Debian/Ubuntu
   sudo apt-get install libmyodbc unixodbc

Oracle requires the Instant Client Basic and Instant Client ODBC packages from
Oracle. On macOS and Windows, use the vendor installers or package-manager
formulae for the relevant ODBC driver.

Connection Strings
==================

SQL Server:

.. code-block:: python

   from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig

   config = ArrowOdbcConfig(
       connection_config={
           "connection_string": (
               "Driver={ODBC Driver 18 for SQL Server};"
               "Server=tcp:localhost,1433;"
               "Database=app;"
               "UID=sa;"
               "PWD=change-me;"
               "Encrypt=yes;"
               "TrustServerCertificate=yes;"
           )
       }
   )

Oracle:

.. code-block:: python

   config = ArrowOdbcConfig(
       connection_config={
           "connection_string": (
               "Driver={Oracle in instantclient_19_24};"
               "DBQ=localhost:1521/FREEPDB1;"
               "UID=app;"
               "PWD=change-me;"
           )
       }
   )

MySQL:

.. code-block:: python

   config = ArrowOdbcConfig(
       connection_config={
           "connection_string": (
               "Driver={MySQL ODBC 8.0 Unicode Driver};"
               "Server=localhost;"
               "Database=app;"
               "UID=app;"
               "PWD=change-me;"
           )
       }
   )

Dialect Detection
=================

The driver resolves SQLSpec dialect behavior from the ODBC DBMS or driver name.
Examples:

- ``Microsoft SQL Server`` and ``ODBC Driver 18 for SQL Server`` map to
  ``mssql``.
- ``Oracle in instantclient_19_24`` maps to ``oracle``.
- ``MySQL ODBC 8.0 Unicode Driver`` maps to ``mysql``.

Unknown sources fall back to ``sqlite`` because qmark parameters are the safest
generic ODBC default.

Arrow Reads
===========

.. code-block:: python

   with config.provide_session() as session:
       result = session.select_to_arrow("SELECT id, total FROM orders")
       table = result.get_data()

       result = session.select_to_arrow("SELECT * FROM orders", return_format="batches", batch_size=100_000)
       for batch in result.get_data():
           process(batch)

Arrow Inserts
=============

``bulk_insert_arrow`` uses ``Connection.from_table_to_db`` when the source is a
``pyarrow.Table`` and falls back to ``Connection.insert_into_table`` for readers.

.. code-block:: python

   import pyarrow as pa

   table = pa.table({"id": [1, 2], "name": ["Ada", "Grace"]})

   with config.provide_session() as session:
       session.bulk_insert_arrow("dbo.people", table, chunk_size=10_000)

When to Use It
==============

Use ``arrow_odbc`` when:

- You need to ingest a ``pyarrow.Table`` into SQL Server.
- You need Arrow transfer for Oracle, MySQL, or another ODBC-only target.
- You are moving analytical batches rather than serving transactional request
  traffic.

Use a dedicated SQLSpec adapter when:

- You need migrations, ADK stores, events, or framework stores.
- You need a native async driver.
- You need row-oriented batch operations.

Limitations
===========

- No ``executemany``; use ``bulk_insert_arrow``.
- No adapter-specific migration tracker.
- No ADK store or Litestar store.
- Savepoint support depends on the target database and is intentionally minimal.
- ODBC driver installation and DSN configuration are outside SQLSpec.
