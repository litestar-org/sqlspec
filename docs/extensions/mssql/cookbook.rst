========
Cookbook
========

MERGE Upsert
============

SQL Server does not support PostgreSQL-style ``ON CONFLICT``. Use ``MERGE`` for
single-row upserts:

.. code-block:: python

   sql = """
   MERGE INTO dbo.users AS target
   USING (SELECT ? AS id, ? AS name) AS src
      ON target.id = src.id
   WHEN MATCHED THEN
       UPDATE SET name = src.name
   WHEN NOT MATCHED THEN
       INSERT (id, name) VALUES (src.id, src.name);
   """

   session.execute(sql, (1, "Ada"))

JSON Columns
============

On SQL Server 2022 and earlier, store JSON in ``NVARCHAR(MAX)`` with an
``ISJSON`` check when appropriate. Query values with ``JSON_VALUE`` and arrays
or objects with ``JSON_QUERY``.

.. code-block:: sql

   CREATE TABLE dbo.documents (
       id INT IDENTITY PRIMARY KEY,
       payload NVARCHAR(MAX) NOT NULL,
       CONSTRAINT CK_documents_payload_json CHECK (ISJSON(payload) = 1)
   );

   SELECT JSON_VALUE(payload, '$.status') AS status
   FROM dbo.documents;

SQL Server versions with a native ``JSON`` type can use SQLSpec's MSSQL data
dictionary feature checks to choose that type for new schema.

UUID Round Trips
================

Use ``UNIQUEIDENTIFIER`` for ``uuid.UUID`` values:

.. code-block:: python

   from uuid import uuid4

   user_id = uuid4()
   session.execute("INSERT INTO dbo.users (id, name) VALUES (?, ?)", (user_id, "Ada"))

DATETIMEOFFSET
==============

Use ``DATETIME2`` for normalized UTC timestamps and ``DATETIMEOFFSET`` when the
original offset is part of the domain model. Avoid using SQL Server
``TIMESTAMP`` for temporal values; it is a rowversion counter.

.. code-block:: sql

   CREATE TABLE dbo.audit_log (
       id BIGINT IDENTITY PRIMARY KEY,
       created_at DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
       observed_at DATETIMEOFFSET(6) NULL
   );

BulkCopy
========

For large row iterables, use ``bulk_copy``:

.. code-block:: python

   rows = [(1, "Ada"), (2, "Grace")]

   session.bulk_copy(
       "dbo.people",
       rows,
       batch_size=64_000,
       table_lock=True,
       keep_identity=False,
       check_constraints=True,
   )

Set ``keep_identity=True`` only when identity values are already present and
the table has ``IDENTITY_INSERT`` enabled. ``table_lock=True`` is fastest for
dedicated loads but blocks concurrent writers.

Arrow Fetch for Analytics
=========================

.. code-block:: python

   result = session.select_to_arrow("SELECT * FROM dbo.fact_sales")
   table = result.get_data()
   table.write_parquet("fact_sales.parquet")

For streaming:

.. code-block:: python

   result = session.select_to_arrow("SELECT * FROM dbo.fact_sales", return_format="batches", batch_size=100_000)
   for batch in result.get_data():
       process(batch)

Microsoft Entra ID
==================

Use connection-string authentication values such as
``ActiveDirectoryDefault`` for hosted identity and
``ActiveDirectoryInteractive`` for local development:

.. code-block:: python

   config = MssqlPythonConfig(
       connection_config={
           "server": "tcp:example.database.windows.net",
           "database": "app",
           "authentication": "ActiveDirectoryDefault",
           "encrypt": True,
           "trust_server_certificate": False,
       }
   )

Always Encrypted
================

Always Encrypted is enabled through Microsoft connection-string keywords. There
is no SQLSpec-specific setting:

.. code-block:: python

   config = MssqlPythonConfig(
       connection_config={
           "connection_string": "Server=...;Database=...;Column Encryption Setting=Enabled;"
       }
   )

AlwaysOn Read-Only Replicas
===========================

Route read workloads to readable replicas with ``ApplicationIntent=ReadOnly``:

.. code-block:: python

   config = MssqlPythonConfig(
       connection_config={
           "server": "listener.example.com",
           "database": "app",
           "application_intent": "ReadOnly",
       }
   )

Litestar Session Store
======================

.. code-block:: python

   from litestar.middleware.session.server_side import ServerSideSessionConfig

   from sqlspec.adapters.mssql_python.litestar import MssqlPythonStore

   session_store = MssqlPythonStore(config)
   session_config = ServerSideSessionConfig(store="sessions")

   app = Litestar(
       route_handlers=[],
       middleware=[session_config.middleware],
       stores={"sessions": session_store},
       on_startup=[session_store.create_table],
   )

Sync vs Async Config
====================

Use ``MssqlPythonConfig`` in sync applications and scripts. Use
``MssqlPythonAsyncConfig`` when a framework or library expects SQLSpec's async
driver contract; it delegates blocking driver work to a worker thread.

arrow-odbc for Oracle
=====================

.. code-block:: python

   import pyarrow.parquet as pq

   from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig

   config = ArrowOdbcConfig(connection_config={"connection_string": "Driver={Oracle in instantclient_19_24};..."})

   with config.provide_session() as session:
       session.bulk_insert_arrow("APP.PEOPLE", pq.read_table("people.parquet"))

arrow-odbc for MySQL
====================

.. code-block:: python

   import pyarrow as pa

   from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig

   table = pa.table({"id": [1, 2], "name": ["Ada", "Grace"]})
   config = ArrowOdbcConfig(connection_config={"connection_string": "Driver={MySQL ODBC 8.0 Unicode Driver};..."})

   with config.provide_session() as session:
       session.bulk_insert_arrow("people", table)
