Drivers and Querying
====================

SQLSpec provides a unified driver interface across supported databases. This page
covers the common execution patterns and points to adapter-specific configuration.

Supported Drivers (High Level)
------------------------------

- **PostgreSQL**: asyncpg, psycopg (sync/async), psqlpy, ADBC
- **SQLite**: sqlite3, aiosqlite, ADBC
- **MySQL**: asyncmy, mysql-connector, pymysql
- **Analytics / Cloud**: DuckDB, BigQuery, Spanner, Oracle, ADBC

Core Execution Pattern
----------------------

.. literalinclude:: /examples/drivers/sqlite_connection.py
   :language: python
   :caption: ``sqlite session``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Transactions
------------

.. literalinclude:: /examples/drivers/transaction_handling.py
   :language: python
   :caption: ``manual transaction``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Parameter Binding
-----------------

.. literalinclude:: /examples/drivers/parameter_binding.py
   :language: python
   :caption: ``positional and named parameters``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Schema Mapping
--------------

Use the ``schema_type`` parameter on ``select()``, ``select_one()``, and
``select_one_or_none()`` to map result rows to dataclass instances automatically.

.. literalinclude:: /examples/querying/schema_mapping.py
   :language: python
   :caption: ``schema_type mapping``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Scalar Values and Pagination
-----------------------------

``select_value`` returns a single scalar from a one-row, one-column result.
``select_value_or_none`` returns ``None`` when no rows match.
``select_with_total`` returns both the data page and the total count for pagination.

.. literalinclude:: /examples/querying/batch_operations.py
   :language: python
   :caption: ``scalar values, execute_many, and pagination``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Streaming Results
-----------------

``select_stream()`` returns a context-managed stream of dict rows fetched in
bounded chunks using each driver's native streaming primitive. Adapters without
a native path materialize the full result eagerly by default (not
bounded-memory). Pass ``native_only=True`` to require a native stream and raise
``ImproperConfigurationError`` when the adapter has no native path.

.. literalinclude:: /examples/querying/streaming_results.py
   :language: python
   :caption: ``streaming rows``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Adapter capability is discoverable via ``Config.supports_native_row_streaming``.
Native paths: psycopg and CockroachDB-psycopg (server-side named cursors),
asyncpg and CockroachDB-asyncpg (cursors inside a stream-owned transaction),
pymysql/aiomysql/asyncmy (``SSCursor``), mysql-connector (unbuffered cursors),
sqlite/aiosqlite and oracledb (chunked ``fetchmany``), psqlpy (server-side
cursor with ``array_size``), and BigQuery (page-wise result iteration).
ADBC, DuckDB, mssql-python, and Spanner are eager-fallback only for row
streaming. ``arrow-odbc`` row streaming also materializes dict rows eagerly, but
``select_to_arrow(..., return_format="reader" | "batches")`` uses the native
``arrow_odbc`` ``RecordBatchReader`` path.

Lifetime and transaction rules:

- Close the stream (or exhaust it) before issuing other statements on the same
  connection. ``close()`` mid-iteration releases the cursor and is idempotent.
- psycopg, asyncpg, and psqlpy streams open their own transaction (a savepoint
  when one is already active) and commit it on close, so they stream correctly
  even on autocommit connections.
- MySQL unbuffered cursors drain remaining rows when closed mid-iteration.
- oracledb streaming returns raw driver values for LOB columns.
- BigQuery ``page_size`` is advisory; pages may exceed the requested chunk size.
- An exception raised during iteration closes the stream and propagates; the
  connection remains usable afterwards (issue a rollback first on PostgreSQL
  drivers, whose transaction is aborted by the failed statement).

Statement Stacks
----------------

Statement stacks bundle multiple SQL statements plus parameter sets. Drivers that
support native pipelines or batch execution can send the stack in a single round
trip, while others execute each statement sequentially.

.. literalinclude:: /examples/querying/statement_stack.py
   :language: python
   :caption: ``statement stack``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Driver Configuration Examples
-----------------------------

.. literalinclude:: /examples/drivers/asyncpg_connection.py
   :language: python
   :caption: ``asyncpg config``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

.. literalinclude:: /examples/drivers/cockroach_psycopg_connection.py
   :language: python
   :caption: ``cockroach + psycopg``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

.. literalinclude:: /examples/drivers/mysqlconnector_connection.py
   :language: python
   :caption: ``mysql connector config``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Related References
------------------

- :doc:`../reference/adapters` for full adapter configuration reference.
- :doc:`/reference/adapters` for adapter capabilities and connection profiles.
