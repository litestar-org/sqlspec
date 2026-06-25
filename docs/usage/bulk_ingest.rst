Bulk Ingest
===========

SQLSpec exposes native bulk-ingest fast paths through a small, adapter-agnostic
storage-bridge API. High-volume writes use each driver's database primitive --
``COPY``, ``LOAD DATA LOCAL INFILE``, direct path load, ``BulkCopy``, Arrow
ingest, load jobs, or mutations -- instead of generic row-by-row execution.

The API
-------

Three methods cover the common shapes. They share return type
``StorageBridgeJob`` (its ``telemetry`` dict reports ``rows_processed``):

- ``load_from_arrow(table, source, *, overwrite=False)`` -- load an Arrow table
  (or anything coercible to one) using the adapter's native ingest path.
- ``load_from_storage(table, source, *, file_format, overwrite=False)`` -- load
  a staged artifact (a local path or cloud URI) into a table.
- ``load_from_records(table, records, *, columns=None, overwrite=False)`` --
  load in-memory rows. ``records`` may be mappings (columns derived from the
  keys) or positional sequences (``columns`` required). Records are normalized
  and routed through the adapter's native ``load_from_arrow`` path, so every
  adapter that supports Arrow ingest supports records too.

.. code-block:: python

    # dict records -- columns inferred from keys
    driver.load_from_records("orders", [{"id": 1, "total": 9.99}, {"id": 2, "total": 4.50}])

    # positional records -- columns required
    driver.load_from_records("orders", [(3, 1.0), (4, 2.0)], columns=["id", "total"])

Empty input, mismatched mapping keys, or a positional/column width mismatch
raise :class:`~sqlspec.exceptions.ImproperConfigurationError`.

Capability matrix
-----------------

.. list-table::
   :header-rows: 1
   :widths: 18 42 22 18

   * - Adapter
     - Native ingest path
     - Transactionality
     - Gate / opt-in
   * - asyncpg
     - ``COPY`` (``copy_records_to_table``)
     - Atomic; exact row counts
     - Always on
   * - psycopg (sync/async)
     - ``COPY`` streaming ``write_row``
     - Atomic; exact row counts
     - Always on
   * - psqlpy
     - Binary ``COPY`` with ``INSERT`` fallback
     - Atomic
     - Always on
   * - adbc
     - ``adbc_ingest`` (append/replace)
     - Driver-dependent; FlightSQL falls back to per-row
     - Always on
   * - duckdb
     - ``register`` + ``INSERT ... SELECT``
     - Single connection transaction
     - Always on
   * - sqlite / aiosqlite
     - ``executemany`` inside one ``BEGIN IMMEDIATE``
     - Atomic when the driver owns the transaction; rolls back on error
     - Always on
   * - oracledb
     - direct path load (Thin mode, default); ``executemany`` fallback
     - Per ``execute_many``; array-DML row counts available
     - ``enable_direct_path_load=False`` to force fallback; ``oracle_batch_errors`` /
       ``oracle_array_dml_row_counts`` execution args
   * - MySQL family (pymysql, asyncmy, aiomysql, mysql-connector)
     - ``executemany`` (default); ``LOAD DATA LOCAL INFILE`` (opt-in)
     - Server-managed
     - ``enable_local_infile_bulk_load`` + connection ``local_infile`` /
       ``allow_local_infile``
   * - bigquery
     - Parquet load job (default); Arrow Storage Write API (opt-in)
     - All-or-nothing load job / PENDING write stream
     - ``enable_storage_write_api``; load retry/timeout via job-control features
   * - spanner
     - ``Transaction.insert_or_update`` mutations (upsert); Batch Write API (opt-in)
     - In-transaction (default); independently committed groups (Batch Write)
     - Always on; ``enable_batch_write_api`` for high-throughput groups
   * - mssql-python
     - ``cursor.bulkcopy()`` via ``load_from_arrow``
     - Driver-managed
     - Always on
   * - arrow_odbc
     - ``bulk_insert_arrow`` via ``load_from_arrow``
     - Driver-managed
     - Always on

Security and opt-in paths
-------------------------

Some fast paths are opt-in because they read local files or change semantics:

- **MySQL ``LOAD DATA LOCAL INFILE``** requires both the adapter feature
  ``enable_local_infile_bulk_load`` and the connection's local-infile setting
  (``local_infile=True`` for pymysql/aiomysql/asyncmy, ``allow_local_infile=True``
  for mysql-connector). Enabling the feature without the connection gate raises
  :class:`~sqlspec.exceptions.ImproperConfigurationError` at config construction.
  The MySQL server must also have ``local_infile`` enabled. mysql-connector
  additionally honors ``allow_local_infile_in_path`` -- the staged temp file must
  live under that directory when it is set.
- **Oracle direct path load** is the default bulk-ingest transport in Thin mode.
  Set ``enable_direct_path_load=False`` to force ``executemany``. Connections
  that do not expose the Direct Path Load API, including Thick-mode connections,
  silently fall back to ``executemany``.
- **BigQuery Storage Write API** (``enable_storage_write_api``) streams Arrow
  rows for ``load_from_arrow`` appends and falls back to the Parquet load job
  when the Storage client is unavailable; ``overwrite=True`` always uses a
  Parquet ``WRITE_TRUNCATE`` load job.
- **Spanner Batch Write API** (``enable_batch_write_api``) routes
  ``load_from_arrow`` through ``Database.mutation_groups().batch_write()`` for
  high-throughput, independently committed ``insert_or_update`` groups instead
  of a single in-transaction flush. The upsert semantics keep each group
  idempotent on replay.

Examples
--------

MySQL ``LOAD DATA LOCAL INFILE``:

.. code-block:: python

    from sqlspec.adapters.pymysql import PyMysqlConfig

    config = PyMysqlConfig(
        connection_config={"host": "localhost", "local_infile": True},
        driver_features={"enable_local_infile_bulk_load": True},
    )
    with config.provide_session() as driver:
        driver.load_from_arrow("orders", arrow_table)

Oracle per-call batch error and array-DML row-count reporting:

.. code-block:: python

    statement_config = driver.statement_config.replace(
        execution_args={"oracle_batch_errors": True, "oracle_array_dml_row_counts": True}
    )
    result = driver.execute_many(
        "INSERT INTO orders (id, total) VALUES (:1, :2)", rows, statement_config=statement_config
    )
    failures = result.metadata["oracle_batch_errors"]      # list of {offset, code, message}
    row_counts = result.metadata["oracle_dml_row_counts"]  # per-statement affected rows

.. note::

   Spanner ``load_from_arrow`` uses ``insert_or_update`` mutations, so re-running
   the same rows is an idempotent upsert rather than a primary-key collision.
