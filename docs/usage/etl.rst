ETL & Data Pipelines
====================

SQLSpec works well for ETL (Extract-Transform-Load) workflows. Use multiple
database configs to move data between systems, and leverage Arrow-based methods
for high-performance bulk transfers.

Multi-Database ETL
------------------

Register source and target databases on a single ``SQLSpec`` instance. Extract
from one, transform in Python, and load into the other.

.. literalinclude:: /examples/patterns/etl_pipeline.py
   :language: python
   :caption: ``multi-database ETL pipeline``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Arrow-Based Bulk Transfer
-------------------------

For large datasets, use ``select_to_arrow()`` to get results as Apache Arrow
tables. This avoids per-row Python object overhead and enables zero-copy
transfers between databases that support native Arrow (ADBC, DuckDB, BigQuery,
Arrow ODBC, mssql-python, and Oracle).

Passing ``native_only=True`` ensures that the query executes through the driver's
fast, zero-copy C/C++ columnar path. If the adapter does not support native Arrow,
it raises :class:`~sqlspec.exceptions.ImproperConfigurationError` rather than
silently falling back to row-by-row dict conversion.

.. code-block:: python

    # Extract as Arrow table using zero-copy native path
    arrow_result = await source_session.select_to_arrow(
        "SELECT * FROM large_table WHERE updated > :since",
        {"since": last_sync},
        native_only=True,
    )

    # Convert to Polars or Pandas DataFrames
    df_polars = arrow_result.to_polars()
    df_pandas = arrow_result.to_pandas()

    # Direct export to cloud storage (Parquet / Arrow IPC)
    await arrow_result.write_to_storage_async("s3://my-bucket/exports/table.parquet")

Direct Zero-Copy Cross-Database Transfer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Because ``load_from_arrow()`` accepts an ``ArrowResult`` directly, you can stream
data between heterogeneous databases without any intermediate Python objects:

.. code-block:: python

    # 1. Zero-copy extract from PostgreSQL via ADBC
    arrow_result = await postgres_session.select_to_arrow(
        "SELECT id, event_type, payload, created_at FROM events WHERE processed = false",
        native_only=True,
    )

    # 2. Direct zero-copy load into DuckDB analytical staging table
    job = duckdb_session.load_from_arrow("staging_events", arrow_result)
    print(f"Transferred {job.telemetry['rows_processed']} rows")

Streaming Large Result Sets
~~~~~~~~~~~~~~~~~~~~~~~~~~~

To stream datasets that exceed available RAM, use ``return_format="reader"`` to
obtain a ``pyarrow.RecordBatchReader``:

.. code-block:: python

    arrow_result = await session.select_to_arrow(
        "SELECT * FROM events",
        return_format="reader",   # Streams RecordBatch objects
        batch_size=10000,         # Rows per batch
    )
    reader = arrow_result.get_data()
    for batch in reader:
        process_batch(batch)

Supported ``return_format`` values:

- ``"table"`` -- single ``pyarrow.Table`` (default)
- ``"batch"`` -- single ``RecordBatch``
- ``"batches"`` -- iterator of ``RecordBatch`` objects
- ``"reader"`` -- ``RecordBatchReader`` for streaming

.. seealso::

   :doc:`bulk_ingest` for the inbound side -- loading Arrow tables, staged
   files, and in-memory records into a table via native driver primitives.


DuckDB as Staging Layer
-----------------------

DuckDB excels as an ETL staging layer because it can read Parquet, CSV, and
JSON files natively and attach to external PostgreSQL databases.

.. code-block:: python

    from sqlspec import SQLSpec
    from sqlspec.adapters.duckdb import DuckDBConfig

    spec = SQLSpec()
    config = spec.add_config(
        DuckDBConfig(connection_config={"database": "/tmp/staging.db"})
    )

    with spec.provide_session(config) as session:
        # Read directly from Parquet files
        session.execute(
            "CREATE TABLE staging AS SELECT * FROM read_parquet('data/*.parquet')"
        )

        # Transform and aggregate
        session.execute(
            "CREATE TABLE summary AS "
            "SELECT date, COUNT(*) as events "
            "FROM staging GROUP BY date"
        )

        # Export results
        result = session.select("SELECT * FROM summary ORDER BY date")

Related Guides
--------------

- :doc:`configuration` for multi-database setup.
- :doc:`drivers_and_querying` for the full query API.
- :doc:`../reference/adapters` for adapter-specific Arrow capabilities.
