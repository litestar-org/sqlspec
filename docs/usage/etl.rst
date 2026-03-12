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
transfers between databases that support native Arrow (ADBC, DuckDB, BigQuery).

.. code-block:: python

    # Extract as Arrow table
    arrow_result = await source_session.select_to_arrow(
        "SELECT * FROM large_table WHERE updated > :since",
        {"since": last_sync},
    )

    # Arrow table can be converted to pandas, polars, or written to Parquet
    df = arrow_result.to_pandas()

    # Or use native Arrow paths for zero-copy with DuckDB/ADBC
    arrow_result = await session.select_to_arrow(
        "SELECT * FROM events",
        return_format="table",    # "table", "batch", "batches", or "reader"
        batch_size=10000,         # rows per batch
    )

Supported ``return_format`` values:

- ``"table"`` -- single ``pyarrow.Table`` (default)
- ``"batch"`` -- single ``RecordBatch``
- ``"batches"`` -- iterator of ``RecordBatch`` objects
- ``"reader"`` -- ``RecordBatchReader`` for streaming

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
