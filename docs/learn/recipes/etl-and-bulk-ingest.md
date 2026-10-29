# ETL and Bulk Ingest

Moving data between operational databases, analytical engines, and cloud object storage requires bounded memory usage and batched or columnar execution. SQLSpec provides three graduated tiers depending on dataset volume: batched DB-API writes (`execute_many` + `select_stream`), zero-copy **Apache Arrow** transfers (`select_to_arrow` + `load_from_arrow`), and direct **Storage Bridge** jobs (`select_to_storage` + `load_from_storage`).

## Two-Database Extract, Transform, and Load

Register source and target configurations on a single `SQLSpec` instance to extract filtered rows from an upstream store and batch-load transformed records into a downstream target:

```{ .python title="docs/examples/patterns/etl_pipeline.py" hl_lines="18 19 29 33 34 35" }
--8<-- "docs/examples/patterns/etl_pipeline.py"
```

## Bounded-Memory Chunked Streaming

When extracting hundreds of thousands of rows over DB-API drivers, pair `session.select_stream(..., chunk_size=1000)` with batched `execute_many` slices so resident memory stays flat regardless of table size:

```python
from itertools import islice

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

spec = SQLSpec()
source = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
target = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

with spec.provide_session(source) as src, spec.provide_session(target) as dst:
    src.execute("CREATE TABLE raw_events (id INTEGER PRIMARY KEY, kind TEXT)")
    src.execute_many("INSERT INTO raw_events (kind) VALUES (?)", [("click",), ("signup",), ("click",)])
    dst.execute("CREATE TABLE event_archive (id INTEGER PRIMARY KEY, kind TEXT)")

    with src.select_stream("SELECT id, kind FROM raw_events ORDER BY id", chunk_size=500) as stream:
        iterator = ((row["id"], row["kind"]) for row in stream)
        while batch := list(islice(iterator, 500)):
            dst.execute_many("INSERT INTO event_archive (id, kind) VALUES (?, ?)", batch)
```

## Columnar Transfers with Arrow and Parquet Storage

For analytical pipelines across DuckDB, PostgreSQL, Oracle 23ai/26ai, BigQuery, ADBC, and S3/GCS Parquet lakes, bypass row-by-row Python dictionaries completely using `select_to_arrow`, `select_to_storage`, `load_from_arrow`, and `load_from_storage`:

```python
from pathlib import Path
import tempfile

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

spec = SQLSpec()
config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

with tempfile.TemporaryDirectory() as tmp_dir, spec.provide_session(config) as session:
    parquet_file = Path(tmp_dir) / "orders.parquet"
    session.execute("CREATE TABLE orders (id INTEGER, amount REAL)")
    session.execute("CREATE TABLE orders_copy (id INTEGER, amount REAL)")
    session.execute_many("INSERT INTO orders VALUES (?, ?)", [(1, 49.95), (2, 125.00)])

    export_job = session.select_to_storage("SELECT id, amount FROM orders", parquet_file, format_hint="parquet")
    import_job = session.load_from_storage("orders_copy", parquet_file, file_format="parquet")
    assert export_job.telemetry["rows_processed"] == 2
    assert import_job.telemetry["rows_processed"] == 2
```

### Inspecting `StorageBridgeJob` Telemetry

`select_to_storage`, `load_from_storage`, and `load_from_arrow` return a `StorageBridgeJob` carrying structured telemetry you can emit to job metrics or audit tables:

| Attribute / Key | Meaning |
| --- | --- |
| `job.status` | Execution status string (such as `"completed"`). |
| `job.telemetry["rows_processed"]` | Total rows exported or ingested by the pipeline. |
| `job.telemetry["bytes_processed"]` | Compressed or uncompressed payload bytes transferred when reported by the backend. |
| `job.telemetry["duration_s"]` | End-to-end transfer duration in seconds. |
| `job.telemetry["destination"]` | Resolved target URI or table name for audit logging. |

## Native Engine Bulk Acceleration

Several adapters accelerate `load_from_arrow` and `load_from_storage` using native wire or server-side bulk paths automatically:

- **PostgreSQL (`psycopg` / `asyncpg` / `adbc`)**: Uses binary `COPY` / Arrow C-stream ingestion for high-throughput table loads.
- **DuckDB & ADBC**: Zero-copy Arrow C Data Interface (`__arrow_c_stream__`) registration and native Parquet reads/writes.
- **Oracle Autonomous Database (`oracledb`)**: Supports `DBMS_CLOUD.COPY_DATA` server-side object-store ingestion when `driver_features={"dbms_cloud_credential": "..."}` is configured.
- **BigQuery**: Streams columnar reads via the BigQuery Storage Read API and loads Parquet/Arrow via Storage Write / load jobs.

## Recap

- Use `select_stream` + `execute_many` batches for row-oriented ETL so worker memory stays bounded.
- Use `select_to_arrow` / `load_from_arrow` for in-process columnar pipelines and `select_to_storage` / `load_from_storage` for Parquet lakehouse staging.
- Wrap target-table refreshes in an explicit transaction (`overwrite=True` or staging-table swap) so readers never observe half-loaded batches.
