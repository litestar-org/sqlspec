# DuckDB

`sqlspec.adapters.duckdb` wraps the in-process analytical DuckDB engine (`duckdb` + `pyarrow`), combining thread-safe connection pooling, declarative extension and cloud secret management, hybrid native/Arrow row materialization, zero-copy Arrow transfers, and per-statement JSON execution profiling.

## Install and When to Choose

```bash
uv add "sqlspec[duckdb]"
```

Choose `DuckDBConfig` for embedded OLAP analytics, local lakehouse queries over Parquet/CSV/JSON on S3 or GCS, fast feature engineering pipelines, and columnar ETL jobs running inside a single Python worker.

## Minimal Connection and Pool Setup

```python
from sqlspec import SQLSpec
from sqlspec.adapters.duckdb import DuckDBConfig

spec = SQLSpec()
config = spec.add_config(
    DuckDBConfig(
        connection_config={
            "database": ":memory:",
            "pool_min_size": 1,
            "pool_max_size": 4,
        },
        driver_features={
            "extensions": [{"name": "httpfs"}, {"name": "json"}],
            "enable_statement_profiling": False,
        },
    )
)
```

For persistent analytical databases, set `"database": "var/analytics.duckdb"` (and `"read_only": True` on read-only replica processes).

## Key Driver Features and Pool Tuning

| Setting | Location | Description |
| --- | --- | --- |
| `database` / `read_only` | `connection_config` | File path or `":memory:"` (shared across pooled cursors derived from the primary connection) and read-only open flag. |
| `pool_min_size` / `pool_max_size` | `connection_config` | Bounds for `DuckDBConnectionPool`. |
| `threads` / `memory_limit` | `connection_config` | DuckDB engine worker threads and memory cap (such as `"4GB"`). |
| `extensions` | `driver_features` | Sequence of `DuckDBExtensionConfig` dicts (`name`, `repository`, `force_install`) installed/loaded on startup. |
| `secrets` | `driver_features` | Sequence of `DuckDBSecretConfig` dicts configuring S3, GCS, Azure, R2, or HuggingFace credentials via `CREATE SECRET`. |
| `enable_statement_profiling` | `driver_features` | Captures DuckDB JSON execution plans (`EXPLAIN (ANALYZE, FORMAT JSON)`) onto `StatementEvent.profile`. |
| `enable_uuid_conversion` | `driver_features` | Normalizes UUID columns consistently across native and Arrow execution paths. |

See [DuckDB API Reference](/reference/adapters/duckdb/) for complete `DuckDBConnectionParams`, `DuckDBPoolParams`, `DuckDBExtensionConfig`, `DuckDBSecretConfig`, and `DuckDBDriverFeatures` TypedDicts.

## Type Mapping and Dialect Notes

- **Unified Native Python Type Contract (`ARROW_MIN_ROWS = 64`)**: Small point lookups (`< 64` observed rows) and queries returning Arrow-divergent types (`HUGEINT`, `UHUGEINT`, `BIT`, `TIMETZ`, `TIMESTAMP_NS`, `INTERVAL`, `MAP`, `BIGNUM`/`VARINT`, fixed-size arrays, or nested `UUID`) materialize directly via `cursor.fetchall()` so cache-miss and cache-hit calls always return identical DuckDB-native Python types without paying Arrow conversion overhead on 1-row reads. Larger analytical scans route through `to_arrow_table()` automatically.
- **Zero-Copy Arrow Reads and Writes**: Call `session.select_to_arrow(...)` (`return_format="table"`, `"batches"`, or `"reader"`) and `session.load_from_arrow(...)` to exchange columnar tables with Polars, Pandas, or PyArrow without dictionary allocations.
- **Caller-Managed Transactions in `execute_many`**: `session.execute_many(...)` delegates directly to `cursor.executemany(...)` without injecting implicit `BEGIN`/`COMMIT` statements, preserving any outer transaction opened via `session.begin()`.
- **ADK Full-Text and Cosine Vector Search**: `DuckDBADKMemoryStore` supports DuckDB's `fts` extension alongside `ARRAY` cosine similarity for local RAG and agent memory testing.
