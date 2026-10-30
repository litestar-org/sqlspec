# Apache Arrow ADBC

`sqlspec.adapters.adbc` integrates Apache Arrow Database Connectivity (`adbc-driver-manager` + `pyarrow`), exchanging query results and bulk table loads directly over the C-level Arrow C Data Interface (`__arrow_c_stream__`) without row-by-row Python object marshaling.

## Install and When to Choose

Install the core `adbc` extra alongside one or more PyPI-installable ADBC wire drivers:

```bash
uv add "sqlspec[adbc]" adbc-driver-postgresql
```

| Target Engine | PyPI Driver Package | URI / Driver Example |
| --- | --- | --- |
| PostgreSQL / AlloyDB | `adbc-driver-postgresql` | `uri="postgresql://app:secret@localhost:5432/prod"` |
| SQLite | `adbc-driver-sqlite` | `uri="file:app.db"` or `":memory:"` |
| DuckDB (In-Process) | `duckdb` | `driver_name="duckdb"`, `uri="analytics.duckdb"` |
| DuckDB Quack (Remote gRPC) | `adbc-driver-quack` | `uri="quack+tls://host:443"`, `token="..."` |
| GizmoSQL / Arrow FlightSQL | `adbc-driver-gizmosql` or `adbc-driver-flightsql` | `uri="grpc+tls://host:31337"` |
| Snowflake | `adbc-driver-snowflake` | `driver_name="adbc_driver_snowflake"` |
| Google Cloud BigQuery | `adbc-driver-bigquery` | `driver_name="adbc_driver_bigquery"` |
| Oracle Database | `adbc-driver-oracle` | `uri="oracle://localhost:1521/FREEPDB1"` (pure Go, zero Instant Client) |
| IBM Db2 | `adbc-driver-db2` | `uri="db2://localhost:50000/appdb"` (pure Go DRDA, includes Linux `aarch64` wheels) |

Choose `AdbcConfig` when your primary workload extracts or ingests Arrow/Polars/Pandas/Parquet datasets across analytical warehouses or when you need pure-Go zero-client wire drivers for Oracle or Db2 on Linux ARM64.

## Minimal Connection and Pool Setup

```python
from sqlspec import SQLSpec, StatementConfig
from sqlspec.adapters.adbc import AdbcConfig

spec = SQLSpec()
config = spec.add_config(
    AdbcConfig(
        connection_config={
            "driver_name": "adbc_driver_postgresql",
            "uri": "postgresql://app:secret@localhost:5432/prod",
        },
        statement_config=StatementConfig(dialect="postgres"),
    )
)

with spec.provide_session(config) as session:
    arrow_result = session.select_to_arrow("SELECT id, total_cents FROM orders WHERE status = :s", s="paid")
```

## Key Driver Features and Pool Tuning

| Setting | Location | Description |
| --- | --- | --- |
| `driver_name` / `uri` | `connection_config` | ADBC driver module name and connection URI (auto-detected from URI scheme for `postgresql://`, `oracle://`, `db2://`, `quack://`, `grpc+tls://`). |
| `username` / `password` / `token` | `connection_config` | Credentials lifted automatically into each driver's expected `db_kwargs` (`adbc.quack.token`, Oracle `db_kwargs`, etc.). |
| `auth_type` / `tls_root_certs` / `headers` | `connection_config` | FlightSQL and GizmoSQL OAuth/SSO (`auth_type="external"`), mTLS, and gRPC options (`grpc_max_message_size`, `query_timeout`). |
| `db_kwargs` / `conn_kwargs` | `connection_config` | Low-level ADBC database and connection option dictionaries. |
| `enable_lowercase_column_names` | `driver_features` | Lowercases uppercase unquoted identifiers (`True` by default when `dialect` is `"oracle"` or `"db2"`). |
| `enable_statement_profiling` | `driver_features` | Captures DuckDB JSON execution profiles onto `StatementEvent.profile` on DuckDB-backed ADBC connections. |
| `enable_pgvector` / `enable_paradedb` | `driver_features` | Enables PostgreSQL `VECTOR` (`?::text::vector`) and hybrid RRF search in `AdbcADKMemoryStore`. |
| `enable_strict_type_coercion` | `driver_features` | Enforces strict Arrow schema validation during parameter and ingest conversion. |

See [ADBC API Reference](/reference/adapters/adbc/) for full `AdbcConnectionParams` and `AdbcDriverFeatures` TypedDict fields.

## Type Mapping and Dialect Notes

- **Zero-Copy `adbc_ingest`**: `session.load_from_arrow(table, arrow_table_or_reader)` passes `pyarrow.Table` and `pyarrow.RecordBatchReader` objects straight to `cursor.adbc_ingest` over `__arrow_c_stream__` without materializing intermediate rows.
- **Cross-Engine Placeholder Rewriting**: Although `adbc_driver_manager` binds parameters as positional Arrow `RecordBatch` columns, SQLSpec translates `:named` parameters to `$1` (`postgres`), `:1` (`oracle`), or `?` (`sqlite`, `duckdb`, `db2`) automatically.
- **DuckDB ADBC `COPY ... TO` Parameters**: DuckDB over ADBC only inlines a bound parameter in `COPY (...) TO :output_path` when `:output_path` is the statement's sole parameter. When combining `COPY` with filter parameters, set filter values via `SET VARIABLE cutoff = :val` beforehand and reference `getvariable('cutoff')` inside the query.
- **Enterprise Error & Savepoint Parity**: `AdbcDriver` translates Oracle `ORA-xxxxx` and Db2 `SQLCODE` errors into SQLSpec exceptions and emits dialect-native savepoints (`ON ROLLBACK RETAIN CURSORS` on Db2).
