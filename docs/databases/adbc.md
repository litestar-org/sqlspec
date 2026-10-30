# Apache Arrow ADBC

`sqlspec.adapters.adbc` integrates Apache Arrow Database Connectivity (`adbc-driver-manager` + `pyarrow`), exchanging query results and bulk table loads directly over the C-level Arrow C Data Interface (`__arrow_c_stream__`) without row-by-row Python object marshaling.

## Install and When to Choose

Install the core `adbc` extra alongside one or more PyPI-installable ADBC wire drivers:

```bash
uv add "sqlspec[adbc]" adbc-driver-postgresql
```

| Target Engine | PyPI Package | Supported URI Schemes | Short `driver` Aliases |
| --- | --- | --- | --- |
| PostgreSQL / AlloyDB | `adbc-driver-postgresql` | `postgresql://`, `postgres://` | `"postgres"`, `"postgresql"`, `"pg"` |
| SQLite | `adbc-driver-sqlite` | `sqlite://`, `file:app.db`, `":memory:"` | `"sqlite"`, `"sqlite3"` |
| DuckDB (In-Process) | `duckdb` | `duckdb://`, `duckdb:///path.db` | `"duckdb"` |
| DuckDB Quack (Remote gRPC) | `adbc-driver-quack` | `quack+tls://`, `quack://` | `"quack"` |
| GizmoSQL / Arrow FlightSQL | `adbc-driver-gizmosql` or `adbc-driver-flightsql` | `grpc+tls://`, `grpc://`, `gizmosql://` | `"flightsql"`, `"gizmosql"`, `"grpc"`, `"gizmo"` |
| Snowflake | `adbc-driver-snowflake` | `snowflake://` | `"snowflake"`, `"sf"` |
| Google Cloud BigQuery | `adbc-driver-bigquery` | `bigquery://` | `"bigquery"`, `"bq"` |
| Oracle Database | `adbc-driver-oracle` | `oracle://` (pure Go wire driver) | `"oracle"`, `"oracledb"` |
| IBM Db2 | `adbc-driver-db2` | `db2://` (pure Go DRDA wire driver) | `"db2"`, `"ibmdb2"`, `"ibm_db"` |

### Driver Resolution Rules

SQLSpec resolves the underlying ADBC DB-API driver in three ways:

1. **Automatic URI Scheme Resolution (Zero Driver Configuration)**:
   Every backend supports top-level scheme detection (`duckdb://`, `snowflake://`,
   `bigquery://`, `postgresql://`, `sqlite://`, `oracle://`, `db2://`,
   `quack+tls://`, `grpc+tls://`). When `uri` contains a recognized scheme,
   SQLSpec automatically binds the driver without requiring `driver` or
   `driver_name`:
   ```python
   # Driver inferred automatically from URI scheme:
   connection_config = {"uri": "duckdb:///data/analytics.duckdb"}
   connection_config = {"uri": "snowflake://app:secret@org-account/db/schema?warehouse=wh"}
   connection_config = {"uri": "bigquery://my-project/my_dataset"}
   ```

2. **Short `driver` Aliases (Recommended for File Paths & Parameter Configs)**:
   When the connection does not specify a top-level URI scheme (such as local
   file paths or cloud credentials passed via dictionary parameters), set the
   short `driver` parameter to the engine name:
   ```python
   # DuckDB in-process file:
   connection_config = {"driver": "duckdb", "path": "analytics.duckdb"}

   # Snowflake warehouse credentials:
   connection_config = {
       "driver": "snowflake",
       "db_kwargs": {"adbc.snowflake.sql.account": "my_org-my_account", "adbc.snowflake.sql.warehouse": "COMPUTE_WH"},
   }

   # BigQuery project configuration:
   connection_config = {"driver": "bigquery", "project_id": "my-project", "dataset_id": "analytics"}
   ```

3. **`driver` vs. `driver_name`**:
   - `driver`: The primary, idiomatic parameter in `AdbcConnectionParams` for
     short aliases (`driver="duckdb"`, `driver="snowflake"`,
     `driver="bigquery"`).
   - `driver_name`: An accepted alias for `driver`. In code, SQLSpec checks
     `config.get("driver_name") or config.get("driver")` equivalently.
     `driver_name` is traditionally used when passing the full PyPI module name
     (`driver_name="adbc_driver_snowflake"`) or a filesystem path to a compiled
     C shared object (`driver_name="/opt/drivers/libadbc_driver_sqlite.so"`).

Choose `AdbcConfig` when your primary workload extracts or ingests
Arrow/Polars/Pandas/Parquet datasets across analytical warehouses or when you
need pure-Go zero-client wire drivers for Oracle or Db2 on Linux ARM64.

## Minimal Connection and Pool Setup

```python
from sqlspec import SQLSpec, StatementConfig
from sqlspec.adapters.adbc import AdbcConfig

spec = SQLSpec()
config = spec.add_config(
    AdbcConfig(
        connection_config={"uri": "postgresql://app:secret@localhost:5432/prod"},
        statement_config=StatementConfig(dialect="postgres"),
    )
)

with spec.provide_session(config) as session:
    arrow_result = session.select_to_arrow("SELECT id, total_cents FROM orders WHERE status = :s", s="paid")
```

## Key Driver Features and Pool Tuning

| Setting | Location | Description |
| --- | --- | --- |
| `driver` / `driver_name` / `uri` | `connection_config` | Short driver alias (`driver="duckdb"`, `driver="snowflake"`, `driver="bigquery"`), explicit module name (`driver_name="adbc_driver_duckdb"`), or connection URI with auto-detected scheme (`duckdb://`, `snowflake://`, `bigquery://`, `postgresql://`, `oracle://`, `db2://`, `quack://`, `grpc+tls://`). |
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
