# Oracle Database

`sqlspec.adapters.oracledb` wraps `python-oracledb` (`>= 3.4`) across Oracle Database 11g through 26ai, supporting synchronous (`OracleSyncConfig`) and `asyncio` (`OracleAsyncConfig`) pools, zero-client Thin mode, Instant Client Thick mode, mTLS/PKCS#12 wallets, TNS topologies, Oracle 26ai `VECTOR` and Hybrid RRF search, PyCapsule Arrow streaming, and server-side `DBMS_CLOUD.COPY_DATA`.

## Install and When to Choose

=== "uv"

    ```bash
    uv add "sqlspec[oracledb]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[oracledb]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[oracledb]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[oracledb]"
    ```

`python-oracledb` runs in pure-Python/Cython **Thin mode** by default with zero Oracle Instant Client dependencies. Enable **Thick mode** only when connecting to legacy encryption suites, Kerberos/Radius OS authentication, or Continuous Query Notification features requiring Oracle Client libraries.

## Minimal Connection and Pool Setup

=== "Async (`OracleAsyncConfig`)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.oracledb import OracleAsyncConfig

    spec = SQLSpec()
    config = spec.add_config(
        OracleAsyncConfig(
            connection_config={
                "user": "app",
                "password": "secret",
                "dsn": "localhost:1521/FREEPDB1",
                "min": 2,
                "max": 20,
            },
            driver_features={
                "enable_lowercase_column_names": True,
                "enable_session_tracing": True,
            },
        )
    )
    ```

=== "Sync (`OracleSyncConfig`)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.oracledb import OracleSyncConfig

    spec = SQLSpec()
    config = spec.add_config(
        OracleSyncConfig(
            connection_config={
                "user": "app",
                "password": "secret",
                "dsn": "localhost:1521/FREEPDB1",
                "min": 2,
                "max": 10,
            }
        )
    )
    ```

### Autonomous Database mTLS Wallets and TNS Topologies

Pass `.zip` archives or extracted wallet directories directly via `wallet_location` (or extract PKCS#12 `ewallet.p12` bundles with `extract_oracle_wallet_zip` when `cryptography` is installed). Process-isolated `tns_alias` and `config_dir` resolution parses `tnsnames.ora` and `sqlnet.ora` without mutating `os.environ["TNS_ADMIN"]`:

```python
from sqlspec.adapters.oracledb import OracleAsyncConfig

adb_config = OracleAsyncConfig(
    connection_config={
        "user": "admin",
        "password": "secret",
        "tns_alias": "mydb_high",
        "config_dir": "/etc/oracle/wallet",
        "wallet_location": "/etc/oracle/wallet",
        "wallet_password": "wallet_secret",
    }
)
```

## Key Driver Features and Pool Tuning

| Setting | Location | Description |
| --- | --- | --- |
| `dsn` / `host`, `port`, `service_name` / `tns_alias` | `connection_config` | Easy Connect string, discrete endpoint fields, or `tnsnames.ora` alias. |
| `min` / `max` / `increment` | `connection_config` | `ConnectionPool` / `AsyncConnectionPool` bounds (compatible with Database Resident Connection Pooling `cclass` / `purity`). |
| `thick_mode` / `lib_dir` | Config kwargs | Initializes Oracle Instant Client Thick mode when explicitly configured. |
| `enable_lowercase_column_names` | `driver_features` | Lowercases implicit uppercase column names (`ID -> id`) while preserving quoted mixed-case aliases (`True` by default). |
| `enable_uuid_binary` | `driver_features` | Coerces Python `uuid.UUID` values cleanly to and from `RAW(16)` binary columns. |
| `enable_numpy_vectors` / `vector_return_format` | `driver_features` | Controls zero-copy `memoryview` / `array.array` / NumPy conversion for Oracle 26ai `VECTOR` and `SparseVector` columns. |
| `enable_session_tracing` | `driver_features` | Piggybacks `module`, `action`, `client_identifier`, and `econtext_id` onto the next TTC packet in zero extra round-trips. |
| `dbms_cloud_credential` | `driver_features` | Enables server-side `DBMS_CLOUD.COPY_DATA` Parquet/JSON/CSV object-store loads in `load_from_storage`. |
| `arraysize` / `prefetchrows` | `driver_features` | Tunes TTC row prefetch batching across `select`, `select_stream`, and Arrow fetches. |

See [OracleDB API Reference](/reference/adapters/oracledb/) for `OracleConnectionParams`, `OraclePoolParams`, `OracleDriverFeatures`, `OracleTopologyConfig`, and `OracleSessionContextConfig`.

## Type Mapping and Dialect Notes

- **Version-Aware JSON Storage**: Oracle 21c+ and 26ai bind Python dicts/lists via native binary OSON (`DB_TYPE_JSON`), while pre-21c connections route JSON automatically to `CLOB`/`BLOB` converters using idempotent type handlers.
- **Oracle 26ai Vectors, HNSW, and Hybrid RRF**: `OracleAsyncADKMemoryStore` and `OracleSyncADKMemoryStore` isolate HNSW `VECTOR_DISTANCE` subqueries so the Cost-Based Optimizer uses approximate vector indexes cleanly, combining vector ranks and Oracle Text `CONTAINS` / `NEAR((...))` scores via single-round-trip Reciprocal Rank Fusion.
- **Zero-Copy PyCapsule Arrow Streaming**: `session.select_to_arrow(..., return_format="batches")` streams `RecordBatch` chunks directly via `pa.RecordBatch.from_pyarrow(...)` without intermediate row dicts.
- **Oracle AQ Events**: `EventChannel` supports both transactional table queues and native Oracle Advanced Queuing (`DBMS_AQ`) via `driver_features={"events_backend": "aq"}`.
