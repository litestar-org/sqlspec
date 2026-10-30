# adbc Adapter

```python
from sqlspec.adapters.adbc import AdbcConfig, AdbcConnectionParams, AdbcDriver, AdbcDriverFeatures, AdbcExceptionHandler
```

Apache Arrow Database Connectivity (ADBC) adapter supporting zero-copy Arrow ingest and result streaming across PostgreSQL, SQLite, DuckDB, Snowflake, BigQuery, FlightSQL, GizmoSQL, Oracle, and IBM Db2. Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md). See the [ADBC Setup Guide](../../databases/adbc.md) for driver URI patterns and bulk Arrow ingestion.

## Configuration & Parameters

::: sqlspec.adapters.adbc.AdbcConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.adbc.AdbcConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.adbc.AdbcDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Driver & Pool

::: sqlspec.adapters.adbc.AdbcDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.adbc.AdbcExceptionHandler
    options:
      heading_level: 3
      members: false

## Extension Stores

::: sqlspec.adapters.adbc.litestar.ADBCStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.adbc.adk.AdbcADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.adbc.adk.AdbcADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.adbc.events.AdbcEventQueueStore
    options:
      heading_level: 4
      members: false
