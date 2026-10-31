# duckdb Adapter

```python
from sqlspec.adapters.duckdb import (
    DuckDBConfig,
    DuckDBConnectionParams,
    DuckDBConnectionPool,
    DuckDBDriver,
    DuckDBDriverFeatures,
    DuckDBExceptionHandler,
    DuckDBExtensionConfig,
    DuckDBPoolParams,
    DuckDBSecretConfig,
)
```

Embedded analytical database adapter built on `duckdb` with zero-copy Apache Arrow, Parquet, and Polars integration. Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md). See the [DuckDB Setup Guide](../../databases/duckdb.md) for extensions, secrets, and Arrow streaming examples, and the [DuckDB Data Dictionary & SQL Catalog](../data-dictionary/dialects/duckdb.md).

## Configuration & Parameters

::: sqlspec.adapters.duckdb.DuckDBConfig
    options:
      heading_level: 3

::: sqlspec.adapters.duckdb.DuckDBConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.duckdb.DuckDBPoolParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.duckdb.DuckDBDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.duckdb.DuckDBExtensionConfig
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.duckdb.DuckDBSecretConfig
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Driver, Pool & Data Dictionary

::: sqlspec.adapters.duckdb.DuckDBDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.duckdb.DuckDBExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.duckdb.DuckDBConnectionPool
    options:
      heading_level: 3

::: sqlspec.adapters.duckdb.data_dictionary.DuckDBDataDictionary
    options:
      heading_level: 3
      members: false

## Extension Stores & Configs

::: sqlspec.adapters.duckdb.litestar.DuckdbStore
    options:
      heading_level: 4

::: sqlspec.adapters.duckdb.adk.DuckdbADKConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.duckdb.adk.DuckdbADKFTSOptions
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.duckdb.adk.DuckdbADKStore
    options:
      heading_level: 4

::: sqlspec.adapters.duckdb.adk.DuckdbADKMemoryStore
    options:
      heading_level: 4

::: sqlspec.adapters.duckdb.events.DuckDBEventQueueStore
    options:
      heading_level: 4
