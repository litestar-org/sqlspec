# spanner Adapter

```python
from sqlspec.adapters.spanner import (
    SpannerAsyncConfig,
    SpannerAsyncDriver,
    SpannerAsyncExceptionHandler,
    SpannerConnectionParams,
    SpannerDriverFeatures,
    SpannerPoolParams,
    SpannerSyncConfig,
    SpannerSyncDriver,
    SpannerSyncExceptionHandler,
)
```

Synchronous and asynchronous Google Cloud Spanner adapters supporting both GoogleSQL (`spanner`) and PostgreSQL (`spangres`) dialects, multiplexed sessions, read-only timestamp snapshots, and CommitResponse mutation tracking. Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md) and [`AsyncDriverAdapterBase`](../driver/async.md). See the [Cloud Spanner Setup Guide](../../databases/spanner.md) for usage examples and the [Spanner (GoogleSQL)](../data-dictionary/dialects/spanner.md) and [Spangres (PostgreSQL)](../data-dictionary/dialects/index.md) Data Dictionary SQL Catalogs.

## Configuration & Parameters

::: sqlspec.adapters.spanner.SpannerSyncConfig
    options:
      heading_level: 3

::: sqlspec.adapters.spanner.SpannerAsyncConfig
    options:
      heading_level: 3

::: sqlspec.adapters.spanner.SpannerConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.spanner.SpannerPoolParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.spanner.SpannerDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Driver, Pool & Data Dictionary

::: sqlspec.adapters.spanner.SpannerSyncDriver
    options:
      heading_level: 3
      members:
        - run_in_transaction
        - execute_partitioned_dml
        - write_insert
        - write_upsert
        - write_update
        - write_delete
        - bind_external_transaction

::: sqlspec.adapters.spanner.SpannerAsyncDriver
    options:
      heading_level: 3
      members:
        - run_in_transaction
        - execute_partitioned_dml
        - write_insert
        - write_upsert
        - write_update
        - write_delete
        - bind_external_transaction

::: sqlspec.adapters.spanner.SpannerSyncExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.spanner.SpannerAsyncExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.spanner.data_dictionary.SpannerSyncDataDictionary
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.spanner.data_dictionary.SpannerAsyncDataDictionary
    options:
      heading_level: 3
      members: false

## Extension Stores & Configs

::: sqlspec.adapters.spanner.litestar.SpannerLitestarConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.spanner.litestar.SpannerSyncStore
    options:
      heading_level: 4

::: sqlspec.adapters.spanner.litestar.SpannerAsyncStore
    options:
      heading_level: 4

::: sqlspec.adapters.spanner.adk.SpannerADKConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.spanner.adk.SpannerADKRetentionConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.spanner.adk.SpannerSyncADKStore
    options:
      heading_level: 4

::: sqlspec.adapters.spanner.adk.SpannerAsyncADKStore
    options:
      heading_level: 4

::: sqlspec.adapters.spanner.adk.SpannerSyncADKMemoryStore
    options:
      heading_level: 4

::: sqlspec.adapters.spanner.adk.SpannerAsyncADKMemoryStore
    options:
      heading_level: 4

::: sqlspec.adapters.spanner.adk.SpannerSyncADKArtifactStore
    options:
      heading_level: 4

::: sqlspec.adapters.spanner.adk.SpannerAsyncADKArtifactStore
    options:
      heading_level: 4

::: sqlspec.adapters.spanner.events.SpannerSyncEventQueueStore
    options:
      heading_level: 4

::: sqlspec.adapters.spanner.events.SpannerAsyncEventQueueStore
    options:
      heading_level: 4
