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

Synchronous and asynchronous Google Cloud Spanner adapters supporting both GoogleSQL (`spanner`) and PostgreSQL (`spangres`) dialects, multiplexed sessions, read-only timestamp snapshots, and CommitResponse mutation tracking. Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md) and [`AsyncDriverAdapterBase`](../driver/async.md). See the [Cloud Spanner Setup Guide](../../databases/spanner.md) for usage examples.

## Configuration & Parameters

::: sqlspec.adapters.spanner.SpannerSyncConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.spanner.SpannerAsyncConfig
    options:
      heading_level: 3
      members: false

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

## Driver & Pool

::: sqlspec.adapters.spanner.SpannerSyncDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.spanner.SpannerAsyncDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.spanner.SpannerSyncExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.spanner.SpannerAsyncExceptionHandler
    options:
      heading_level: 3
      members: false

## Extension Stores

::: sqlspec.adapters.spanner.litestar.SpannerSyncStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.spanner.litestar.SpannerAsyncStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.spanner.adk.SpannerSyncADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.spanner.adk.SpannerAsyncADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.spanner.adk.SpannerSyncADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.spanner.adk.SpannerAsyncADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.spanner.adk.SpannerSyncADKArtifactStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.spanner.adk.SpannerAsyncADKArtifactStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.spanner.events.SpannerSyncEventQueueStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.spanner.events.SpannerAsyncEventQueueStore
    options:
      heading_level: 4
      members: false
