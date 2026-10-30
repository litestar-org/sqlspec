# Storage & Pipelines

```python
from sqlspec.storage import (
    AsyncStoragePipeline,
    PartitionStrategyConfig,
    ResolvedStorageTarget,
    StorageBridgeJob,
    StorageCapabilities,
    StorageDestination,
    StorageFormat,
    StorageRegistry,
    StorageTelemetry,
    SyncStoragePipeline,
    create_storage_bridge_job,
    resolve_storage_path,
    storage_registry,
)
```

Multi-backend object storage registry (`local`, `fsspec`, `obstore`) and Apache Arrow / Parquet / JSONL streaming storage pipelines. See [Arrow & Storage](../learn/arrow-and-storage.md) for configuration and usage examples.

## Storage Registry

::: sqlspec.storage.registry.StorageRegistry
    options:
      heading_level: 3

::: sqlspec.storage._paths.resolve_storage_path
    options:
      heading_level: 3

## Storage Pipelines

::: sqlspec.storage.pipeline.SyncStoragePipeline
    options:
      heading_level: 3

::: sqlspec.storage.pipeline.AsyncStoragePipeline
    options:
      heading_level: 3

## Bridge Jobs, Capabilities & Telemetry

::: sqlspec.storage.pipeline.StorageBridgeJob
    options:
      heading_level: 4

::: sqlspec.storage.pipeline.StorageCapabilities
    options:
      heading_level: 4

::: sqlspec.storage.pipeline.PartitionStrategyConfig
    options:
      heading_level: 4

::: sqlspec.storage.pipeline.ResolvedStorageTarget
    options:
      heading_level: 4

::: sqlspec.storage.pipeline.StorageTelemetry
    options:
      heading_level: 4

::: sqlspec.storage.pipeline.create_storage_bridge_job
    options:
      heading_level: 4

::: sqlspec.storage.pipeline.get_storage_bridge_diagnostics
    options:
      heading_level: 4

::: sqlspec.storage.pipeline.get_storage_bridge_metrics
    options:
      heading_level: 4

::: sqlspec.storage.pipeline.reset_storage_bridge_metrics
    options:
      heading_level: 4
