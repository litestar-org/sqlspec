# Storage & Pipelines

```python
from sqlspec.storage import (
    AsyncStoragePipeline,
    AsyncWritableBytesSink,
    DirectStreamWriteProtocol,
    PartitionStrategyConfig,
    ResolvedStorageTarget,
    StorageBridgeJob,
    StorageCapabilities,
    StorageDestination,
    StorageFormat,
    StorageRegistry,
    StorageTelemetry,
    SyncStoragePipeline,
    WritableBytesSink,
    create_storage_bridge_job,
    encode_arrow_to_async_sink,
    encode_arrow_to_sink,
    get_storage_bridge_diagnostics,
    get_storage_bridge_metrics,
    reset_storage_bridge_metrics,
    resolve_storage_path,
    storage_registry,
    supports_direct_stream_write,
)
```

Multi-backend object storage registry (`local`, `fsspec`, `obstore`) and Apache Arrow / Parquet / JSONL streaming storage pipelines. See [Arrow & Storage](../learn/arrow-and-storage.md) for configuration and usage examples.

## Storage Registry

::: sqlspec.storage.registry.StorageRegistry
    options:
      heading_level: 3
      separate_signature: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_raises: false

::: sqlspec.storage.registry.storage_registry
    options:
      heading_level: 4

::: sqlspec.storage._paths.resolve_storage_path
    options:
      heading_level: 3
      separate_signature: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_raises: false

::: sqlspec.storage.pipeline.StorageDestination
    options:
      heading_level: 4

::: sqlspec.storage.pipeline.StorageFormat
    options:
      heading_level: 4

## Storage Pipelines

::: sqlspec.storage.pipeline.SyncStoragePipeline
    options:
      heading_level: 3
      separate_signature: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_raises: false

::: sqlspec.storage.pipeline.AsyncStoragePipeline
    options:
      heading_level: 3
      separate_signature: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_raises: false

## Bridge Jobs, Capabilities & Telemetry

::: sqlspec.storage.pipeline.StorageBridgeJob
    options:
      heading_level: 4
      separate_signature: false

::: sqlspec.storage.pipeline.StorageCapabilities
    options:
      heading_level: 4

::: sqlspec.storage.pipeline.PartitionStrategyConfig
    options:
      heading_level: 4

::: sqlspec.storage.pipeline.ResolvedStorageTarget
    options:
      heading_level: 4
      separate_signature: false

::: sqlspec.storage.pipeline.StorageTelemetry
    options:
      heading_level: 4

::: sqlspec.storage.pipeline.create_storage_bridge_job
    options:
      heading_level: 4
      separate_signature: false

::: sqlspec.storage.pipeline.get_storage_bridge_diagnostics
    options:
      heading_level: 4
      separate_signature: false

::: sqlspec.storage.pipeline.get_storage_bridge_metrics
    options:
      heading_level: 4
      separate_signature: false

::: sqlspec.storage.pipeline.reset_storage_bridge_metrics
    options:
      heading_level: 4
      separate_signature: false

## Direct Stream Writing & Arrow Sink Encoders

::: sqlspec.storage._arrow_payload.encode_arrow_to_sink
    options:
      heading_level: 4
      separate_signature: false

::: sqlspec.storage._arrow_payload.encode_arrow_to_async_sink
    options:
      heading_level: 4
      separate_signature: false

::: sqlspec.protocols.DirectStreamWriteProtocol
    options:
      heading_level: 4
      separate_signature: false

::: sqlspec.protocols.WritableBytesSink
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false
      show_docstring_returns: false

::: sqlspec.protocols.AsyncWritableBytesSink
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false
      show_docstring_returns: false

::: sqlspec.utils.type_guards.supports_direct_stream_write
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false
