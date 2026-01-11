"""Storage abstraction layer for SQLSpec.

Provides a storage system with:
- Multiple backend support (local, fsspec, obstore)
- Configuration-based registration
- URI scheme-based backend resolution
- Named storage configurations
- Capability-based backend selection
"""

from sqlspec.storage._utils import resolve_storage_path
from sqlspec.storage.pipeline import (
    AsyncStoragePipeline,
    PartitionStrategyConfig,
    StagedArtifact,
    StorageBridgeJob,
    StorageCapabilities,
    StorageDestination,
    StorageFormat,
    StorageLoadRequest,
    StorageTelemetry,
    SyncStoragePipeline,
    create_storage_bridge_job,
    get_storage_bridge_diagnostics,
    get_storage_bridge_metrics,
    reset_storage_bridge_metrics,
)
from sqlspec.storage.registry import StorageRegistry, storage_registry

__all__ = (
    "AsyncStoragePipeline",
    "PartitionStrategyConfig",
    "StagedArtifact",
    "StorageBridgeJob",
    "StorageCapabilities",
    "StorageDestination",
    "StorageFormat",
    "StorageLoadRequest",
    "StorageRegistry",
    "StorageTelemetry",
    "SyncStoragePipeline",
    "create_storage_bridge_job",
    "get_storage_bridge_diagnostics",
    "get_storage_bridge_metrics",
    "reset_storage_bridge_metrics",
    "resolve_storage_path",
    "storage_registry",
)
