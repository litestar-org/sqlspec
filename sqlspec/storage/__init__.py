"""Storage abstraction layer for SQLSpec.

Provides a storage system with:
    - Multiple backend support (local, fsspec, obstore)
    - Configuration-based registration
    - URI scheme-based backend resolution
    - Named storage configurations
    - Capability-based backend selection
"""

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlspec.storage._paths import resolve_storage_path
    from sqlspec.storage.pipeline import (
        AsyncStoragePipeline,
        PartitionStrategyConfig,
        ResolvedStorageTarget,
        StorageBridgeJob,
        StorageCapabilities,
        StorageDestination,
        StorageFormat,
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
    "ResolvedStorageTarget",
    "StorageBridgeJob",
    "StorageCapabilities",
    "StorageDestination",
    "StorageFormat",
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

_EXPORTS: dict[str, tuple[str, str]] = {
    "AsyncStoragePipeline": ("sqlspec.storage.pipeline", "AsyncStoragePipeline"),
    "PartitionStrategyConfig": ("sqlspec.storage.pipeline", "PartitionStrategyConfig"),
    "ResolvedStorageTarget": ("sqlspec.storage.pipeline", "ResolvedStorageTarget"),
    "StorageBridgeJob": ("sqlspec.storage.pipeline", "StorageBridgeJob"),
    "StorageCapabilities": ("sqlspec.storage.pipeline", "StorageCapabilities"),
    "StorageDestination": ("sqlspec.storage.pipeline", "StorageDestination"),
    "StorageFormat": ("sqlspec.storage.pipeline", "StorageFormat"),
    "StorageRegistry": ("sqlspec.storage.registry", "StorageRegistry"),
    "StorageTelemetry": ("sqlspec.storage.pipeline", "StorageTelemetry"),
    "SyncStoragePipeline": ("sqlspec.storage.pipeline", "SyncStoragePipeline"),
    "create_storage_bridge_job": ("sqlspec.storage.pipeline", "create_storage_bridge_job"),
    "get_storage_bridge_diagnostics": ("sqlspec.storage.pipeline", "get_storage_bridge_diagnostics"),
    "get_storage_bridge_metrics": ("sqlspec.storage.pipeline", "get_storage_bridge_metrics"),
    "reset_storage_bridge_metrics": ("sqlspec.storage.pipeline", "reset_storage_bridge_metrics"),
    "resolve_storage_path": ("sqlspec.storage._paths", "resolve_storage_path"),
    "storage_registry": ("sqlspec.storage.registry", "storage_registry"),
}

_COMPILED = getattr(__spec__, "origin", "").endswith((".so", ".pyd"))


def __getattr__(name: str) -> Any:
    """Lazily resolve package exports via PEP 562."""
    target = _EXPORTS.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_path, attr_name = target
    value = getattr(import_module(module_path), attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Return sorted module attributes including lazy exports."""
    return sorted({*globals(), *__all__})


if _COMPILED:
    for _export_name in __all__:
        __getattr__(_export_name)
