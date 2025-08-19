"""Storage abstraction layer for SQLSpec.

Provides a storage system with:
- Multiple backend support (local, fsspec, obstore)
- Configuration-based registration
- URI scheme-based backend resolution
- Named storage configurations
- Capability-based backend selection
"""

from sqlspec.protocols import ObjectStoreProtocol
from sqlspec.storage.capabilities import HasStorageCapabilities, StorageCapabilities
from sqlspec.storage.registry import StorageRegistry

storage_registry = StorageRegistry()

__all__ = (
    "HasStorageCapabilities",
    "ObjectStoreProtocol",
    "StorageCapabilities",
    "StorageRegistry",
    "storage_registry",
)
