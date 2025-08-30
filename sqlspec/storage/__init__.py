"""Storage abstraction layer for SQLSpec.

Provides a storage system with:
- Multiple backend support (local, fsspec, obstore)
- Configuration-based registration
- URI scheme-based backend resolution
- Named storage configurations
- Capability-based backend selection
"""

from sqlspec.storage.registry import StorageRegistry, storage_registry

__all__ = ("StorageRegistry", "storage_registry")
