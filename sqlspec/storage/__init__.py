"""Storage abstraction layer for SQLSpec.

This module provides a flexible storage system with:
- Multiple backend support (local, fsspec, obstore)
- Lazy loading and configuration-based registration
- URI scheme-based automatic backend resolution
- Key-based named storage configurations
"""

from sqlspec.storage.protocol import StorageBackendProtocol
from sqlspec.storage.registry import StorageRegistry

storage_registry = StorageRegistry()

__all__ = (
    "StorageBackendProtocol",
    "StorageRegistry",
    "storage_registry",
)
