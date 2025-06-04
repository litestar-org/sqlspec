from typing import Any

from sqlspec.exceptions import SQLSpecError
from sqlspec.storage.protocol import StorageBackendProtocol


class BackendNotRegisteredError(SQLSpecError):
    """Raised when a requested storage backend key is not registered."""


class StorageRegistry:
    """Registry for managing and retrieving pre-configured storage backend instances by key."""

    def __init__(self) -> None:
        self._backends: dict[str, StorageBackendProtocol] = {}

    def register_backend(self, key: str, backend_instance: StorageBackendProtocol) -> None:
        """Register a pre-configured backend instance with a unique key."""
        if not isinstance(key, str) or not key:
            msg = "Backend key must be a non-empty string."
            raise TypeError(msg)
        if not isinstance(backend_instance, StorageBackendProtocol):
            msg = "Backend instance must implement StorageBackendProtocol."
            raise TypeError(msg)
        if key in self._backends:
            # Consider if warning or error is more appropriate, or allow overwrite via a flag
            # For now, let's be strict to avoid accidental overwrites.
            msg = f"Backend key '{key}' is already registered."
            raise ValueError(msg)
        self._backends[key] = backend_instance

    def get_backend(self, key: str) -> StorageBackendProtocol:
        """Retrieve a registered backend instance by its key."""
        try:
            return self._backends[key]
        except KeyError:
            msg = f"No storage backend registered for key: '{key}'"
            raise BackendNotRegisteredError(msg) from None

    def unregister_backend(self, key: str) -> None:
        """Unregister a backend instance by its key."""
        if key not in self._backends:
            msg = f"Backend key '{key}' not found for unregistration."
            raise BackendNotRegisteredError(msg)
        del self._backends[key]

    def is_registered(self, key: str) -> bool:
        """Check if a backend key is registered."""
        return key in self._backends

    def list_registered_keys(self) -> list[str]:
        """List all registered backend keys."""
        return list(self._backends.keys())

    def register_from_config(self, key: str, config: "dict[str, Any]") -> None:
        """Register a backend from configuration dictionary."""
        backend_type = config.get("backend_type")
        if not backend_type:
            msg = "Configuration must specify 'backend_type'"
            raise ValueError(msg)
        backend_instance = self._create_backend_from_config(backend_type, config)
        self.register_backend(key, backend_instance)

    def _create_backend_from_config(self, backend_type: str, config: "dict[str, Any]") -> StorageBackendProtocol:
        """Create a backend instance from configuration."""
        if backend_type == "obstore":
            from sqlspec.storage.backends.obstore import ObstoreBackend

            return ObstoreBackend.from_config(config)
        if backend_type == "fsspec":
            from sqlspec.storage.backends.fsspec import FsspecBackend

            return FsspecBackend.from_config(config)
        if backend_type == "local":
            from sqlspec.storage.backends.file import LocalFileBackend

            return LocalFileBackend.from_config(config)
        msg = f"Unknown backend type: {backend_type}"
        raise ValueError(msg)


# Global default registry instance
storage_registry = StorageRegistry()
