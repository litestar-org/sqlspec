# ruff: noqa: PLR6301
"""Unified Storage Registry for ObjectStore backends.

This module provides a flexible, lazy-loading storage registry that supports:
- Two-backend architecture: ObStore (primary) and FSSpec (fallback)
- Object store terminology with base path support
- Smart protocol detection with automatic backend selection
- Lazy instantiation with caching
- Instrumentation integration
"""

import logging
from pathlib import Path
from typing import Any, Optional, TypeVar, Union

from sqlspec.storage.protocol import ObjectStoreProtocol

__all__ = ("StorageRegistry", "storage_registry")

logger = logging.getLogger(__name__)

BackendT = TypeVar("BackendT", bound=ObjectStoreProtocol)


class StorageRegistry:
    """Unified storage registry supporting sync and async operations.

    This registry implements the storage redesign plan with:
    - Two backends: ObStore (primary) and FSSpec (fallback/extended protocols)
    - Object store terminology consistent with cloud storage patterns
    - Base path support for different configurations
    - Smart protocol detection with automatic backend selection
    - Instrumentation integration for all operations

    Examples:
        # Register with URI and base path
        registry.register(
            "s3-data",
            uri="s3://my-bucket/data",
            base_path="sqlspec",
            aws_access_key_id="...",
            aws_secret_access_key="..."
        )

        # Register with explicit backend
        registry.register(
            "local-storage",
            backend=FSSpecBackend,
            uri="file:///tmp/storage",
            base_path="app"
        )

        # Get backend (lazy loaded)
        backend = registry.get("s3-data")

        # Resolve from URI with automatic fallback
        backend = registry.get("s3://my-bucket/file.parquet")  # Auto-creates ObStore backend
    """

    def __init__(self) -> None:
        self._backends: dict[str, tuple[type[ObjectStoreProtocol], dict]] = {}
        self._instances: dict[str, ObjectStoreProtocol] = {}
        self._scheme_mapping: dict[str, str] = {}
        self._default_backend: str = "obstore"

        # Register default schemes
        self._register_defaults()

    def _register_defaults(self) -> None:
        """Register default scheme mappings."""
        # ObStore protocols (preferred)
        for scheme in ("s3", "gs", "az", "file", "memory", "http", "https"):
            self._scheme_mapping[scheme] = "obstore"

        # FSSpec-only protocols (extended support)
        for scheme in ("ftp", "sftp", "webdav", "dropbox", "gdrive"):
            self._scheme_mapping[scheme] = "fsspec"

    def register(
        self,
        key: str,
        *,
        backend: Optional[type[ObjectStoreProtocol]] = None,
        uri: Optional[str] = None,
        base_path: str = "",
        config: Optional[dict] = None,
        **kwargs: Any,
    ) -> None:
        """Register a storage backend configuration.

        Args:
            key: Unique identifier for the backend
            backend: Backend class to use (auto-detected from URI if not provided)
            uri: Storage URI (e.g., "s3://bucket", "file:///path")
            base_path: Base path to prepend to all operations
            config: Additional configuration dict
            **kwargs: Backend-specific configuration options
        """
        if backend is None:
            # Auto-detect from URI
            if uri:
                scheme = uri.split("://")[0]
                backend_type = self._scheme_mapping.get(scheme, self._default_backend)
                backend = self._get_backend_class(backend_type)
            else:
                msg = "Either backend or uri must be provided"
                raise ValueError(msg)

        config = config or {}
        config.update(kwargs)
        config["base_path"] = base_path
        if uri:
            config["store"] = uri  # For ObStore
            config["fs"] = uri  # For FSSpec

        self._backends[key] = (backend, config)

    def get(self, key_or_uri: Union[str, Path]) -> ObjectStoreProtocol:
        """Get backend instance by key or URI.

        Args:
            key_or_uri: Either a registered key or a URI to resolve

        Returns:
            Backend instance

        Raises:
            KeyError: If key not found and URI cannot be resolved
        """
        # Handle Path objects
        if isinstance(key_or_uri, Path):
            key_or_uri = f"file://{key_or_uri.resolve()}"

        # Check if it's a registered key
        if key_or_uri in self._instances:
            return self._instances[key_or_uri]

        if key_or_uri in self._backends:
            # Lazy instantiation
            backend_cls, config = self._backends[key_or_uri]
            instance = backend_cls(**config)
            self._instances[key_or_uri] = instance
            return instance

        # Try to parse as URI
        if "://" in key_or_uri:
            return self._resolve_from_uri(key_or_uri)

        msg = f"No backend registered for '{key_or_uri}'"
        raise KeyError(msg)

    def register_scheme(self, scheme: str, backend: str = "obstore") -> None:
        """Register a URI scheme to backend mapping.

        Args:
            scheme: URI scheme (e.g., 's3', 'gs', 'file')
            backend: Backend type ('obstore' or 'fsspec')
        """
        self._scheme_mapping[scheme] = backend

    def _resolve_from_uri(self, uri: str) -> ObjectStoreProtocol:
        """Resolve backend from URI with intelligent fallback.

        Args:
            uri: URI to resolve backend for

        Returns:
            Backend instance

        Raises:
            KeyError: If no suitable backend can be created
        """
        scheme = uri.split("://")[0]
        backend_type = self._scheme_mapping.get(scheme)

        if not backend_type:
            # Try ObStore first, fall back to FSSpec
            try:
                return self._create_backend("obstore", uri)
            except Exception:
                try:
                    return self._create_backend("fsspec", uri)
                except Exception as e:
                    msg = f"Failed to create backend for URI '{uri}': {e}"
                    raise KeyError(msg) from e

        try:
            return self._create_backend(backend_type, uri)
        except Exception as e:
            # Try alternative backend type as fallback
            fallback_type = "fsspec" if backend_type == "obstore" else "obstore"
            try:
                logger.debug("Falling back from %s to %s for %s", backend_type, fallback_type, uri)
                return self._create_backend(fallback_type, uri)
            except Exception:
                msg = f"Failed to create backend for URI '{uri}': {e}"
                raise KeyError(msg) from e

    def _get_backend_class(self, backend_type: str) -> type[ObjectStoreProtocol]:
        """Get backend class by type name.

        Args:
            backend_type: Backend type ('obstore' or 'fsspec')

        Returns:
            Backend class

        Raises:
            ValueError: If unknown backend type
        """
        if backend_type == "obstore":
            from sqlspec.storage.backends.obstore import ObStoreBackend

            return ObStoreBackend
        if backend_type == "fsspec":
            from sqlspec.storage.backends.fsspec import FSSpecBackend

            return FSSpecBackend
        msg = f"Unknown backend type: {backend_type}"
        raise ValueError(msg)

    def _create_backend(self, backend_type: str, uri: str) -> ObjectStoreProtocol:
        """Create backend instance for URI.

        Args:
            backend_type: Backend type ('obstore' or 'fsspec')
            uri: URI to create backend for

        Returns:
            Backend instance
        """
        backend_cls = self._get_backend_class(backend_type)
        return backend_cls(uri)

    # Utility methods
    def is_registered(self, key: str) -> bool:
        """Check if a backend key is registered."""
        return key in self._backends

    def list_keys(self) -> list[str]:
        """List all registered backend keys."""
        return list(self._backends.keys())

    def list_schemes(self) -> dict[str, str]:
        """List all registered scheme mappings."""
        return self._scheme_mapping.copy()

    def clear_cache(self, key: Optional[str] = None) -> None:
        """Clear resolved backend cache.

        Args:
            key: Specific key to clear, or None to clear all
        """
        if key:
            self._instances.pop(key, None)
        else:
            self._instances.clear()


# Global registry instance
storage_registry = StorageRegistry()
