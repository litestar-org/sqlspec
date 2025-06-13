# ruff: noqa: PLR6301
"""Unified Storage Registry for ObjectStore backends.

This module provides a flexible, lazy-loading storage registry that supports:
- URI-first access pattern with automatic backend detection
- ObStore preferred, FSSpec fallback architecture
- Intelligent scheme-based routing with dependency detection
- Named aliases for commonly used configurations (secondary feature)
- Automatic instrumentation integration
"""

# TODO: TRY300 - Review try-except patterns for else block opportunities
import logging
from pathlib import Path
from typing import Any, Optional, TypeVar, Union, cast

from sqlspec.storage.protocol import ObjectStoreProtocol
from sqlspec.typing import FSSPEC_INSTALLED, OBSTORE_INSTALLED

__all__ = ("StorageRegistry", "storage_registry")

logger = logging.getLogger(__name__)

BackendT = TypeVar("BackendT", bound=ObjectStoreProtocol)


class StorageRegistry:
    """Unified storage registry with URI-first access and intelligent backend selection.

    This registry implements Phase 3 of the unified storage redesign:
    - URI-first access pattern - pass URIs directly to get()
    - Automatic ObStore preference when available
    - Intelligent FSSpec fallback for unsupported schemes or when ObStore unavailable
    - Named aliases as secondary feature for commonly used configurations
    - Dependency-aware backend selection with clear error messages

    Examples:
        # Primary usage: Direct URI access (no registration needed)
        backend = registry.get("s3://my-bucket/file.parquet")    # ObStore preferred
        backend = registry.get("file:///tmp/data.csv")          # Obstore for local files
        backend = registry.get("gs://bucket/data.json")         # ObStore for GCS

        # Secondary usage: Named aliases for complex configurations
        registry.register_alias(
            "production-s3",
            uri="s3://prod-bucket/data",
            base_path="sqlspec",
            aws_access_key_id="...",
            aws_secret_access_key="..."
        )
        backend = registry.get("production-s3")  # Uses alias

        # Automatic fallback when ObStore unavailable
        # If obstore not installed: s3:// → FSSpec automatically
        # Clear error if neither backend supports the scheme
    """

    def __init__(self) -> None:
        # Named aliases (secondary feature)
        self._aliases: dict[str, tuple[type[ObjectStoreProtocol], dict[str, Any]]] = {}
        self._instances: dict[str, ObjectStoreProtocol] = {}

    def register_alias(
        self,
        alias: str,
        uri: str,
        *,
        backend: Optional[type[ObjectStoreProtocol]] = None,
        base_path: str = "",
        config: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Register a named alias for a storage configuration.

        Args:
            alias: Unique alias name for the configuration
            uri: Storage URI (e.g., "s3://bucket", "file:///path")
            backend: Backend class to use (auto-detected from URI if not provided)
            base_path: Base path to prepend to all operations
            config: Additional configuration dict
            **kwargs: Backend-specific configuration options
        """
        if backend is None:
            # Auto-detect from URI using new intelligent selection
            backend = self._determine_backend_class(uri)

        config = config or {}
        config.update(kwargs)
        config["base_path"] = base_path

        # Set URI for both backend types
        if "obstore" in backend.__module__:
            config["store_uri"] = uri
        else:
            config["uri"] = uri

        self._aliases[alias] = (backend, config)

    def get(self, uri_or_alias: Union[str, Path]) -> ObjectStoreProtocol:
        """Get backend instance using URI-first routing with intelligent backend selection.

        Args:
            uri_or_alias: URI to resolve directly OR named alias (secondary feature)

        Returns:
            Backend instance with automatic ObStore preference and FSSpec fallback

        Raises:
            KeyError: If alias not found and URI cannot be resolved
        """
        # Handle Path objects - convert to file:// URI
        if isinstance(uri_or_alias, Path):
            uri_or_alias = f"file://{uri_or_alias.resolve()}"

        # Check cache first
        if uri_or_alias in self._instances:
            return self._instances[uri_or_alias]

        # PRIMARY: Try URI-first routing
        if "://" in uri_or_alias:
            backend = self._resolve_from_uri(uri_or_alias)
            # Cache the instance for future use
            self._instances[uri_or_alias] = backend
            return backend

        # SECONDARY: Check if it's a registered alias
        if uri_or_alias in self._aliases:
            backend_cls, config = self._aliases[uri_or_alias]
            instance = backend_cls(**config)
            self._instances[uri_or_alias] = instance
            return instance

        # Not a URI and not an alias
        msg = f"No backend available for '{uri_or_alias}'. Use a valid URI (e.g., 's3://bucket/path') or register an alias."
        raise KeyError(msg)

    def _resolve_from_uri(self, uri: str) -> ObjectStoreProtocol:
        """Resolve backend from URI.

        Tries ObStore first, then falls back to FSSpec.

        Args:
            uri: URI to resolve backend for

        Returns:
            Backend instance

        Raises:
            KeyError: If no suitable backend can be created
        """
        last_exc: Optional[Exception] = None
        if OBSTORE_INSTALLED:
            try:
                return self._create_backend("obstore", uri)
            except (ImportError, ValueError) as e:
                logger.debug("ObStore backend failed for %s: %s", uri, e)
                last_exc = e

        if FSSPEC_INSTALLED:
            try:
                return self._create_backend("fsspec", uri)
            except (ImportError, ValueError) as e:
                logger.debug("FSSpec backend failed for %s: %s", uri, e)
                last_exc = e

        msg = f"No backend available for URI '{uri}'. Install 'obstore' or 'fsspec' and ensure dependencies for your filesystem are installed."
        raise KeyError(msg) from last_exc

    def _determine_backend_class(self, uri: str) -> type[ObjectStoreProtocol]:
        """Determine the best backend class for a URI based on availability.

        Prefers ObStore, falls back to FSSpec.

        Args:
            uri: URI to determine backend for.

        Returns:
            Backend class (not instance)

        Raises:
            ValueError: If no suitable backend is available
        """
        if OBSTORE_INSTALLED:
            return self._get_backend_class("obstore")
        if FSSPEC_INSTALLED:
            return self._get_backend_class("fsspec")

        scheme = uri.split("://", maxsplit=1)[0].lower()
        msg = f"No backend available for URI scheme '{scheme}'. Install obstore or fsspec."
        raise ValueError(msg)

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

            return cast("type[ObjectStoreProtocol]", ObStoreBackend)
        if backend_type == "fsspec":
            from sqlspec.storage.backends.fsspec import FSSpecBackend

            return cast("type[ObjectStoreProtocol]", FSSpecBackend)
        msg = f"Unknown backend type: {backend_type}. Supported types: 'obstore', 'fsspec'"
        raise ValueError(msg)

    def _create_backend(self, backend_type: str, uri: str) -> ObjectStoreProtocol:
        """Create backend instance for URI.

        Args:
            backend_type: Backend type ('obstore' or 'fsspec')
            uri: URI to create backend for

        Returns:
            Backend instance
        """
        return self._get_backend_class(backend_type)(uri)

    # Utility methods
    def is_alias_registered(self, alias: str) -> bool:
        """Check if a named alias is registered."""
        return alias in self._aliases

    def list_aliases(self) -> list[str]:
        """List all registered aliases."""
        return list(self._aliases.keys())

    def clear_cache(self, uri_or_alias: Optional[str] = None) -> None:
        """Clear resolved backend cache.

        Args:
            uri_or_alias: Specific URI or alias to clear, or None to clear all
        """
        if uri_or_alias:
            self._instances.pop(uri_or_alias, None)
        else:
            self._instances.clear()


# Global registry instance
storage_registry = StorageRegistry()
