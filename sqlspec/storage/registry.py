"""Unified Storage Registry for ObjectStore backends.

This module provides a flexible, lazy-loading storage registry that supports:
- URI-first access pattern with automatic backend detection
- ObStore preferred, FSSpec fallback architecture
- Intelligent scheme-based routing with dependency detection
- Named aliases for commonly used configurations (secondary feature)
- Automatic instrumentation integration
"""

import logging
from pathlib import Path
from typing import Any, Optional, TypeVar, Union, cast

from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError
from sqlspec.protocols import ObjectStoreProtocol
from sqlspec.storage.capabilities import StorageCapabilities
from sqlspec.typing import FSSPEC_INSTALLED, OBSTORE_INSTALLED

__all__ = ("StorageRegistry", "storage_registry")

logger = logging.getLogger(__name__)

BackendT = TypeVar("BackendT", bound=ObjectStoreProtocol)

FSSPEC_ONLY_SCHEMES = {"http", "https", "ftp", "sftp", "ssh"}


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
        self._alias_configs: dict[str, tuple[type[ObjectStoreProtocol], str, dict[str, Any]]] = {}
        self._aliases: dict[str, dict[str, Any]] = {}
        self._instances: dict[Union[str, tuple[str, tuple[tuple[str, Any], ...]]], ObjectStoreProtocol] = {}

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
            backend = self._determine_backend_class(uri)

        config = config or {}
        config.update(kwargs)

        backend_config = dict(config)
        if base_path:
            backend_config["base_path"] = base_path

        self._alias_configs[alias] = (backend, uri, backend_config)

        test_config = dict(backend_config)
        test_config["uri"] = uri
        self._aliases[alias] = test_config

    def get(self, uri_or_alias: Union[str, Path], **kwargs: Any) -> ObjectStoreProtocol:
        """Get backend instance using URI-first routing with intelligent backend selection.

        Args:
            uri_or_alias: URI to resolve directly OR named alias (secondary feature)
            **kwargs: Additional backend-specific configuration options

        Returns:
            Backend instance with automatic ObStore preference and FSSpec fallback

        Raises:
            ImproperConfigurationError: If alias not found or invalid input
        """
        if not uri_or_alias:
            msg = "URI or alias cannot be empty."
            raise ImproperConfigurationError(msg)

        if isinstance(uri_or_alias, Path):
            uri_or_alias = f"file://{uri_or_alias.resolve()}"

        cache_key = (uri_or_alias, tuple(sorted(kwargs.items()))) if kwargs else uri_or_alias
        if cache_key in self._instances:
            return self._instances[cache_key]

        if "://" in uri_or_alias:
            instance = self._resolve_from_uri(uri_or_alias, **kwargs)
        elif uri_or_alias in self._alias_configs:
            backend_cls, stored_uri, config = self._alias_configs[uri_or_alias]
            merged_config = {**config, **kwargs}
            instance = backend_cls(stored_uri, **merged_config)
        else:
            msg = f"Unknown storage alias or invalid URI: '{uri_or_alias}'"
            raise ImproperConfigurationError(msg)

        self._instances[cache_key] = instance
        return instance

    def _resolve_from_uri(self, uri: str, **kwargs: Any) -> ObjectStoreProtocol:
        """Resolve backend from URI, trying ObStore first, then FSSpec."""
        scheme = self._get_scheme(uri)
        last_exc: Optional[Exception] = None

        if scheme not in FSSPEC_ONLY_SCHEMES and OBSTORE_INSTALLED:
            try:
                return self._create_backend("obstore", uri, **kwargs)
            except (ImportError, ValueError) as e:
                logger.debug("ObStore backend failed for %s: %s", uri, e)
                last_exc = e

        if FSSPEC_INSTALLED:
            try:
                return self._create_backend("fsspec", uri, **kwargs)
            except (ImportError, ValueError) as e:
                logger.debug("FSSpec backend failed for %s: %s", uri, e)
                last_exc = e

        msg = f"No storage backend available for URI '{uri}'. Install 'obstore' or 'fsspec' and required dependencies."
        raise MissingDependencyError(msg) from last_exc

    def _determine_backend_class(self, uri: str) -> type[ObjectStoreProtocol]:
        """Determine the best backend class for a URI based on availability and capabilities.

        Prefers ObStore for its superior performance and native capabilities,
        falls back to FSSpec for extended protocol support.

        Args:
            uri: URI to determine backend for.

        Returns:
            Backend class (not instance)
        """
        scheme = self._get_scheme(uri)

        # Check if scheme requires FSSpec (not supported by ObStore)
        if scheme in FSSPEC_ONLY_SCHEMES and FSSPEC_INSTALLED:
            return self._get_backend_class("fsspec")

        # Prefer ObStore for its superior performance
        if OBSTORE_INSTALLED:
            return self._get_backend_class("obstore")
            # Could check capabilities here if needed

        if FSSPEC_INSTALLED:
            return self._get_backend_class("fsspec")

        msg = f"No backend available for URI scheme '{scheme}'. Install obstore or fsspec."
        raise MissingDependencyError(msg)

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

    def _create_backend(self, backend_type: str, uri: str, **kwargs: Any) -> ObjectStoreProtocol:
        """Create backend instance for URI."""
        backend_cls = self._get_backend_class(backend_type)
        return backend_cls(uri, **kwargs)

    def _get_scheme(self, uri: str) -> str:
        """Extract scheme from URI."""
        if not uri or "://" not in uri:
            return "file"
        return uri.split("://", maxsplit=1)[0].lower()

    # Utility methods
    def is_alias_registered(self, alias: str) -> bool:
        """Check if a named alias is registered."""
        return alias in self._alias_configs

    def list_aliases(self) -> list[str]:
        """List all registered aliases."""
        return list(self._alias_configs.keys())

    def clear_cache(self, uri_or_alias: Optional[str] = None) -> None:
        """Clear resolved backend cache.

        Args:
            uri_or_alias: Specific URI or alias to clear, or None to clear all
        """
        if uri_or_alias:
            self._instances.pop(uri_or_alias, None)
        else:
            self._instances.clear()

    def clear(self) -> None:
        """Clear all aliases and instances."""
        self._alias_configs.clear()
        self._aliases.clear()
        self._instances.clear()

    def clear_instances(self) -> None:
        """Clear only cached instances, keeping aliases."""
        self._instances.clear()

    def clear_aliases(self) -> None:
        """Clear only aliases, keeping cached instances."""
        self._alias_configs.clear()
        self._aliases.clear()

    def get_backend_capabilities(self, uri_or_alias: Union[str, Path]) -> "StorageCapabilities":
        """Get capabilities for a backend without creating an instance.

        Args:
            uri_or_alias: URI or alias to check capabilities for

        Returns:
            StorageCapabilities object describing backend capabilities
        """
        if isinstance(uri_or_alias, Path):
            uri_or_alias = f"file://{uri_or_alias.resolve()}"

        if "://" in uri_or_alias:
            backend_cls = self._determine_backend_class(uri_or_alias)
        elif uri_or_alias in self._alias_configs:
            backend_cls, _, _ = self._alias_configs[uri_or_alias]
        else:
            msg = f"Unknown storage alias or invalid URI: '{uri_or_alias}'"
            raise ImproperConfigurationError(msg)

        # Get capabilities from the backend class
        if hasattr(backend_cls, "capabilities"):
            return backend_cls.capabilities

        # Default capabilities if not defined

        return StorageCapabilities()


# Global registry instance
storage_registry = StorageRegistry()
