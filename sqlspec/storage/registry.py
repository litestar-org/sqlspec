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
from typing import Any, Optional, TypeVar, Union

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
        self._aliases: dict[str, tuple[type[ObjectStoreProtocol], dict]] = {}
        self._instances: dict[str, ObjectStoreProtocol] = {}

        # ObStore preferred schemes (will fallback to FSSpec if ObStore unavailable)
        self._obstore_schemes = {"s3", "s3a", "gs", "gcs", "az", "azure", "abfs", "abfss", "memory", "http", "https"}

        # FSSpec preferred schemes (use FSSpec first)
        self._fsspec_schemes = {"file", "ftp", "sftp", "webdav", "dropbox", "gdrive"}

        # Extended schemes that either backend might support
        self._flexible_schemes = {"hdfs", "gcp", "adl"}

    def register_alias(
        self,
        alias: str,
        uri: str,
        *,
        backend: Optional[type[ObjectStoreProtocol]] = None,
        base_path: str = "",
        config: Optional[dict] = None,
        **kwargs: Any,
    ) -> None:
        """Register a named alias for a storage configuration.

        Note: This is a secondary feature. Primary usage is direct URI access via get().

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
            ValueError: If no suitable backend is available for the URI scheme
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
        """Resolve backend from URI with intelligent ObStore preference and FSSpec fallback.

        Args:
            uri: URI to resolve backend for

        Returns:
            Backend instance

        Raises:
            KeyError: If no suitable backend can be created
        """
        scheme = uri.split("://", maxsplit=1)[0].lower()

        # Determine backend preference based on scheme and availability
        if scheme in self._obstore_schemes:
            # Try ObStore first for these schemes
            if OBSTORE_INSTALLED:
                try:
                    return self._create_backend("obstore", uri)
                except Exception as e:
                    logger.debug("ObStore failed for %s, trying FSSpec fallback: %s", uri, e)
                    # Fall through to FSSpec attempt

            # ObStore not available or failed, try FSSpec
            if FSSPEC_INSTALLED:
                try:
                    return self._create_backend("fsspec", uri)
                except Exception as e:
                    msg = f"Neither ObStore nor FSSpec could handle URI '{uri}': {e}"
                    raise KeyError(msg) from e
            else:
                msg = f"URI '{uri}' requires ObStore or FSSpec but neither is installed"
                raise KeyError(msg)

        elif scheme in self._fsspec_schemes:
            # Use FSSpec first for these schemes
            if FSSPEC_INSTALLED:
                try:
                    return self._create_backend("fsspec", uri)
                except Exception as e:
                    logger.debug("FSSpec failed for %s: %s", uri, e)
                    msg = f"FSSpec could not handle URI '{uri}': {e}"
                    raise KeyError(msg) from e
            else:
                msg = f"URI scheme '{scheme}' requires FSSpec but it is not installed"
                raise KeyError(msg)

        elif scheme in self._flexible_schemes:
            # Try both backends for flexible schemes
            last_error = None

            # Try ObStore first if available
            if OBSTORE_INSTALLED:
                try:
                    return self._create_backend("obstore", uri)
                except Exception as e:
                    last_error = e
                    logger.debug("ObStore failed for %s, trying FSSpec: %s", uri, e)

            # Try FSSpec
            if FSSPEC_INSTALLED:
                try:
                    return self._create_backend("fsspec", uri)
                except Exception as e:
                    last_error = e
                    logger.debug("FSSpec also failed for %s: %s", uri, e)

            # Both failed or neither available
            msg = f"No backend could handle URI '{uri}'"
            if last_error:
                msg += f": {last_error}"
            raise KeyError(msg)
        else:
            # Unknown scheme - try both backends
            logger.warning("Unknown URI scheme '%s', trying available backends", scheme)
            last_error = None

            # Try ObStore first if available
            if OBSTORE_INSTALLED:
                try:
                    return self._create_backend("obstore", uri)
                except Exception as e:
                    last_error = e

            # Try FSSpec
            if FSSPEC_INSTALLED:
                try:
                    return self._create_backend("fsspec", uri)
                except Exception as e:
                    last_error = e

            # Neither worked
            msg = f"Unknown URI scheme '{scheme}' and no backend could handle '{uri}'"
            if last_error:
                msg += f": {last_error}"
            raise KeyError(msg)

    def _determine_backend_class(self, uri: str) -> type[ObjectStoreProtocol]:
        """Determine the best backend class for a URI based on scheme and availability.

        Args:
            uri: URI to determine backend for

        Returns:
            Backend class (not instance)

        Raises:
            ValueError: If no suitable backend is available
        """
        scheme = uri.split("://", maxsplit=1)[0].lower()

        if scheme in self._obstore_schemes and OBSTORE_INSTALLED:
            return self._get_backend_class("obstore")
        if scheme in self._fsspec_schemes and FSSPEC_INSTALLED:
            return self._get_backend_class("fsspec")
        if OBSTORE_INSTALLED:
            return self._get_backend_class("obstore")
        if FSSPEC_INSTALLED:
            return self._get_backend_class("fsspec")
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
            try:
                from sqlspec.storage.backends.obstore import ObStoreBackend
            except ImportError as e:
                # ObStore not available, raise MissingDependencyError
                from sqlspec.exceptions import MissingDependencyError

                msg = "obstore"
                raise MissingDependencyError(msg) from e
            else:
                return ObStoreBackend
        elif backend_type == "fsspec":
            from sqlspec.storage.backends.fsspec import FSSpecBackend

            return FSSpecBackend
        else:
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
        backend_cls = self._get_backend_class(backend_type)
        return backend_cls(uri)  # type: ignore[call-arg]

    # Utility methods
    def is_alias_registered(self, alias: str) -> bool:
        """Check if a named alias is registered."""
        return alias in self._aliases

    def list_aliases(self) -> list[str]:
        """List all registered aliases."""
        return list(self._aliases.keys())

    def get_scheme_preferences(self) -> dict[str, str]:
        """Get current scheme preferences for debugging.

        Returns:
            Dict mapping schemes to preferred backend types
        """
        preferences = {}
        for scheme in self._obstore_schemes:
            preferences[scheme] = "obstore" if OBSTORE_INSTALLED else "fsspec"
        for scheme in self._fsspec_schemes:
            preferences[scheme] = "fsspec"
        for scheme in self._flexible_schemes:
            if OBSTORE_INSTALLED:
                preferences[scheme] = "obstore"
            elif FSSPEC_INSTALLED:
                preferences[scheme] = "fsspec"
            else:
                preferences[scheme] = "none"
        return preferences

    def clear_cache(self, uri_or_alias: Optional[str] = None) -> None:
        """Clear resolved backend cache.

        Args:
            uri_or_alias: Specific URI or alias to clear, or None to clear all
        """
        if uri_or_alias:
            self._instances.pop(uri_or_alias, None)
        else:
            self._instances.clear()

    def register_scheme(self, scheme: str, backend: str = "obstore") -> None:
        """Register a custom scheme preference (mainly for testing).

        Args:
            scheme: URI scheme to register
            backend: Backend preference ("obstore" or "fsspec")
        """
        if backend == "obstore":
            self._obstore_schemes.add(scheme)
            self._fsspec_schemes.discard(scheme)
            self._flexible_schemes.discard(scheme)
        elif backend == "fsspec":
            self._fsspec_schemes.add(scheme)
            self._obstore_schemes.discard(scheme)
            self._flexible_schemes.discard(scheme)
        else:
            msg = f"Unknown backend type: {backend}. Use 'obstore' or 'fsspec'"
            raise ValueError(msg)


# Global registry instance
storage_registry = StorageRegistry()
