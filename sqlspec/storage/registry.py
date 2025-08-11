"""Unified Storage Registry for ObjectStore backends.

Provides a flexible, lazy-loading storage registry that supports URI-first access
pattern with automatic backend detection, ObStore preferred with FSSpec fallback,
intelligent scheme-based routing, and named aliases for common configurations.
"""

import logging
import re
from pathlib import Path
from typing import Any, Final, Optional, Union, cast

from mypy_extensions import mypyc_attr

from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError
from sqlspec.protocols import ObjectStoreProtocol
from sqlspec.storage.capabilities import StorageCapabilities
from sqlspec.typing import FSSPEC_INSTALLED, OBSTORE_INSTALLED

__all__ = ("StorageRegistry", "storage_registry")

logger = logging.getLogger(__name__)


SCHEME_REGEX: Final = re.compile(r"([a-zA-Z0-9+.-]+)://")
FILE_PROTOCOL: Final[str] = "file"
S3_PROTOCOL: Final[str] = "s3"
GCS_PROTOCOL: Final[str] = "gs"
AZURE_PROTOCOL: Final[str] = "az"
FSSPEC_ONLY_SCHEMES: Final[frozenset[str]] = frozenset({"http", "https", "ftp", "sftp", "ssh"})


@mypyc_attr(allow_interpreted_subclasses=True)
class StorageRegistry:
    """Storage registry with URI-first access and automatic backend selection.

    Provides URI-first access pattern with automatic backend selection.
    Named aliases support complex configurations.

    Examples:
        backend = registry.get("s3://my-bucket/file.parquet")
        backend = registry.get("file:///tmp/data.csv")
        backend = registry.get("gs://bucket/data.json")

        registry.register_alias(
            "production-s3",
            uri="s3://prod-bucket/data",
            base_path="sqlspec",
            aws_access_key_id="...",
            aws_secret_access_key="..."
        )
        backend = registry.get("production-s3")
    """

    __slots__ = ("_alias_configs", "_aliases", "_cache", "_instances")

    def __init__(self) -> None:
        self._alias_configs: dict[str, tuple[type[ObjectStoreProtocol], str, dict[str, Any]]] = {}
        self._aliases: dict[str, dict[str, Any]] = {}
        self._instances: dict[Union[str, tuple[str, tuple[tuple[str, Any], ...]]], ObjectStoreProtocol] = {}
        self._cache: dict[str, tuple[str, type[ObjectStoreProtocol]]] = {}

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
        """Get backend instance using URI-first routing with automatic backend selection.

        Args:
            uri_or_alias: URI to resolve directly OR named alias
            **kwargs: Additional backend-specific configuration options

        Returns:
            Backend instance with automatic backend selection

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
        scheme = self._get_scheme(uri_or_alias)
        if not scheme and (
            Path(uri_or_alias).exists()
            or Path(uri_or_alias).is_absolute()
            or uri_or_alias.startswith(("~", "."))
            or ":\\" in uri_or_alias
            or "/" in uri_or_alias
        ):
            scheme = "file"
            uri_or_alias = f"file://{uri_or_alias}"

        if scheme:
            instance = self._resolve_from_uri(uri_or_alias, **kwargs)
        elif uri_or_alias in self._alias_configs:
            backend_cls, stored_uri, config = self._alias_configs[uri_or_alias]
            instance = backend_cls(stored_uri, **{**config, **kwargs})
        else:
            msg = f"Unknown storage alias or invalid URI: '{uri_or_alias}'"
            raise ImproperConfigurationError(msg)
        self._instances[cache_key] = instance
        return instance

    def _resolve_from_uri(self, uri: str, **kwargs: Any) -> ObjectStoreProtocol:
        """Resolve backend from URI, trying ObStore first, then FSSpec."""
        scheme = self._get_scheme(uri)
        if scheme not in FSSPEC_ONLY_SCHEMES and OBSTORE_INSTALLED:
            try:
                return self._create_backend("obstore", uri, **kwargs)
            except (ValueError, ImportError, NotImplementedError):
                pass
        if FSSPEC_INSTALLED:
            try:
                return self._create_backend("fsspec", uri, **kwargs)
            except (ValueError, ImportError, NotImplementedError):
                pass
        msg = f"No storage backend available for scheme '{scheme}'. Install obstore or fsspec."
        raise MissingDependencyError(msg)

    def _determine_backend_class(self, uri: str) -> type[ObjectStoreProtocol]:
        """Determine the backend class for a URI based on availability."""
        scheme = self._get_scheme(uri)
        if scheme in FSSPEC_ONLY_SCHEMES and FSSPEC_INSTALLED:
            return self._get_backend_class("fsspec")
        if OBSTORE_INSTALLED:
            return self._get_backend_class("obstore")
        if FSSPEC_INSTALLED:
            return self._get_backend_class("fsspec")
        msg = f"No backend available for URI scheme '{scheme}'. Install obstore or fsspec."
        raise MissingDependencyError(msg)

    def _get_backend_class(self, backend_type: str) -> type[ObjectStoreProtocol]:
        """Get backend class by type name."""
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
        return self._get_backend_class(backend_type)(uri, **kwargs)

    def _get_scheme(self, uri: str) -> Optional[str]:
        """Extract the scheme from a URI using regex."""
        if not uri:
            return None
        match = SCHEME_REGEX.match(uri)
        return match.group(1).lower() if match else None

    def is_alias_registered(self, alias: str) -> bool:
        """Check if a named alias is registered."""
        return alias in self._alias_configs

    def list_aliases(self) -> list[str]:
        """List all registered aliases."""
        return list(self._alias_configs.keys())

    def clear_cache(self, uri_or_alias: Optional[str] = None) -> None:
        """Clear resolved backend cache."""
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
        """Get capabilities for a backend without creating an instance."""
        if isinstance(uri_or_alias, Path):
            uri_or_alias = f"file://{uri_or_alias.resolve()}"
        if "://" in uri_or_alias:
            backend_cls = self._determine_backend_class(uri_or_alias)
        elif uri_or_alias in self._alias_configs:
            backend_cls, _, _ = self._alias_configs[uri_or_alias]
        else:
            msg = f"Unknown storage alias or invalid URI: '{uri_or_alias}'"
            raise ImproperConfigurationError(msg)
        if hasattr(backend_cls, "capabilities"):
            return backend_cls.capabilities
        return StorageCapabilities()


storage_registry = StorageRegistry()
