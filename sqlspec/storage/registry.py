"""Storage registry for ObjectStore backends.

Provides a storage registry that supports URI-first access
pattern with automatic backend detection, ObStore preferred with FSSpec fallback,
scheme-based routing, and named aliases for common configurations.
"""

import logging
import re
from pathlib import Path
from typing import Any, Final, cast

from mypy_extensions import mypyc_attr

from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError
from sqlspec.protocols import ObjectStoreProtocol
from sqlspec.typing import FSSPEC_INSTALLED, OBSTORE_INSTALLED
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.type_guards import is_local_path

__all__ = ("StorageRegistry", "storage_registry")

logger = get_logger(__name__)

SCHEME_REGEX: Final = re.compile(r"([a-zA-Z0-9+.-]+)://")


FSSPEC_ONLY_SCHEMES: Final[frozenset[str]] = frozenset({"http", "https", "ftp", "sftp", "ssh"})


@mypyc_attr(allow_interpreted_subclasses=True)
class StorageRegistry:
    """Global storage registry for named backend configurations.

    Allows registering named storage backends that can be accessed from anywhere
    in your application. Backends are automatically selected based on URI scheme
    unless explicitly overridden.

    Examples:
        backend = registry.get("s3://my-bucket")
        backend = registry.get("file:///tmp/data")
        backend = registry.get("gs://my-gcs-bucket")

        registry.register_alias("my_app_store", "file:///tmp/dev_data")

        registry.register_alias("my_app_store", "s3://prod-bucket/data")

        store = registry.get("my_app_store")

        backend = registry.get("s3://bucket", backend="fsspec")
    """

    __slots__ = ("_alias_configs", "_aliases", "_cache", "_instances")

    def __init__(self) -> None:
        self._alias_configs: dict[str, tuple[type[ObjectStoreProtocol], str, dict[str, Any]]] = {}
        self._aliases: dict[str, dict[str, Any]] = {}
        self._instances: dict[str | tuple[str, tuple[tuple[str, Any], ...]], ObjectStoreProtocol] = {}
        self._cache: dict[str, tuple[str, type[ObjectStoreProtocol]]] = {}

    def _make_hashable(self, obj: Any) -> Any:
        """Convert nested dict/list structures to hashable tuples."""
        if isinstance(obj, dict):
            return tuple(sorted((k, self._make_hashable(v)) for k, v in obj.items()))
        if isinstance(obj, list):
            return tuple(self._make_hashable(item) for item in obj)
        if isinstance(obj, set):
            return tuple(sorted(self._make_hashable(item) for item in obj))
        return obj

    def register_alias(
        self, alias: str, uri: str, *, backend: str | None = None, base_path: str = "", **kwargs: Any
    ) -> None:
        """Register a named alias for a storage configuration.

        Args:
            alias: Unique alias name (e.g., "my_app_store", "user_uploads")
            uri: Storage URI (e.g., "s3://bucket", "file:///path", "gs://bucket")
            backend: Force specific backend ("local", "fsspec", "obstore") instead of auto-detection
            base_path: Base path to prepend to all operations
            **kwargs: Backend-specific configuration options
        """
        backend_cls = self._get_backend_class(backend) if backend else self._determine_backend_class(uri)

        backend_config = dict(kwargs)
        if base_path:
            backend_config["base_path"] = base_path
        self._alias_configs[alias] = (backend_cls, uri, backend_config)

        test_config = dict(backend_config)
        test_config["uri"] = uri
        self._aliases[alias] = test_config
        log_with_context(
            logger,
            logging.DEBUG,
            "storage.alias.register",
            alias=alias,
            uri=uri,
            backend_type=backend_cls.__name__,
            base_path=base_path or None,
        )

    def get(self, uri_or_alias: str | Path, *, backend: str | None = None, **kwargs: Any) -> ObjectStoreProtocol:
        """Get backend instance using URI-first routing with automatic backend selection.

        Args:
            uri_or_alias: URI to resolve directly OR named alias (e.g., "my_app_store")
            backend: Force specific backend ("local", "fsspec", "obstore") instead of auto-selection
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

        cache_params = dict(kwargs)
        if backend:
            cache_params["__backend__"] = backend
        cache_key = (uri_or_alias, self._make_hashable(cache_params)) if cache_params else uri_or_alias
        if cache_key in self._instances:
            log_with_context(logger, logging.DEBUG, "storage.resolve", uri_or_alias=str(uri_or_alias), cached=True)
            return self._instances[cache_key]
        scheme = self._get_scheme(uri_or_alias)
        if not scheme and is_local_path(uri_or_alias):
            scheme = "file"
            uri_or_alias = f"file://{uri_or_alias}"

        if scheme:
            instance = self._resolve_from_uri(uri_or_alias, backend_override=backend, **kwargs)
        elif uri_or_alias in self._alias_configs:
            backend_cls, stored_uri, config = self._alias_configs[uri_or_alias]
            if backend:
                backend_cls = self._get_backend_class(backend)
            instance = backend_cls(stored_uri, **{**config, **kwargs})
        else:
            msg = f"Unknown storage alias or invalid URI: '{uri_or_alias}'"
            raise ImproperConfigurationError(msg)
        self._instances[cache_key] = instance
        log_with_context(logger, logging.DEBUG, "storage.resolve", uri_or_alias=str(uri_or_alias), cached=False)
        return instance

    def _resolve_from_uri(self, uri: str, *, backend_override: str | None = None, **kwargs: Any) -> ObjectStoreProtocol:
        """Resolve backend from URI with optional backend override.

        Backend selection priority for local files (file:// or bare paths):
        1. obstore (if installed) - provides async I/O performance
        2. fsspec (if installed) - async wrapper fallback
        3. local (always available) - zero-dependency sync backend

        For cloud storage, prefer obstore over fsspec when available.

        Args:
            uri: Storage URI to resolve.
            backend_override: Force specific backend type.
            **kwargs: Additional backend configuration.

        Returns:
            Configured backend instance.

        Raises:
            MissingDependencyError: No backend available for URI scheme.
        """
        if backend_override:
            return self._create_backend(backend_override, uri, **kwargs)

        scheme = self._get_scheme(uri)

        if scheme in {None, "file"}:
            if OBSTORE_INSTALLED:
                try:
                    return self._create_backend("obstore", uri, **kwargs)
                except (ValueError, ImportError, NotImplementedError):
                    pass

            if FSSPEC_INSTALLED:
                try:
                    return self._create_backend("fsspec", uri, **kwargs)
                except (ValueError, ImportError, NotImplementedError):
                    pass

            return self._create_backend("local", uri, **kwargs)

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

        msg = f"No backend available for URI scheme '{scheme}'. Install obstore or fsspec for cloud storage support."
        raise MissingDependencyError(msg)

    def _determine_backend_class(self, uri: str) -> type[ObjectStoreProtocol]:
        """Determine the backend class for a URI based on availability.

        Args:
            uri: Storage URI to analyze.

        Returns:
            Backend class type to use.

        Raises:
            MissingDependencyError: No backend available for URI scheme.
        """
        scheme = self._get_scheme(uri)

        if scheme in {None, "file"}:
            if OBSTORE_INSTALLED:
                return self._get_backend_class("obstore")
            if FSSPEC_INSTALLED:
                return self._get_backend_class("fsspec")
            return self._get_backend_class("local")

        if scheme in FSSPEC_ONLY_SCHEMES:
            if not FSSPEC_INSTALLED:
                msg = f"Scheme '{scheme}' requires fsspec. Install with: pip install fsspec"
                raise MissingDependencyError(msg)
            return self._get_backend_class("fsspec")

        if OBSTORE_INSTALLED:
            return self._get_backend_class("obstore")

        if FSSPEC_INSTALLED:
            return self._get_backend_class("fsspec")

        msg = f"No backend available for URI scheme '{scheme}'. Install obstore or fsspec for cloud storage support."
        raise MissingDependencyError(msg)

    def _get_backend_class(self, backend_type: str) -> type[ObjectStoreProtocol]:
        """Get backend class by type name."""
        if backend_type == "local":
            from sqlspec.storage.backends.local import LocalStore

            return cast("type[ObjectStoreProtocol]", LocalStore)
        if backend_type == "obstore":
            from sqlspec.storage.backends.obstore import ObStoreBackend

            return cast("type[ObjectStoreProtocol]", ObStoreBackend)
        if backend_type == "fsspec":
            from sqlspec.storage.backends.fsspec import FSSpecBackend

            return cast("type[ObjectStoreProtocol]", FSSpecBackend)
        msg = f"Unknown backend type: {backend_type}. Supported types: 'local', 'obstore', 'fsspec'"
        raise ValueError(msg)

    def _create_backend(self, backend_type: str, uri: str, **kwargs: Any) -> ObjectStoreProtocol:
        """Create backend instance for URI."""
        return self._get_backend_class(backend_type)(uri, **kwargs)

    def _get_scheme(self, uri: str) -> str | None:
        """Extract the scheme from a URI using regex."""
        if not uri:
            return None
        match = SCHEME_REGEX.match(uri)
        return match.group(1).lower() if match else None

    def is_alias_registered(self, alias: str) -> bool:
        """Check if a named alias is registered."""
        return alias in self._alias_configs

    def list_aliases(self) -> "list[str]":
        """List all registered aliases."""
        return list(self._alias_configs.keys())

    def clear_cache(self, uri_or_alias: str | None = None) -> None:
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


storage_registry = StorageRegistry()
