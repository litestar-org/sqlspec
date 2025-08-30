"""Storage registry for ObjectStore backends.

Provides a storage registry that supports URI-first access
pattern with automatic backend detection, ObStore preferred with FSSpec fallback,
scheme-based routing, and named aliases for common configurations.
"""

import logging
import re
from pathlib import Path
from typing import Any, Final, Optional, Union, cast

from mypy_extensions import mypyc_attr

from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError
from sqlspec.protocols import ObjectStoreProtocol
from sqlspec.typing import FSSPEC_INSTALLED, OBSTORE_INSTALLED

__all__ = ("StorageRegistry", "storage_registry")

logger = logging.getLogger(__name__)


def _is_local_uri(uri: str) -> bool:
    """Check if URI represents a local filesystem path."""
    if "://" in uri and not uri.startswith("file://"):
        return False
    windows_drive_min_length = 3
    return (
        Path(uri).exists()
        or Path(uri).is_absolute()
        or uri.startswith(("~", ".", "/"))
        or (len(uri) >= windows_drive_min_length and uri[1:3] == ":\\")
        or "/" in uri
    )


SCHEME_REGEX: Final = re.compile(r"([a-zA-Z0-9+.-]+)://")


FSSPEC_ONLY_SCHEMES: Final[frozenset[str]] = frozenset({"http", "https", "ftp", "sftp", "ssh"})


@mypyc_attr(allow_interpreted_subclasses=True)
class StorageRegistry:
    """Global storage registry for named backend configurations.

    Allows registering named storage backends that can be accessed from anywhere
    in your application. Backends are automatically selected based on URI scheme
    unless explicitly overridden.

    Examples:
        # Direct URI access to storage containers
        backend = registry.get("s3://my-bucket")
        backend = registry.get("file:///tmp/data")
        backend = registry.get("gs://my-gcs-bucket")

        # Named store pattern for environment-specific backends
        # Development
        registry.register_alias("my_app_store", "file:///tmp/dev_data")

        # Production
        registry.register_alias("my_app_store", "s3://prod-bucket/data")

        # Access from anywhere in your app
        store = registry.get("my_app_store")  # Works in both environments

        # Force specific backend when multiple options available
        backend = registry.get("s3://bucket", backend="fsspec")  # Force fsspec over obstore
    """

    __slots__ = ("_alias_configs", "_aliases", "_cache", "_instances")

    def __init__(self) -> None:
        self._alias_configs: dict[str, tuple[type[ObjectStoreProtocol], str, dict[str, Any]]] = {}
        self._aliases: dict[str, dict[str, Any]] = {}
        self._instances: dict[Union[str, tuple[str, tuple[tuple[str, Any], ...]]], ObjectStoreProtocol] = {}
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
        self, alias: str, uri: str, *, backend: Optional[str] = None, base_path: str = "", **kwargs: Any
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

    def get(
        self, uri_or_alias: Union[str, Path], *, backend: Optional[str] = None, **kwargs: Any
    ) -> ObjectStoreProtocol:
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

        cache_key = (uri_or_alias, self._make_hashable(kwargs)) if kwargs else uri_or_alias
        if cache_key in self._instances:
            return self._instances[cache_key]
        scheme = self._get_scheme(uri_or_alias)
        if not scheme and _is_local_uri(uri_or_alias):
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
        return instance

    def _resolve_from_uri(
        self, uri: str, *, backend_override: Optional[str] = None, **kwargs: Any
    ) -> ObjectStoreProtocol:
        """Resolve backend from URI with optional backend override."""
        if backend_override:
            return self._create_backend(backend_override, uri, **kwargs)
        scheme = self._get_scheme(uri)

        # For local files, prefer LocalStore first
        if scheme in {None, "file"}:
            return self._create_backend("local", uri, **kwargs)

        # Try ObStore first if available and appropriate
        if scheme not in FSSPEC_ONLY_SCHEMES and OBSTORE_INSTALLED:
            try:
                return self._create_backend("obstore", uri, **kwargs)
            except (ValueError, ImportError, NotImplementedError):
                pass

        # Try FSSpec if available
        if FSSPEC_INSTALLED:
            try:
                return self._create_backend("fsspec", uri, **kwargs)
            except (ValueError, ImportError, NotImplementedError):
                pass

        # For cloud schemes without backends, provide helpful error
        msg = f"No backend available for URI scheme '{scheme}'. Install obstore or fsspec for cloud storage support."
        raise MissingDependencyError(msg)

    def _determine_backend_class(self, uri: str) -> type[ObjectStoreProtocol]:
        """Determine the backend class for a URI based on availability."""
        scheme = self._get_scheme(uri)

        # For local files, always use LocalStore
        if scheme in {None, "file"}:
            return self._get_backend_class("local")

        # FSSpec-only schemes require FSSpec
        if scheme in FSSPEC_ONLY_SCHEMES and FSSPEC_INSTALLED:
            return self._get_backend_class("fsspec")

        # Prefer ObStore for cloud storage if available
        if OBSTORE_INSTALLED:
            return self._get_backend_class("obstore")

        # Fall back to FSSpec if available
        if FSSPEC_INSTALLED:
            return self._get_backend_class("fsspec")

        # For cloud schemes without backends, provide helpful error
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


storage_registry = StorageRegistry()
