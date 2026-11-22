"""Storage registry for ObjectStore backends.

Provides a storage registry that supports URI-first access
pattern with automatic backend detection, LocalStore for local file paths,
ObStore preferred with FSSpec fallback for remote storage, scheme-based routing,
and named aliases for common configurations.
"""

import logging
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final, TypeAlias, cast, overload
from urllib.parse import urlparse, urlunparse

from mypy_extensions import mypyc_attr

from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError
from sqlspec.protocols import ObjectStoreProtocol
from sqlspec.storage._paths import is_file_destination, parse_file_uri_path
from sqlspec.storage._utils import sanitize_uri_for_log
from sqlspec.storage.backends.fsspec import FSSpecBackend
from sqlspec.storage.backends.local import LocalStore
from sqlspec.storage.backends.obstore import ObStoreBackend
from sqlspec.typing import FSSPEC_INSTALLED, OBSTORE_INSTALLED
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.type_guards import is_local_path

__all__ = ("StorageBackendFactory", "StorageRegistry", "storage_registry")

logger = get_logger(__name__)

SCHEME_REGEX: Final = re.compile(r"([a-zA-Z0-9+.-]+)://")


FSSPEC_ONLY_SCHEMES: Final[frozenset[str]] = frozenset({"http", "https", "ftp", "sftp", "ssh"})
_CLOUD_OBJECT_STORE_SCHEMES: Final[frozenset[str]] = frozenset({
    "s3",
    "s3a",
    "gs",
    "gcs",
    "az",
    "azure",
    "abfs",
    "abfss",
})
_BUILTIN_BACKEND_NAMES: Final[frozenset[str]] = frozenset({"local", "obstore", "fsspec"})
_INSTANCE_REQUIRED_CALLABLES: Final[tuple[str, ...]] = (
    "resolve_uri",
    "read_bytes_sync",
    "write_bytes_sync",
    "exists_sync",
)

StorageBackendFactory: TypeAlias = Callable[..., ObjectStoreProtocol]
"""Callable accepting a storage URI and keyword options and returning an ``ObjectStoreProtocol`` backend."""


@mypyc_attr(allow_interpreted_subclasses=True)
class StorageRegistry:
    """Global storage registry for named backend configurations.

    Allows registering named storage backends that can be accessed from anywhere
    in your application. Backends are automatically selected based on URI scheme
    unless explicitly overridden.
    """

    __slots__ = ("_alias_configs", "_alias_instances", "_backends", "_instances")

    def __init__(self) -> None:
        self._alias_configs: dict[str, tuple[StorageBackendFactory, str, dict[str, Any]]] = {}
        self._alias_instances: dict[str, ObjectStoreProtocol] = {}
        self._backends: dict[str, StorageBackendFactory] = {}
        self._instances: dict[str | tuple[str, tuple[tuple[str, Any], ...]], ObjectStoreProtocol] = {}

    def register_backend(
        self, name: str, backend_factory: type[ObjectStoreProtocol] | StorageBackendFactory, *, overwrite: bool = False
    ) -> None:
        """Register a named custom storage backend class or factory.

        Names are normalized by stripping surrounding whitespace and folding to
        lowercase so ``"  OCI  "`` and ``"oci"`` refer to the same entry.
        Built-in backend names (``"local"``, ``"obstore"``, ``"fsspec"``) cannot
        be overridden.

        Args:
            name: Unique backend identifier.
            backend_factory: Backend class or factory callable producing an
                ``ObjectStoreProtocol`` instance when invoked with a URI and
                optional keyword arguments.
            overwrite: Replace an existing custom backend registration when
                ``True`` and evict any cached instances created with that
                backend override name.

        Raises:
            TypeError: If ``name`` is not a ``str``, or ``backend_factory`` is a
                pre-configured backend instance or not callable.
            ValueError: If ``name`` is empty, targets a built-in backend name,
                or already exists while ``overwrite`` is ``False``.
        """
        normalized = _normalize_backend_name(name)
        if normalized in _BUILTIN_BACKEND_NAMES:
            msg = f"Cannot override built-in storage backend: '{normalized}'."
            raise ValueError(msg)
        if _is_backend_instance(backend_factory):
            msg = (
                "register_backend() expects a backend class or factory callable, "
                "not a pre-configured ObjectStoreProtocol instance. "
                "Pass the instance to register_alias(..., backend=instance) instead."
            )
            raise TypeError(msg)
        if not callable(backend_factory):
            msg = f"Backend factory must be a callable or class, got {type(backend_factory).__name__}."
            raise TypeError(msg)

        existed = normalized in self._backends
        if existed and not overwrite:
            msg = f"Storage backend '{normalized}' is already registered. Pass overwrite=True to replace it."
            raise ValueError(msg)

        self._backends[normalized] = cast("StorageBackendFactory", backend_factory)
        if existed:
            self._evict_backend_instances({normalized})

        log_with_context(
            logger,
            logging.DEBUG,
            "storage.backend.register",
            backend=normalized,
            factory=_describe_backend(backend_factory),
            overwrite=overwrite,
        )

    def unregister_backend(self, name: str) -> bool:
        """Unregister a previously registered custom storage backend.

        Removes the named backend factory and evicts transient cached instances
        created with that backend override name. Aliases already stored in
        ``_alias_configs`` retain the factory captured when registered.

        Args:
            name: Custom backend name (whitespace-trimmed and case-insensitive).

        Returns:
            ``True`` if a custom backend was removed, ``False`` if empty or absent.

        Raises:
            TypeError: If ``name`` is not a ``str``.
            ValueError: If ``name`` refers to a built-in backend name.
        """
        if not isinstance(name, str):
            msg = f"Backend name must be a string, got {type(name).__name__}."
            raise TypeError(msg)
        normalized = name.strip().lower()
        if not normalized:
            return False
        if normalized in _BUILTIN_BACKEND_NAMES:
            msg = f"Cannot unregister built-in storage backend: '{normalized}'."
            raise ValueError(msg)
        removed = self._backends.pop(normalized, None)
        if removed is None:
            return False
        self._evict_backend_instances({normalized})
        log_with_context(logger, logging.DEBUG, "storage.backend.unregister", backend=normalized)
        return True

    def is_backend_registered(self, name: str) -> bool:
        """Check if a named backend (built-in or custom) is registered.

        Args:
            name: Backend name (whitespace-trimmed and case-insensitive).

        Returns:
            ``True`` when ``name`` maps to a built-in or registered custom backend.

        Raises:
            TypeError: If ``name`` is not a ``str``.
        """
        if not isinstance(name, str):
            msg = f"Backend name must be a string, got {type(name).__name__}."
            raise TypeError(msg)
        normalized = name.strip().lower()
        return bool(normalized) and (normalized in _BUILTIN_BACKEND_NAMES or normalized in self._backends)

    def list_backends(self) -> list[str]:
        """List all available backend names (built-in and custom), sorted."""
        return sorted((*_BUILTIN_BACKEND_NAMES, *self._backends.keys()))

    @overload
    def register_alias(
        self,
        alias: str,
        uri: str,
        *,
        backend: str | StorageBackendFactory | None = None,
        base_path: str = "",
        **kwargs: Any,
    ) -> None: ...

    @overload
    def register_alias(self, alias: str, uri: str | None = None, *, backend: ObjectStoreProtocol) -> None: ...

    def register_alias(
        self,
        alias: str,
        uri: str | None = None,
        *,
        backend: str | type[ObjectStoreProtocol] | StorageBackendFactory | ObjectStoreProtocol | None = None,
        base_path: str = "",
        **kwargs: Any,
    ) -> None:
        """Register a named alias for a storage configuration or backend instance.

        Aliases may point to:

        - A URI with automatic backend selection (``backend=None``).
        - A URI bound to a built-in or custom registered backend name (``str``).
        - A URI bound to an explicit backend class or factory callable.
        - A pre-configured ``ObjectStoreProtocol`` instance (``uri`` optional).

        Pre-configured instances are stored separately from the transient backend
        cache so ``clear_cache()`` and ``clear_instances()`` do not evict them.

        Args:
            alias: Unique alias name (must be non-empty, without leading/trailing
                whitespace, forward slashes, backslashes, or colons).
            uri: Storage URI. Required and non-empty unless ``backend`` is a
                pre-configured ``ObjectStoreProtocol`` instance.
            backend: Backend name, class, factory callable, or pre-configured
                ``ObjectStoreProtocol`` instance.
            base_path: Base path to prepend to all operations (not allowed for
                pre-configured backend instances).
            **kwargs: Backend-specific configuration options (not allowed for
                pre-configured backend instances).

        Raises:
            TypeError: If ``alias`` is not a ``str``, ``uri`` is neither a ``str``
                nor ``None``, ``base_path`` is not a ``str``, or ``backend`` is
                not a valid name, class, callable factory, or backend instance.
            ImproperConfigurationError: If ``alias`` is empty or invalid, ``uri``
                is empty/missing for a non-instance alias, or ``base_path``/``kwargs``
                are supplied alongside a pre-configured instance.
            ValueError: If ``backend`` is an empty or unknown backend name.
            MissingDependencyError: If auto-detected URI scheme lacks a backend.
        """
        _validate_alias(alias)
        if uri is not None and not isinstance(uri, str):
            msg = "Storage URI must be a string or None."
            raise TypeError(msg)
        if not isinstance(base_path, str):
            msg = "base_path must be a string."
            raise TypeError(msg)

        if backend is None or isinstance(backend, str):
            if uri is None or not uri.strip():
                msg = "Storage URI must be a non-empty string unless registering a pre-configured backend instance."
                raise ImproperConfigurationError(msg)
            backend_factory = self._determine_backend_class(uri) if backend is None else self._backend_class(backend)
        elif _is_backend_instance(backend):
            if base_path or kwargs:
                msg = (
                    f"Storage alias {alias!r} is registered as a pre-configured instance "
                    "and does not accept base_path or keyword arguments."
                )
                raise ImproperConfigurationError(msg)
            self._alias_instances[alias] = cast("ObjectStoreProtocol", backend)
            self._alias_configs.pop(alias, None)
            self.clear_cache(alias)
            log_with_context(
                logger,
                logging.DEBUG,
                "storage.alias.register",
                alias=alias,
                uri=sanitize_uri_for_log(uri) if uri and uri.strip() else None,
                backend_type=_describe_backend(backend),
                base_path=None,
            )
            return
        elif callable(backend):
            if uri is None or not uri.strip():
                msg = "Storage URI must be a non-empty string unless registering a pre-configured backend instance."
                raise ImproperConfigurationError(msg)
            backend_factory = cast("StorageBackendFactory", backend)
        else:
            msg = (
                "Storage backend must be a backend name, class, factory callable, or ObjectStoreProtocol instance, "
                f"got {type(backend).__name__}."
            )
            raise TypeError(msg)

        backend_config = dict(kwargs)
        if base_path:
            backend_config["base_path"] = base_path
        self._alias_instances.pop(alias, None)
        self._alias_configs[alias] = (backend_factory, uri, backend_config)
        self.clear_cache(alias)
        log_with_context(
            logger,
            logging.DEBUG,
            "storage.alias.register",
            alias=alias,
            uri=sanitize_uri_for_log(uri),
            backend_type=_describe_backend(backend_factory),
            base_path=base_path or None,
        )

    def unregister_alias(self, alias: str) -> bool:
        """Unregister a previously registered storage alias.

        Removes ``alias`` from both the configuration and instance alias maps
        and evicts any transient cached instances keyed under that alias.

        Args:
            alias: Alias name to remove.

        Returns:
            ``True`` if the alias existed and was removed, ``False`` otherwise.

        Raises:
            TypeError: If ``alias`` is not a ``str``.
        """
        if not isinstance(alias, str):
            msg = "Storage alias must be a string."
            raise TypeError(msg)
        removed_config = self._alias_configs.pop(alias, None) is not None
        removed_instance = self._alias_instances.pop(alias, None) is not None
        if removed_config or removed_instance:
            self.clear_cache(alias)
            return True
        return False

    def get(
        self,
        uri_or_alias: str | Path,
        *,
        backend: str | type[ObjectStoreProtocol] | StorageBackendFactory | None = None,
        **kwargs: Any,
    ) -> ObjectStoreProtocol:
        """Get backend instance using URI-first routing with automatic backend selection.

        Args:
            uri_or_alias: URI to resolve directly OR named alias.
            backend: Force specific backend name (built-in ``"local"``, ``"fsspec"``,
                ``"obstore"``, or a custom backend registered via ``register_backend``),
                backend class, or factory callable instead of auto-selection.
            **kwargs: Additional backend-specific configuration options.

        Returns:
            Backend instance matching ``uri_or_alias``.

        Raises:
            ImproperConfigurationError: If alias is not found, input is empty/invalid,
                or overrides/kwargs are passed for a pre-configured instance alias.
            TypeError: If ``backend`` is a pre-configured instance or not callable.
            ValueError: If ``backend`` is an empty or unknown backend name.
        """
        if not uri_or_alias:
            msg = "URI or alias cannot be empty."
            raise ImproperConfigurationError(msg)

        path_str = str(uri_or_alias)
        scheme = self._scheme(path_str)
        potential_alias = path_str.split("/", 1)[0]

        if scheme is None and potential_alias in self._alias_instances:
            if backend is not None or kwargs:
                msg = (
                    f"Storage alias {potential_alias!r} is registered as a pre-configured instance "
                    "and does not accept backend overrides or keyword arguments."
                )
                raise ImproperConfigurationError(msg)
            log_with_context(
                logger, logging.DEBUG, "storage.resolve", uri_or_alias=sanitize_uri_for_log(path_str), cached=True
            )
            return self._alias_instances[potential_alias]

        base_uri = path_str
        is_alias = False

        if scheme is None and potential_alias in self._alias_configs:
            base_uri = potential_alias
            is_alias = True
        elif scheme:
            if scheme == "file":
                file_path = parse_file_uri_path(path_str)
                path_obj = Path(file_path).expanduser().resolve()
                base_uri = _local_backend_uri(path_obj)
            elif scheme in _CLOUD_OBJECT_STORE_SCHEMES:
                parsed = urlparse(path_str)
                if parsed.netloc:
                    base_uri = f"{scheme}://{parsed.netloc}"
        elif is_local_path(path_str):
            scheme = "file"
            path_obj = Path(path_str).expanduser().resolve()
            base_uri = _local_backend_uri(path_obj)
        else:
            sanitized_input = sanitize_uri_for_log(path_str)
            msg = f"Unknown storage alias or invalid URI: '{sanitized_input}'"
            raise ImproperConfigurationError(msg)

        cache_params = dict(kwargs)
        resolved_factory: StorageBackendFactory | None = None
        cacheable = True
        if backend is not None:
            resolved_factory = self._resolve_backend_factory(backend)
            if isinstance(backend, str):
                cache_params["__backend__"] = _normalize_backend_name(backend)
            else:
                try:
                    hash(backend)
                except TypeError:
                    cacheable = False
                else:
                    cache_params["__backend__"] = backend

        sanitized_log_uri = sanitize_uri_for_log(path_str)
        cache_key: str | tuple[str, tuple[tuple[str, Any], ...]] = base_uri
        if cacheable:
            cache_key = (base_uri, self._make_hashable(cache_params)) if cache_params else base_uri
            if cache_key in self._instances:
                log_with_context(logger, logging.DEBUG, "storage.resolve", uri_or_alias=sanitized_log_uri, cached=True)
                return self._instances[cache_key]

        if not is_alias:
            instance = self._backend_from_uri(base_uri, backend_override=backend, **kwargs)
        else:
            backend_cls, stored_uri, config = self._alias_configs[base_uri]
            factory = resolved_factory if resolved_factory is not None else backend_cls
            instance = factory(stored_uri, **{**config, **kwargs})

        if cacheable:
            self._instances[cache_key] = instance
        log_with_context(logger, logging.DEBUG, "storage.resolve", uri_or_alias=sanitized_log_uri, cached=False)
        return instance

    def is_alias_registered(self, alias: str) -> bool:
        """Check if a named alias is registered.

        Args:
            alias: Alias name to check.

        Returns:
            ``True`` when ``alias`` is registered as a config or instance alias.

        Raises:
            TypeError: If ``alias`` is not a ``str``.
        """
        if not isinstance(alias, str):
            msg = "Storage alias must be a string."
            raise TypeError(msg)
        return alias in self._alias_configs or alias in self._alias_instances

    def list_aliases(self) -> list[str]:
        """List all registered aliases (sorted and deduplicated)."""
        return sorted({*self._alias_configs, *self._alias_instances})

    def clear_cache(self, uri_or_alias: str | None = None) -> None:
        """Clear resolved backend cache without removing registered aliases."""
        if uri_or_alias:
            keys_to_remove: list[str | tuple[str, tuple[tuple[str, Any], ...]]] = []
            for key in list(self._instances):
                if isinstance(key, str):
                    if key == uri_or_alias:
                        keys_to_remove.append(key)
                    continue
                if key and key[0] == uri_or_alias:
                    keys_to_remove.append(key)
            for key in keys_to_remove:
                self._instances.pop(key, None)
            return
        self._instances.clear()

    def clear(self) -> None:
        """Clear all aliases, instance aliases, custom backends, and cached instances."""
        self._alias_configs.clear()
        self._alias_instances.clear()
        self._backends.clear()
        self._instances.clear()

    def clear_instances(self) -> None:
        """Clear only cached transient instances, keeping aliases and custom backends."""
        self._instances.clear()

    def clear_aliases(self) -> None:
        """Clear only aliases (both config and instance), keeping cached instances and custom backends."""
        self._alias_configs.clear()
        self._alias_instances.clear()

    def clear_backends(self) -> None:
        """Clear all custom backends and evict instances cached for those backend names."""
        removed = set(self._backends)
        self._backends.clear()
        self._evict_backend_instances(removed)

    def _evict_backend_instances(self, backend_names: set[str] | frozenset[str]) -> None:
        """Evict cached instances constructed with any of the given backend override names."""
        if not backend_names or not self._instances:
            return
        keys_to_remove: list[str | tuple[str, tuple[tuple[str, Any], ...]]] = []
        for key in self._instances:
            if isinstance(key, str):
                continue
            _, params = key
            for param_key, param_value in params:
                if param_key == "__backend__" and isinstance(param_value, str) and param_value in backend_names:
                    keys_to_remove.append(key)
                    break
        for key in keys_to_remove:
            self._instances.pop(key, None)

    def _make_hashable(self, obj: Any) -> Any:
        """Convert nested dict/list structures to hashable tuples."""
        if isinstance(obj, dict):
            return tuple(sorted(((str(k), self._make_hashable(v)) for k, v in obj.items()), key=repr))
        if isinstance(obj, (list, tuple)):
            return tuple(self._make_hashable(item) for item in obj)
        if isinstance(obj, (set, frozenset)):
            return tuple(sorted((self._make_hashable(item) for item in obj), key=repr))
        if isinstance(obj, Path):
            return obj.as_posix()
        try:
            hash(obj)
        except TypeError:
            return repr(obj)
        return obj

    def _resolve_backend_factory(
        self, backend: str | type[ObjectStoreProtocol] | StorageBackendFactory
    ) -> StorageBackendFactory:
        """Resolve a backend override to a callable factory.

        Args:
            backend: Backend name, class, or factory callable.

        Returns:
            Callable backend factory returning an ``ObjectStoreProtocol`` instance.

        Raises:
            TypeError: If ``backend`` is a pre-configured instance or not callable.
            ValueError: If ``backend`` is an empty or unknown backend name.
        """
        if isinstance(backend, str):
            return self._backend_class(backend)
        if _is_backend_instance(backend):
            msg = (
                "Backend overrides expect a backend name, class, or factory callable, "
                "not a pre-configured ObjectStoreProtocol instance. "
                "Pass the instance to register_alias(..., backend=instance) instead."
            )
            raise TypeError(msg)
        if not callable(backend):
            msg = f"Backend factory must be a name, class, or callable, got {type(backend).__name__}."
            raise TypeError(msg)
        return cast("StorageBackendFactory", backend)

    def _backend_from_uri(
        self,
        uri: str,
        *,
        backend_override: str | type[ObjectStoreProtocol] | StorageBackendFactory | None = None,
        **kwargs: Any,
    ) -> ObjectStoreProtocol:
        """Resolve backend from URI with optional backend override.

        Local files (``file://`` URIs and bare paths) use ``LocalStore``. Cloud
        storage prefers obstore over fsspec when available.

        Args:
            uri: Storage URI to resolve.
            backend_override: Force specific backend name, class, or factory callable.
            **kwargs: Additional backend configuration.

        Returns:
            Configured backend instance.

        Raises:
            MissingDependencyError: No backend available for URI scheme.
        """
        if backend_override is not None:
            return self._resolve_backend_factory(backend_override)(uri, **kwargs)

        scheme = self._scheme(uri)

        if scheme in {None, "file"}:
            return self._backend("local", uri, **kwargs)

        if scheme not in FSSPEC_ONLY_SCHEMES and OBSTORE_INSTALLED:
            try:
                return self._backend("obstore", uri, **kwargs)
            except (ValueError, ImportError, NotImplementedError):
                pass

        if FSSPEC_INSTALLED:
            try:
                return self._backend("fsspec", uri, **kwargs)
            except (ValueError, ImportError, NotImplementedError):
                pass

        msg = f"No backend available for URI scheme '{scheme}'. Install obstore or fsspec for cloud storage support."
        raise MissingDependencyError(msg)

    def _determine_backend_class(self, uri: str) -> StorageBackendFactory:
        """Determine the backend class for a URI based on availability.

        Args:
            uri: Storage URI to analyze.

        Returns:
            Backend class or factory callable to use.

        Raises:
            MissingDependencyError: No backend available for URI scheme.
        """
        scheme = self._scheme(uri)

        if scheme in {None, "file"}:
            return self._backend_class("local")

        if scheme in FSSPEC_ONLY_SCHEMES:
            if not FSSPEC_INSTALLED:
                msg = f"Scheme '{scheme}' requires fsspec. Install with: pip install fsspec"
                raise MissingDependencyError(msg)
            return self._backend_class("fsspec")

        if OBSTORE_INSTALLED:
            return self._backend_class("obstore")

        if FSSPEC_INSTALLED:
            return self._backend_class("fsspec")

        msg = f"No backend available for URI scheme '{scheme}'. Install obstore or fsspec for cloud storage support."
        raise MissingDependencyError(msg)

    def _backend_class(self, backend_type: str) -> StorageBackendFactory:
        """Get backend class or factory callable by type name."""
        normalized = _normalize_backend_name(backend_type)
        if normalized == "local":
            return cast("StorageBackendFactory", LocalStore)
        if normalized == "obstore":
            return cast("StorageBackendFactory", ObStoreBackend)
        if normalized == "fsspec":
            return cast("StorageBackendFactory", FSSpecBackend)
        custom_backend = self._backends.get(normalized)
        if custom_backend is not None:
            return custom_backend
        supported = ", ".join(f"'{name}'" for name in self.list_backends())
        msg = f"Unknown backend type: {backend_type}. Supported types: {supported}"
        raise ValueError(msg)

    def _backend(self, backend_type: str, uri: str, **kwargs: Any) -> ObjectStoreProtocol:
        """Create backend instance for URI."""
        return self._backend_class(backend_type)(uri, **kwargs)

    def _scheme(self, uri: str) -> str | None:
        """Extract the scheme from a URI using regex."""
        if not uri:
            return None
        match = SCHEME_REGEX.match(uri)
        return match.group(1).lower() if match else None


storage_registry = StorageRegistry()


def _local_backend_uri(path: Path) -> str:
    root_path = path.parent if is_file_destination(path) else path
    uri_path = root_path.as_posix()
    if root_path.drive and not uri_path.startswith("/"):
        uri_path = f"/{uri_path}"
    return urlunparse(("file", "", uri_path, "", "", ""))


def _validate_alias(alias: str) -> None:
    """Validate that a storage alias is a clean non-empty path segment."""
    if not isinstance(alias, str):
        msg = "Storage alias must be a string."
        raise TypeError(msg)
    stripped = alias.strip()
    if not stripped:
        msg = "Storage alias cannot be empty."
        raise ImproperConfigurationError(msg)
    if stripped != alias:
        msg = f"Storage alias cannot have leading or trailing whitespace: {alias!r}."
        raise ImproperConfigurationError(msg)
    if "/" in alias or "\\" in alias or ":" in alias:
        msg = f"Storage alias cannot contain '/', '\\\\', or ':': {alias!r}."
        raise ImproperConfigurationError(msg)


def _normalize_backend_name(name: str) -> str:
    """Validate and normalize a backend name to trimmed lowercase."""
    if not isinstance(name, str):
        msg = f"Backend name must be a string, got {type(name).__name__}."
        raise TypeError(msg)
    normalized = name.strip().lower()
    if not normalized:
        msg = "Backend name cannot be empty."
        raise ValueError(msg)
    return normalized


def _is_backend_instance(obj: Any) -> bool:
    """Return ``True`` when *obj* satisfies the structural backend instance contract."""
    if isinstance(obj, type):
        return False
    if not isinstance(getattr(obj, "protocol", None), str):
        return False
    if not isinstance(getattr(obj, "backend_type", None), str):
        return False
    return all(callable(getattr(obj, attr, None)) for attr in _INSTANCE_REQUIRED_CALLABLES)


def _describe_backend(target: Any) -> str:
    """Return a concise name for a backend class, factory callable, or instance."""
    name = getattr(target, "__name__", None)
    if isinstance(name, str) and name:
        return name
    return type(target).__name__
