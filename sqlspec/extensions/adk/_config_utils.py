"""ADK store configuration helpers."""

import importlib
from collections.abc import Mapping
from typing import Any, NoReturn, NotRequired, Protocol, cast

from typing_extensions import TypedDict

from sqlspec.exceptions import ImproperConfigurationError, SQLSpecError
from sqlspec.utils.adapters import resolve_adapter_name, resolve_extension_store_class
from sqlspec.utils.module_loader import import_string

__all__ = (
    "_ADKArtifactStoreConfig",
    "_ADKMemoryStoreConfig",
    "_ADKSessionStoreConfig",
    "_adk_artifact_store_config",
    "_adk_feature_enabled",
    "_adk_memory_store_config",
    "_adk_session_store_config",
    "_ensure_adk_store_registration",
    "adk_memory_migration_enabled",
    "adk_sessions_migration_enabled",
    "resolve_adk_artifact_store_class",
    "resolve_adk_config",
    "resolve_adk_memory_store_class",
    "resolve_adk_store_class",
)


class _ADKSessionStoreConfig(TypedDict):
    """Normalized ADK session store configuration."""

    session_table: str
    events_table: str
    app_state_table: str
    user_state_table: str
    metadata_table: str
    owner_id_column: NotRequired[str]


class _ADKMemoryStoreConfig(TypedDict):
    """Normalized ADK memory store configuration."""

    enable_memory: bool
    memory_table: str
    use_fts: bool
    max_results: int
    vector_index_type: str
    vector_dimensions: int
    enable_bm25: bool
    scann_num_leaves: int
    scann_quantizer: str
    owner_id_column: NotRequired[str]


class _ADKArtifactStoreConfig(TypedDict):
    """Normalized ADK artifact store configuration."""

    artifact_table: str


class _ADKConfigSource(Protocol):
    """Config-like object exposing ADK extension settings."""

    @property
    def extension_config(self) -> dict[str, Any]:
        """Return extension settings."""
        ...


def resolve_adk_config(config: Any) -> dict[str, Any]:
    """Return a validated, mutable copy of the ADK extension configuration.

    Reads ``config.extension_config["adk"]`` when present, treats missing or
    ``None`` values as an empty mapping, rejects non-mapping values, and
    validates ``migration_mode`` and accepted option keys against ``ADKConfig``,
    the adapter's ``*ADKConfig`` TypedDicts, and any explicit store overrides.

    Args:
        config: Database configuration instance, config class, or object
            exposing an optional ``extension_config`` mapping.

    Returns:
        A newly copied ``dict[str, Any]`` containing the ADK settings so
        caller mutations never modify ``config.extension_config["adk"]``.

    Raises:
        ImproperConfigurationError: If the ``adk`` extension entry is not a
            mapping or contains unsupported configuration keys.
        MigrationError: If ``migration_mode`` is set to an invalid value.
    """
    extension_config = getattr(config, "extension_config", None)
    raw_adk: Any = extension_config.get("adk", {}) if isinstance(extension_config, Mapping) else {}
    if raw_adk is None:
        adk_config: dict[str, Any] = {}
    elif isinstance(raw_adk, Mapping):
        adk_config = dict(raw_adk)
    else:
        msg = f"Invalid ADK extension config: expected a mapping, got {type(raw_adk).__name__}"
        raise ImproperConfigurationError(msg)
    _validate_adk_extension_config(config, adk_config)
    return adk_config


def resolve_adk_store_class(config: Any, store_suffix: str) -> type[Any]:
    """Resolve the adapter-specific ADK store class for a database config.

    Checks for an explicit ``adk_memory_store_class`` (when ``store_suffix`` is
    ``"ADKMemoryStore"``) or ``adk_store_class`` override first, then imports
    the adapter's store class using its config class name and exported module
    fallback.

    Args:
        config: Database configuration instance or class.
        store_suffix: Store class suffix to resolve (for example ``"ADKStore"``
            or ``"ADKMemoryStore"``).

    Returns:
        The resolved ADK store class.

    Raises:
        SQLSpecError: If ``config`` cannot be mapped to an adapter name or the
            corresponding store class cannot be imported.
    """
    store_attr = "adk_memory_store_class" if store_suffix == "ADKMemoryStore" else "adk_store_class"
    explicit_store = _adk_explicit_store_class(config, store_attr)
    if explicit_store is not None:
        return explicit_store

    store_path = _adk_store_path(config, store_suffix)
    try:
        return cast("type[Any]", import_string(store_path))
    except ImportError as e:
        store_class = _adk_exported_store_class(config, store_suffix)
        if store_class is not None:
            return store_class
        _raise_store_import_failed(store_path, e)


def resolve_adk_memory_store_class(config: Any) -> type[Any] | None:
    """Resolve the ADK memory store class when the adapter provides one.

    Returns ``None`` instead of raising when the adapter is known but does not
    export an ``ADKMemoryStore`` implementation.

    Args:
        config: Database configuration instance or class.

    Returns:
        The resolved ADK memory store class, or ``None`` if the adapter does
        not provide a memory store.

    Raises:
        SQLSpecError: If ``config`` has no explicit ``adk_memory_store_class``
            and cannot be mapped to an adapter name.
    """
    explicit_store = _adk_explicit_store_class(config, "adk_memory_store_class")
    if explicit_store is not None:
        return explicit_store

    store_path = _adk_store_path(config, "ADKMemoryStore")
    try:
        return cast("type[Any]", import_string(store_path))
    except ImportError:
        return _adk_exported_store_class(config, "ADKMemoryStore")


def resolve_adk_artifact_store_class(config: Any) -> type[Any] | None:
    """Resolve the ADK artifact store class when the adapter provides one.

    Checks for an explicit ``adk_artifact_store_class`` attribute before
    importing ``sqlspec.adapters.<adapter>.adk.artifact_store``. Missing
    adapter or ``artifact_store`` modules return ``None``; missing transitive
    third-party dependencies inside an existing ``artifact_store`` module are
    re-raised.

    Args:
        config: Database configuration instance or class.

    Returns:
        The resolved ADK artifact store class, or ``None`` when no artifact
        store is available for ``config``.

    Raises:
        ModuleNotFoundError: If importing an adapter's ``artifact_store``
            module fails because of a missing third-party or internal
            dependency.
    """
    explicit_store = _adk_explicit_store_class(config, "adk_artifact_store_class")
    if explicit_store is not None:
        return explicit_store

    adapter_name = resolve_adapter_name(config)
    if adapter_name is None:
        return None

    adapter_prefix = f"sqlspec.adapters.{adapter_name}"
    module_name = f"{adapter_prefix}.adk.artifact_store"
    try:
        artifact_module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name in {"sqlspec.adapters", adapter_prefix, f"{adapter_prefix}.adk", module_name}:
            return None
        raise

    store_suffix = "ADKArtifactStore"
    exported = tuple(
        name
        for name in getattr(artifact_module, "__all__", ())
        if isinstance(name, str)
        and name.endswith(store_suffix)
        and isinstance(getattr(artifact_module, name, None), type)
    )
    target_cls = config if isinstance(config, type) else type(config)
    for cls in target_cls.__mro__:
        cls_name = cls.__name__
        if cls_name.endswith("Config"):
            expected_prefix = cls_name.removesuffix("Config").lower()
            for exported_name in exported:
                if exported_name[: -len(store_suffix)].lower() == expected_prefix:
                    return cast("type[Any]", getattr(artifact_module, exported_name))
    if len(exported) == 1:
        return cast("type[Any]", getattr(artifact_module, exported[0]))
    if len(exported) > 1:
        mode_suffix = f"Async{store_suffix}" if bool(getattr(config, "is_async", False)) else f"Sync{store_suffix}"
        for exported_name in exported:
            if exported_name.endswith(mode_suffix):
                return cast("type[Any]", getattr(artifact_module, exported_name))
    return None


def adk_sessions_migration_enabled(config: Any) -> bool:
    """Return whether ADK session DDL should be included for this config.

    Reads ``enable_sessions`` from the validated ADK extension configuration
    and defaults to ``True`` when unset or ``None``.

    Args:
        config: Database configuration instance or object exposing optional
            ``extension_config``.

    Returns:
        ``True`` if ADK session migrations are enabled, ``False`` otherwise.

    Raises:
        ImproperConfigurationError: If the ``adk`` extension entry is invalid.
        MigrationError: If ``migration_mode`` is invalid.
    """
    return _adk_feature_enabled(config, "enable_sessions")


def adk_memory_migration_enabled(config: Any) -> bool:
    """Return whether ADK memory DDL should be included for this config.

    Reads ``enable_memory`` from the validated ADK extension configuration
    and defaults to ``True`` when unset or ``None``.

    Args:
        config: Database configuration instance or object exposing optional
            ``extension_config``.

    Returns:
        ``True`` if ADK memory migrations are enabled, ``False`` otherwise.

    Raises:
        ImproperConfigurationError: If the ``adk`` extension entry is invalid.
        MigrationError: If ``migration_mode`` is invalid.
    """
    return _adk_feature_enabled(config, "enable_memory")


def _adk_session_store_config(config: _ADKConfigSource) -> _ADKSessionStoreConfig:
    """Return normalized session store table settings."""
    adk_config = resolve_adk_config(config)
    result: _ADKSessionStoreConfig = {
        "session_table": str(adk_config.get("session_table") or "adk_session"),
        "events_table": str(adk_config.get("events_table") or "adk_event"),
        "app_state_table": str(adk_config.get("app_state_table") or "adk_app_state"),
        "user_state_table": str(adk_config.get("user_state_table") or "adk_user_state"),
        "metadata_table": str(adk_config.get("metadata_table") or "adk_internal_metadata"),
    }
    _apply_owner_id(result, adk_config)
    return result


def _adk_memory_store_config(config: _ADKConfigSource) -> _ADKMemoryStoreConfig:
    """Return normalized memory store settings."""
    adk_config = resolve_adk_config(config)
    enable_memory = adk_config.get("enable_memory")
    max_results = adk_config.get("memory_max_results")
    result: _ADKMemoryStoreConfig = {
        "enable_memory": bool(enable_memory) if enable_memory is not None else True,
        "memory_table": str(adk_config.get("memory_table") or "adk_memory"),
        "use_fts": bool(adk_config.get("memory_use_fts", False)),
        "max_results": int(max_results) if isinstance(max_results, int) else 20,
        "vector_index_type": str(adk_config.get("vector_index_type") or "hnsw"),
        "vector_dimensions": int(adk_config.get("vector_dimensions") or 768),
        "enable_bm25": bool(adk_config.get("enable_bm25", False)),
        "scann_num_leaves": int(adk_config.get("scann_num_leaves") or 100),
        "scann_quantizer": str(adk_config.get("scann_quantizer") or "SQ8"),
    }
    _apply_owner_id(result, adk_config)
    return result


def _adk_artifact_store_config(config: _ADKConfigSource) -> _ADKArtifactStoreConfig:
    """Return normalized artifact store settings."""
    adk_config = resolve_adk_config(config)
    return {"artifact_table": str(adk_config.get("artifact_table") or "adk_artifact")}


def _apply_owner_id(result: "_ADKSessionStoreConfig | _ADKMemoryStoreConfig", adk_config: dict[str, Any]) -> None:
    """Copy the configured owner column into normalized store settings."""
    owner_id = adk_config.get("owner_id_column")
    if owner_id is not None:
        result["owner_id_column"] = cast("str", owner_id)


def _adk_store_path(config: Any, store_suffix: str) -> str:
    """Return the adapter-specific ADK store import path."""
    config_class = config if isinstance(config, type) else type(config)
    config_module = config_class.__module__
    config_name = config_class.__name__
    adapter_name = resolve_adapter_name(config)

    if adapter_name is None:
        _raise_unsupported_config(f"{config_module}.{config_name}")

    store_class_name = config_name.replace("Config", store_suffix)
    return f"sqlspec.adapters.{adapter_name}.adk.{store_class_name}"


def _adk_exported_store_class(config: Any, store_suffix: str) -> type[Any] | None:
    """Return an ADK store export that matches a config name or sync/async mode."""
    store_attr = "adk_memory_store_class" if store_suffix == "ADKMemoryStore" else "adk_store_class"
    try:
        resolved = resolve_extension_store_class(
            config, extension="adk", store_attr=store_attr, store_suffix=store_suffix
        )
    except ImportError:
        resolved = None
    if resolved is not None:
        return resolved

    adapter_name = resolve_adapter_name(config)
    if adapter_name is None:
        return None

    module_path = f"sqlspec.adapters.{adapter_name}.adk"
    try:
        module = importlib.import_module(module_path)
    except ImportError:
        return None

    config_class = config if isinstance(config, type) else type(config)
    config_name = config_class.__name__
    expected_prefix = config_name.removesuffix("Config").lower()
    store_names = tuple(
        name
        for name in getattr(module, "__all__", ())
        if name.endswith(store_suffix) and not (store_suffix == "ADKStore" and name.endswith("ADKMemoryStore"))
    )

    for store_name in store_names:
        if store_name[: -len(store_suffix)].lower() == expected_prefix:
            return cast("type[Any]", getattr(module, store_name))

    return cast("type[Any]", getattr(module, store_names[0])) if len(store_names) == 1 else None


def _adk_feature_enabled(config: Any, feature_key: str) -> bool:
    """Return whether an ADK feature flag enables its store and DDL."""
    adk_config = resolve_adk_config(config)
    enabled = adk_config.get(feature_key)
    return bool(enabled) if enabled is not None else True


def _ensure_adk_store_registration(config: Any) -> None:
    """Validate ADK store class resolution before extension migrations run."""
    if adk_sessions_migration_enabled(config):
        resolve_adk_store_class(config, "ADKStore")
    if adk_memory_migration_enabled(config):
        resolve_adk_store_class(config, "ADKMemoryStore")


def _validate_adk_extension_config(config: Any, settings: dict[str, Any]) -> None:
    """Reject ADK options that the adapter and explicit stores do not accept."""
    from sqlspec.config import ADKConfig
    from sqlspec.migrations.utils import resolve_extension_migration_mode

    resolve_extension_migration_mode(None, dict(settings), extension_name="adk")
    adapter_name = resolve_adapter_name(config)
    adapter_keys = _adk_adapter_config_keys(adapter_name)
    explicit_stores = tuple(
        store
        for store in (
            _adk_explicit_store_class(config, "adk_store_class"),
            _adk_explicit_store_class(config, "adk_memory_store_class"),
        )
        if store is not None
    )
    if adapter_keys is None and not explicit_stores:
        return
    accepted = set(ADKConfig.__required_keys__) | set(ADKConfig.__optional_keys__) | (adapter_keys or set())
    for store in explicit_stores:
        accepted.update(cast("frozenset[str]", getattr(store, "extension_config_options", frozenset())))
    unsupported = sorted(set(settings).difference(accepted))
    if unsupported:
        label = adapter_name or resolve_adapter_name(explicit_stores[0]) or explicit_stores[0].__name__
        keys = ", ".join(repr(key) for key in unsupported)
        msg = f"Unsupported ADK configuration key(s) for {label}: {keys}"
        raise ImproperConfigurationError(msg)


def _adk_adapter_config_keys(adapter_name: str | None) -> set[str] | None:
    """Return keys from a built-in adapter's ADK TypedDicts, or None when it has no ADK store module."""
    if adapter_name is None:
        return None
    module_name = f"sqlspec.adapters.{adapter_name}.adk.store"
    try:
        store_module = importlib.import_module(module_name)
    except ModuleNotFoundError as error:
        if error.name not in {module_name, module_name.rsplit(".", 1)[0], f"sqlspec.adapters.{adapter_name}"}:
            raise
        return None
    keys: set[str] = set()
    for exported_name in getattr(store_module, "__all__", ()):
        if exported_name.endswith("ADKConfig"):
            config_type = getattr(store_module, exported_name)
            keys.update(getattr(config_type, "__required_keys__", ()))
            keys.update(getattr(config_type, "__optional_keys__", ()))
    return keys


def _adk_explicit_store_class(config: Any, store_attr: str) -> type[Any] | None:
    """Return the store class set on a config instance or its class."""
    if config is None:
        return None
    explicit_store = None if isinstance(config, type) else getattr(config, store_attr, None)
    if not isinstance(explicit_store, type):
        target_cls = config if isinstance(config, type) else type(config)
        explicit_store = getattr(target_cls, store_attr, None)
    return explicit_store if isinstance(explicit_store, type) else None


def _raise_unsupported_config(config_type: str) -> NoReturn:
    msg = f"Unsupported config type for ADK migration: {config_type}"
    raise SQLSpecError(msg)


def _raise_store_import_failed(store_path: str, error: ImportError) -> NoReturn:
    msg = f"Failed to import ADK store class from {store_path}: {error}"
    raise SQLSpecError(msg) from error
