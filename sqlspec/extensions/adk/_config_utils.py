"""ADK store configuration helpers."""

import importlib
from typing import Any, NoReturn, Protocol, cast

from typing_extensions import NotRequired, TypedDict

from sqlspec.exceptions import SQLSpecError
from sqlspec.extensions.adk._versioning import ADKVersionPlan, resolve_adk_version_plan
from sqlspec.utils.module_loader import import_string

__all__ = (
    "_ADKArtifactStoreConfig",
    "_ADKMemoryStoreConfig",
    "_ADKSessionStoreConfig",
    "_get_adk_adapter_store_class",
    "_get_adk_artifact_store_config",
    "_get_adk_config_from_extension",
    "_get_adk_memory_migration_store_class",
    "_get_adk_memory_store_config",
    "_get_adk_session_store_config",
    "_get_adk_version_plan",
    "_is_adk_memory_migration_enabled",
    "_validate_adk_store_registration",
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
    owner_id_column: NotRequired[str]


class _ADKArtifactStoreConfig(TypedDict):
    """Normalized ADK artifact store configuration."""

    artifact_table: str
    storage_uri: NotRequired[str]


class _ADKConfigSource(Protocol):
    """Config-like object exposing ADK extension settings."""

    @property
    def extension_config(self) -> dict[str, Any]:
        """Return extension settings."""
        ...


def _get_adk_config_from_extension(config: _ADKConfigSource) -> dict[str, Any]:
    """Return a mutable copy of the ADK extension config."""

    return dict(cast("dict[str, Any]", config.extension_config.get("adk", {})))


def _get_adk_config_section(adk_config: dict[str, Any], name: str) -> dict[str, Any]:
    """Return a mutable nested ADK config section."""

    value = adk_config.get(name)
    return dict(cast("dict[str, Any]", value)) if isinstance(value, dict) else {}


def _get_first_value(*values: Any, default: Any = None) -> Any:
    """Return the first non-None value."""

    for value in values:
        if value is not None:
            return value
    return default


def _get_adk_session_store_config(config: _ADKConfigSource) -> _ADKSessionStoreConfig:
    """Return normalized session store table settings."""

    adk_config = _get_adk_config_from_extension(config)
    schema_config = _get_adk_config_section(adk_config, "schema")
    session_table = _get_first_value(schema_config.get("session_table"), adk_config.get("session_table"))
    events_table = _get_first_value(schema_config.get("events_table"), adk_config.get("events_table"))
    app_state_table = _get_first_value(schema_config.get("app_state_table"), adk_config.get("app_state_table"))
    user_state_table = _get_first_value(schema_config.get("user_state_table"), adk_config.get("user_state_table"))
    metadata_table = _get_first_value(schema_config.get("metadata_table"), adk_config.get("metadata_table"))
    result: _ADKSessionStoreConfig = {
        "session_table": str(session_table) if session_table is not None else "adk_sessions",
        "events_table": str(events_table) if events_table is not None else "adk_events",
        "app_state_table": str(app_state_table) if app_state_table is not None else "adk_app_states",
        "user_state_table": str(user_state_table) if user_state_table is not None else "adk_user_states",
        "metadata_table": str(metadata_table) if metadata_table is not None else "adk_internal_metadata",
    }
    owner_id = _get_first_value(schema_config.get("owner_id_column"), adk_config.get("owner_id_column"))
    if owner_id is not None:
        result["owner_id_column"] = cast("str", owner_id)
    return result


def _get_adk_memory_store_config(config: _ADKConfigSource) -> _ADKMemoryStoreConfig:
    """Return normalized memory store settings."""

    adk_config = _get_adk_config_from_extension(config)
    schema_config = _get_adk_config_section(adk_config, "schema")
    memory_config = _get_adk_config_section(adk_config, "memory")
    search_config = _get_adk_config_section(adk_config, "search")
    nested_memory_search_config = _get_adk_config_section(memory_config, "search")
    enable_memory = _get_first_value(memory_config.get("enabled"), adk_config.get("enable_memory"))
    memory_table = _get_first_value(
        memory_config.get("table"), schema_config.get("memory_table"), adk_config.get("memory_table")
    )
    use_fts = _get_first_value(
        nested_memory_search_config.get("use_fts"),
        search_config.get("use_fts"),
        memory_config.get("use_fts"),
        adk_config.get("memory_use_fts"),
    )
    max_results = _get_first_value(
        memory_config.get("max_results"),
        nested_memory_search_config.get("max_results"),
        search_config.get("max_results"),
        adk_config.get("memory_max_results"),
    )

    result: _ADKMemoryStoreConfig = {
        "enable_memory": bool(enable_memory) if enable_memory is not None else True,
        "memory_table": str(memory_table) if memory_table is not None else "adk_memory_entries",
        "use_fts": bool(use_fts) if use_fts is not None else False,
        "max_results": int(max_results) if type(max_results) is int else 20,
    }
    owner_id = _get_first_value(schema_config.get("owner_id_column"), adk_config.get("owner_id_column"))
    if owner_id is not None:
        result["owner_id_column"] = cast("str", owner_id)
    return result


def _get_adk_artifact_store_config(config: _ADKConfigSource) -> _ADKArtifactStoreConfig:
    """Return normalized artifact store settings."""

    adk_config = _get_adk_config_from_extension(config)
    schema_config = _get_adk_config_section(adk_config, "schema")
    artifact_config = _get_adk_config_section(adk_config, "artifact")
    artifact_table = _get_first_value(
        artifact_config.get("table"), schema_config.get("artifact_table"), adk_config.get("artifact_table")
    )
    result: _ADKArtifactStoreConfig = {
        "artifact_table": str(artifact_table) if artifact_table is not None else "adk_artifact_versions"
    }
    storage_uri = _get_first_value(artifact_config.get("storage_uri"), adk_config.get("artifact_storage_uri"))
    if storage_uri is not None:
        result["storage_uri"] = str(storage_uri)
    return result


def _get_adk_version_plan(config: _ADKConfigSource) -> ADKVersionPlan:
    """Return normalized ADK schema and payload version settings."""

    return resolve_adk_version_plan(_get_adk_config_from_extension(config))


def _resolve_adk_store_path(config: Any, store_suffix: str) -> str:
    """Return the adapter-specific ADK store import path."""

    config_class = type(config)
    config_module = config_class.__module__
    config_name = config_class.__name__

    if not config_module.startswith("sqlspec.adapters."):
        _raise_unsupported_config(f"{config_module}.{config_name}")

    adapter_name = config_module.split(".")[2]
    store_class_name = config_name.replace("Config", store_suffix)
    return f"sqlspec.adapters.{adapter_name}.adk.{store_class_name}"


def _get_adk_adapter_store_class(config: Any, store_suffix: str) -> Any:
    """Import an adapter-specific ADK store class for a config."""

    store_path = _resolve_adk_store_path(config, store_suffix)
    try:
        return import_string(store_path)
    except ImportError as e:
        store_class = _get_adk_exported_store_class(config, store_suffix)
        if store_class is not None:
            return store_class
        _raise_store_import_failed(store_path, e)


def _get_adk_memory_migration_store_class(config: Any) -> Any | None:
    """Import the ADK memory store class when the adapter provides one."""

    store_path = _resolve_adk_store_path(config, "ADKMemoryStore")
    try:
        return import_string(store_path)
    except ImportError:
        return _get_adk_exported_store_class(config, "ADKMemoryStore")


def _get_adk_exported_store_class(config: Any, store_suffix: str) -> Any | None:
    """Return an ADK store export that matches a config name case-insensitively."""

    config_class = type(config)
    config_module = config_class.__module__
    config_name = config_class.__name__
    if not config_module.startswith("sqlspec.adapters."):
        return None

    adapter_name = config_module.split(".")[2]
    module_path = f"sqlspec.adapters.{adapter_name}.adk"
    try:
        module = importlib.import_module(module_path)
    except ImportError:
        return None

    expected_prefix = config_name.removesuffix("Config").lower()
    store_names = tuple(name for name in getattr(module, "__all__", ()) if name.endswith(store_suffix))

    for store_name in store_names:
        if store_name[: -len(store_suffix)].lower() == expected_prefix:
            return getattr(module, store_name)

    return getattr(module, store_names[0]) if len(store_names) == 1 else None


def _is_adk_memory_migration_enabled(config: Any) -> bool:
    """Return whether ADK memory DDL should be included for this config."""

    adk_config = _get_adk_config_from_extension(cast("_ADKConfigSource", config))
    schema_config = _get_adk_config_section(adk_config, "schema")
    memory_config = _get_adk_config_section(adk_config, "memory")
    include_memory = _get_first_value(
        schema_config.get("include_memory_migration"),
        memory_config.get("include_migration"),
        adk_config.get("include_memory_migration"),
    )
    if include_memory is not None:
        return bool(include_memory)
    return bool(_get_first_value(memory_config.get("enabled"), adk_config.get("enable_memory"), default=True))


def _validate_adk_store_registration(config: Any) -> None:
    """Validate ADK store class resolution before extension migrations run."""

    _get_adk_adapter_store_class(config, "ADKStore")
    if _is_adk_memory_migration_enabled(config):
        _get_adk_adapter_store_class(config, "ADKMemoryStore")


def _raise_unsupported_config(config_type: str) -> NoReturn:
    msg = f"Unsupported config type for ADK migration: {config_type}"
    raise SQLSpecError(msg)


def _raise_store_import_failed(store_path: str, error: ImportError) -> NoReturn:
    msg = f"Failed to import ADK store class from {store_path}: {error}"
    raise SQLSpecError(msg) from error
