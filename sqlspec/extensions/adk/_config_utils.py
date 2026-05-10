"""Shared ADK store configuration helpers."""

from typing import Any, Protocol, cast

from typing_extensions import NotRequired, TypedDict

__all__ = (
    "_ADKArtifactStoreConfig",
    "_ADKMemoryStoreConfig",
    "_ADKSessionStoreConfig",
    "_get_adk_artifact_store_config",
    "_get_adk_config_from_extension",
    "_get_adk_memory_store_config",
    "_get_adk_session_store_config",
)


class _ADKSessionStoreConfig(TypedDict):
    """Normalized ADK session store configuration."""

    session_table: str
    events_table: str
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


class _ADKConfigSource(Protocol):
    """Config-like object exposing ADK extension settings."""

    @property
    def extension_config(self) -> dict[str, Any]:
        """Return extension settings."""
        ...


def _get_adk_config_from_extension(config: _ADKConfigSource) -> dict[str, Any]:
    """Return a mutable copy of the ADK extension config."""

    return dict(cast("dict[str, Any]", config.extension_config.get("adk", {})))


def _get_adk_session_store_config(config: _ADKConfigSource) -> _ADKSessionStoreConfig:
    """Return normalized session store table settings."""

    adk_config = _get_adk_config_from_extension(config)
    session_table = adk_config.get("session_table")
    events_table = adk_config.get("events_table")
    result: _ADKSessionStoreConfig = {
        "session_table": str(session_table) if session_table is not None else "adk_sessions",
        "events_table": str(events_table) if events_table is not None else "adk_events",
    }
    owner_id = adk_config.get("owner_id_column")
    if owner_id is not None:
        result["owner_id_column"] = cast("str", owner_id)
    return result


def _get_adk_memory_store_config(config: _ADKConfigSource) -> _ADKMemoryStoreConfig:
    """Return normalized memory store settings."""

    adk_config = _get_adk_config_from_extension(config)
    enable_memory = adk_config.get("enable_memory")
    memory_table = adk_config.get("memory_table")
    use_fts = adk_config.get("memory_use_fts")
    max_results = adk_config.get("memory_max_results")

    result: _ADKMemoryStoreConfig = {
        "enable_memory": bool(enable_memory) if enable_memory is not None else True,
        "memory_table": str(memory_table) if memory_table is not None else "adk_memory_entries",
        "use_fts": bool(use_fts) if use_fts is not None else False,
        "max_results": int(max_results) if isinstance(max_results, int) else 20,
    }
    owner_id = adk_config.get("owner_id_column")
    if owner_id is not None:
        result["owner_id_column"] = cast("str", owner_id)
    return result


def _get_adk_artifact_store_config(config: _ADKConfigSource) -> _ADKArtifactStoreConfig:
    """Return normalized artifact store settings."""

    adk_config = _get_adk_config_from_extension(config)
    artifact_table = adk_config.get("artifact_table")
    return {"artifact_table": str(artifact_table) if artifact_table is not None else "adk_artifact_versions"}
