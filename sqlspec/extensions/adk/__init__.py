"""Google ADK session, memory, and artifact backend extension for SQLSpec.

Provides session, event, memory, and artifact storage for Google Agent Development Kit
using SQLSpec database adapters.

Public API exports:
    - ADKConfig: TypedDict for extension config (type-safe configuration)
    - SQLSpecSessionService: Main service class implementing BaseSessionService
    - SQLSpecMemoryService: Main async service class implementing BaseMemoryService
    - SQLSpecSyncMemoryService: Sync memory service for sync adapters
    - SQLSpecArtifactService: Artifact service implementing BaseArtifactService
    - BaseAsyncADKStore: Base class for async database store implementations
    - BaseSyncADKStore: Base class for sync database store implementations
    - BaseAsyncADKMemoryStore: Base class for async memory store implementations
    - BaseSyncADKMemoryStore: Base class for sync memory store implementations
    - BaseAsyncADKArtifactStore: Base class for async artifact metadata stores
    - BaseSyncADKArtifactStore: Base class for sync artifact metadata stores
    - SessionOrderBy: Literal of the timestamp columns a session listing may order by
    - normalize_session_list_options: Shared validator for session list ordering and paging
    - StoredSession: TypedDict for session database records
    - StoredEvent: TypedDict for event database records
    - StoredMemory: TypedDict for memory database records
    - StoredArtifact: TypedDict for artifact metadata database records
"""

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlspec.config import ADKConfig
    from sqlspec.extensions.adk._types import SessionOrderBy, StoredEvent, StoredSession
    from sqlspec.extensions.adk.artifact import (
        BaseAsyncADKArtifactStore,
        BaseSyncADKArtifactStore,
        SQLSpecArtifactService,
        StoredArtifact,
    )
    from sqlspec.extensions.adk.maintenance import (
        PruneReport,
        prune_artifacts,
        prune_artifacts_sync,
        prune_events,
        prune_events_sync,
        prune_memory,
        prune_memory_sync,
        prune_sessions,
        prune_sessions_sync,
        prune_user_state,
        prune_user_state_sync,
    )
    from sqlspec.extensions.adk.memory import (
        BaseAsyncADKMemoryStore,
        BaseSyncADKMemoryStore,
        SQLSpecMemoryService,
        SQLSpecSyncMemoryService,
        StoredMemory,
    )
    from sqlspec.extensions.adk.service import SQLSpecSessionService
    from sqlspec.extensions.adk.store import BaseAsyncADKStore, BaseSyncADKStore, normalize_session_list_options

__all__ = (
    "ADKConfig",
    "BaseAsyncADKArtifactStore",
    "BaseAsyncADKMemoryStore",
    "BaseAsyncADKStore",
    "BaseSyncADKArtifactStore",
    "BaseSyncADKMemoryStore",
    "BaseSyncADKStore",
    "PruneReport",
    "SQLSpecArtifactService",
    "SQLSpecMemoryService",
    "SQLSpecSessionService",
    "SQLSpecSyncMemoryService",
    "SessionOrderBy",
    "StoredArtifact",
    "StoredEvent",
    "StoredMemory",
    "StoredSession",
    "normalize_session_list_options",
    "prune_artifacts",
    "prune_artifacts_sync",
    "prune_events",
    "prune_events_sync",
    "prune_memory",
    "prune_memory_sync",
    "prune_sessions",
    "prune_sessions_sync",
    "prune_user_state",
    "prune_user_state_sync",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "ADKConfig": ("sqlspec.config", "ADKConfig"),
    "BaseAsyncADKArtifactStore": ("sqlspec.extensions.adk.artifact", "BaseAsyncADKArtifactStore"),
    "BaseAsyncADKMemoryStore": ("sqlspec.extensions.adk.memory", "BaseAsyncADKMemoryStore"),
    "BaseAsyncADKStore": ("sqlspec.extensions.adk.store", "BaseAsyncADKStore"),
    "BaseSyncADKArtifactStore": ("sqlspec.extensions.adk.artifact", "BaseSyncADKArtifactStore"),
    "BaseSyncADKMemoryStore": ("sqlspec.extensions.adk.memory", "BaseSyncADKMemoryStore"),
    "BaseSyncADKStore": ("sqlspec.extensions.adk.store", "BaseSyncADKStore"),
    "PruneReport": ("sqlspec.extensions.adk.maintenance", "PruneReport"),
    "SQLSpecArtifactService": ("sqlspec.extensions.adk.artifact", "SQLSpecArtifactService"),
    "SQLSpecMemoryService": ("sqlspec.extensions.adk.memory", "SQLSpecMemoryService"),
    "SQLSpecSessionService": ("sqlspec.extensions.adk.service", "SQLSpecSessionService"),
    "SQLSpecSyncMemoryService": ("sqlspec.extensions.adk.memory", "SQLSpecSyncMemoryService"),
    "SessionOrderBy": ("sqlspec.extensions.adk._types", "SessionOrderBy"),
    "StoredArtifact": ("sqlspec.extensions.adk.artifact", "StoredArtifact"),
    "StoredEvent": ("sqlspec.extensions.adk._types", "StoredEvent"),
    "StoredMemory": ("sqlspec.extensions.adk.memory", "StoredMemory"),
    "StoredSession": ("sqlspec.extensions.adk._types", "StoredSession"),
    "normalize_session_list_options": ("sqlspec.extensions.adk.store", "normalize_session_list_options"),
    "prune_artifacts": ("sqlspec.extensions.adk.maintenance", "prune_artifacts"),
    "prune_artifacts_sync": ("sqlspec.extensions.adk.maintenance", "prune_artifacts_sync"),
    "prune_events": ("sqlspec.extensions.adk.maintenance", "prune_events"),
    "prune_events_sync": ("sqlspec.extensions.adk.maintenance", "prune_events_sync"),
    "prune_memory": ("sqlspec.extensions.adk.maintenance", "prune_memory"),
    "prune_memory_sync": ("sqlspec.extensions.adk.maintenance", "prune_memory_sync"),
    "prune_sessions": ("sqlspec.extensions.adk.maintenance", "prune_sessions"),
    "prune_sessions_sync": ("sqlspec.extensions.adk.maintenance", "prune_sessions_sync"),
    "prune_user_state": ("sqlspec.extensions.adk.maintenance", "prune_user_state"),
    "prune_user_state_sync": ("sqlspec.extensions.adk.maintenance", "prune_user_state_sync"),
}


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_path, attr_name = _EXPORTS[name]
    module = import_module(module_path)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_EXPORTS))
