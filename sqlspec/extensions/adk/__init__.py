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
    - StoredSession: TypedDict for session database records
    - StoredEvent: TypedDict for event database records
    - StoredMemory: TypedDict for memory database records
    - StoredArtifact: TypedDict for artifact metadata database records
"""

from sqlspec.config import ADKConfig
from sqlspec.extensions.adk._types import StoredEvent, StoredSession
from sqlspec.extensions.adk.artifact import (
    BaseAsyncADKArtifactStore,
    BaseSyncADKArtifactStore,
    SQLSpecArtifactService,
    StoredArtifact,
)
from sqlspec.extensions.adk.maintenance import (
    MaintenanceReport,
    PruneReport,
    maintain_tables,
    maintain_tables_sync,
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
from sqlspec.extensions.adk.store import BaseAsyncADKStore, BaseSyncADKStore

__all__ = (
    "ADKConfig",
    "BaseAsyncADKArtifactStore",
    "BaseAsyncADKMemoryStore",
    "BaseAsyncADKStore",
    "BaseSyncADKArtifactStore",
    "BaseSyncADKMemoryStore",
    "BaseSyncADKStore",
    "MaintenanceReport",
    "PruneReport",
    "SQLSpecArtifactService",
    "SQLSpecMemoryService",
    "SQLSpecSessionService",
    "SQLSpecSyncMemoryService",
    "StoredArtifact",
    "StoredEvent",
    "StoredMemory",
    "StoredSession",
    "maintain_tables",
    "maintain_tables_sync",
    "prune_events",
    "prune_events_sync",
    "prune_memory",
    "prune_memory_sync",
    "prune_sessions",
    "prune_sessions_sync",
    "prune_user_state",
    "prune_user_state_sync",
)
