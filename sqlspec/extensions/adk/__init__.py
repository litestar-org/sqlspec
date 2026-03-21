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
    - SessionRecord: TypedDict for session database records
    - EventRecord: TypedDict for event database records
    - MemoryRecord: TypedDict for memory database records
    - ArtifactRecord: TypedDict for artifact metadata database records

Example (with extension_config):
    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.adapters.asyncpg.adk.store import AsyncpgADKStore
    from sqlspec.extensions.adk import SQLSpecSessionService

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://..."},
        extension_config={
            "adk": {
                "session_table": "my_sessions",
                "events_table": "my_events",
                "owner_id_column": "tenant_id INTEGER REFERENCES tenants(id)"
            }
        }
    )

    store = AsyncpgADKStore(config)
    await store.ensure_tables()

    service = SQLSpecSessionService(store)
    session = await service.create_session(
        app_name="my_app",
        user_id="user123",
        state={"key": "value"}
    )
"""

from sqlspec.config import ADKConfig
from sqlspec.extensions.adk._types import EventRecord, SessionRecord
from sqlspec.extensions.adk.artifact import (
    ArtifactRecord,
    BaseAsyncADKArtifactStore,
    BaseSyncADKArtifactStore,
    SQLSpecArtifactService,
)
from sqlspec.extensions.adk.memory import (
    BaseAsyncADKMemoryStore,
    BaseSyncADKMemoryStore,
    MemoryRecord,
    SQLSpecMemoryService,
    SQLSpecSyncMemoryService,
)
from sqlspec.extensions.adk.service import SQLSpecSessionService
from sqlspec.extensions.adk.store import BaseAsyncADKStore, BaseSyncADKStore

__all__ = (
    "ADKConfig",
    "ArtifactRecord",
    "BaseAsyncADKArtifactStore",
    "BaseAsyncADKMemoryStore",
    "BaseAsyncADKStore",
    "BaseSyncADKArtifactStore",
    "BaseSyncADKMemoryStore",
    "BaseSyncADKStore",
    "EventRecord",
    "MemoryRecord",
    "SQLSpecArtifactService",
    "SQLSpecMemoryService",
    "SQLSpecSessionService",
    "SQLSpecSyncMemoryService",
    "SessionRecord",
)
