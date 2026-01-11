"""Google ADK memory backend extension for SQLSpec.

Provides memory entry storage for Google Agent Development Kit using
SQLSpec database adapters. Memory stores are used to persist conversational
context across agent sessions for long-term recall.

Public API exports:
    - SQLSpecMemoryService: Main async service class implementing BaseMemoryService
    - SQLSpecSyncMemoryService: Sync service for sync adapters
    - BaseAsyncADKMemoryStore: Base class for async database store implementations
    - BaseSyncADKMemoryStore: Base class for sync database store implementations
    - MemoryRecord: TypedDict for memory database records
    - extract_content_text: Helper to extract searchable text from Content
    - session_to_memory_records: Convert Session to memory records
    - record_to_memory_entry: Convert database record to MemoryEntry

Example (async):
    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.adapters.asyncpg.adk.store import AsyncpgADKMemoryStore
    from sqlspec.extensions.adk.memory import SQLSpecMemoryService

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://..."},
        extension_config={
            "adk": {
                "memory_table": "adk_memory_entries",
                "memory_use_fts": True,
                "memory_max_results": 50,
            }
        }
    )

    store = AsyncpgADKMemoryStore(config)
    await store.ensure_tables()

    service = SQLSpecMemoryService(store)

    # Store completed session as memories
    await service.add_session_to_memory(completed_session)

    # Search memories
    response = await service.search_memory(
        app_name="my_app",
        user_id="user123",
        query="previous discussion about Python"
    )
    for entry in response.memories:
        print(entry.content)
"""

from sqlspec.extensions.adk.memory._types import MemoryRecord
from sqlspec.extensions.adk.memory.converters import (
    extract_content_text,
    record_to_memory_entry,
    session_to_memory_records,
)
from sqlspec.extensions.adk.memory.service import SQLSpecMemoryService, SQLSpecSyncMemoryService
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore

__all__ = (
    "BaseAsyncADKMemoryStore",
    "BaseSyncADKMemoryStore",
    "MemoryRecord",
    "SQLSpecMemoryService",
    "SQLSpecSyncMemoryService",
    "extract_content_text",
    "record_to_memory_entry",
    "session_to_memory_records",
)
