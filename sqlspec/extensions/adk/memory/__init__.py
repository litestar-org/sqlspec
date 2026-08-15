"""Google ADK memory backend extension for SQLSpec.

Provides memory entry storage for Google Agent Development Kit using
SQLSpec database adapters. Memory stores are used to persist conversational
context across agent sessions for long-term recall.

Public API exports:
    - SQLSpecMemoryService: Main async service class implementing BaseMemoryService
    - SQLSpecSyncMemoryService: Sync service for sync adapters
    - BaseAsyncADKMemoryStore: Base class for async database store implementations
    - BaseSyncADKMemoryStore: Base class for sync database store implementations
    - StoredMemory: TypedDict for memory database records
    - extract_content_text: Helper to extract searchable text from Content
    - session_to_memory_records: Convert Session to memory records
    - record_to_memory_entry: Convert database record to MemoryEntry
"""

from sqlspec.extensions.adk.memory._types import StoredMemory
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
    "SQLSpecMemoryService",
    "SQLSpecSyncMemoryService",
    "StoredMemory",
    "extract_content_text",
    "record_to_memory_entry",
    "session_to_memory_records",
)
