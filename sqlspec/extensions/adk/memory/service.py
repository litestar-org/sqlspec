"""SQLSpec-backed memory service for Google ADK."""

from typing import TYPE_CHECKING

from google.adk.memory.base_memory_service import BaseMemoryService, SearchMemoryResponse

from sqlspec.extensions.adk.memory.converters import records_to_memory_entries, session_to_memory_records
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from google.adk.memory.memory_entry import MemoryEntry
    from google.adk.sessions import Session

    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore

logger = get_logger("sqlspec.extensions.adk.memory.service")

__all__ = ("SQLSpecMemoryService", "SQLSpecSyncMemoryService")


class SQLSpecMemoryService(BaseMemoryService):
    """SQLSpec-backed implementation of BaseMemoryService.

    Provides memory entry storage using SQLSpec database adapters.
    Delegates all database operations to a store implementation.

    ADK BaseMemoryService defines two core methods:
    - add_session_to_memory(session) - Ingests session into memory (returns void)
    - search_memory(app_name, user_id, query) - Searches stored memories

    Args:
        store: Database store implementation (e.g., AsyncpgADKMemoryStore).

    Example:
        from sqlspec.adapters.asyncpg import AsyncpgConfig
        from sqlspec.adapters.asyncpg.adk.store import AsyncpgADKMemoryStore
        from sqlspec.extensions.adk.memory.service import SQLSpecMemoryService

        config = AsyncpgConfig(
            connection_config={"dsn": "postgresql://..."},
            extension_config={
                "adk": {
                    "memory_table": "adk_memory_entries",
                    "memory_use_fts": True,
                }
            }
        )
        store = AsyncpgADKMemoryStore(config)
        await store.ensure_tables()

        service = SQLSpecMemoryService(store)
        await service.add_session_to_memory(completed_session)

        response = await service.search_memory(
            app_name="my_app",
            user_id="user123",
            query="previous conversation about Python"
        )
    """

    def __init__(self, store: "BaseAsyncADKMemoryStore") -> None:
        """Initialize the memory service.

        Args:
            store: Database store implementation.
        """
        self._store = store

    @property
    def store(self) -> "BaseAsyncADKMemoryStore":
        """Return the database store."""
        return self._store

    async def add_session_to_memory(self, session: "Session") -> None:
        """Add a completed session to the memory store.

        Extracts all events with content from the session and stores them
        as searchable memory entries. Uses UPSERT to skip duplicates.

        The Session object contains app_name and user_id properties.
        Events are converted to memory records and bulk inserted via store.
        Returns void per ADK BaseMemoryService contract.

        Args:
            session: Completed ADK Session with events.

        Notes:
            - Events without content are skipped
            - Duplicate event_ids are silently ignored (idempotent)
            - Uses bulk insert for efficiency
        """
        records = session_to_memory_records(session)

        if not records:
            logger.debug(
                "No content to store for session %s (app=%s, user=%s)", session.id, session.app_name, session.user_id
            )
            return

        inserted_count = await self._store.insert_memory_entries(records)
        logger.debug(
            "Stored %d memory entries for session %s (total events: %d)", inserted_count, session.id, len(records)
        )

    async def search_memory(self, *, app_name: str, user_id: str, query: str) -> "SearchMemoryResponse":
        """Search memory entries by text query.

        Uses the store's configured search strategy (simple ILIKE or FTS).

        Args:
            app_name: Name of the application.
            user_id: ID of the user.
            query: Text query to search for.

        Returns:
            SearchMemoryResponse with memories: List[MemoryEntry].
        """
        records = await self._store.search_entries(query=query, app_name=app_name, user_id=user_id)

        memories = records_to_memory_entries(records)

        logger.debug("Found %d memories for query '%s' (app=%s, user=%s)", len(memories), query[:50], app_name, user_id)

        return SearchMemoryResponse(memories=memories)


class SQLSpecSyncMemoryService:
    """Synchronous SQLSpec-backed memory service.

    Provides memory entry storage using SQLSpec sync database adapters.
    This is a sync-compatible version for use with sync drivers like SQLite.

    Note: This does NOT inherit from BaseMemoryService since ADK's base class
    requires async methods. Use this for sync-only deployments.

    Args:
        store: Sync database store implementation.

    Example:
        from sqlspec.adapters.sqlite import SqliteConfig
        from sqlspec.adapters.sqlite.adk.store import SqliteADKMemoryStore
        from sqlspec.extensions.adk.memory.service import SQLSpecSyncMemoryService

        config = SqliteConfig(
            connection_config={"database": "app.db"},
            extension_config={
                "adk": {
                    "memory_table": "adk_memory_entries",
                }
            }
        )
        store = SqliteADKMemoryStore(config)
        store.ensure_tables()

        service = SQLSpecSyncMemoryService(store)
        service.add_session_to_memory(completed_session)

        memories = service.search_memory(
            app_name="my_app",
            user_id="user123",
            query="Python discussion"
        )
    """

    def __init__(self, store: "BaseSyncADKMemoryStore") -> None:
        """Initialize the sync memory service.

        Args:
            store: Sync database store implementation.
        """
        self._store = store

    @property
    def store(self) -> "BaseSyncADKMemoryStore":
        """Return the database store."""
        return self._store

    def add_session_to_memory(self, session: "Session") -> None:
        """Add a completed session to the memory store.

        Extracts all events with content from the session and stores them
        as searchable memory entries. Uses UPSERT to skip duplicates.

        Args:
            session: Completed ADK Session with events.
        """
        records = session_to_memory_records(session)

        if not records:
            logger.debug(
                "No content to store for session %s (app=%s, user=%s)", session.id, session.app_name, session.user_id
            )
            return

        inserted_count = self._store.insert_memory_entries(records)
        logger.debug(
            "Stored %d memory entries for session %s (total events: %d)", inserted_count, session.id, len(records)
        )

    def search_memory(self, *, app_name: str, user_id: str, query: str) -> list["MemoryEntry"]:
        """Search memory entries by text query.

        Args:
            app_name: Name of the application.
            user_id: ID of the user.
            query: Text query to search for.

        Returns:
            List of MemoryEntry objects.
        """
        records = self._store.search_entries(query=query, app_name=app_name, user_id=user_id)

        memories = records_to_memory_entries(records)

        logger.debug("Found %d memories for query '%s' (app=%s, user=%s)", len(memories), query[:50], app_name, user_id)

        return memories
