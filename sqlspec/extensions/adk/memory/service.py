"""SQLSpec-backed memory service for Google ADK."""

import inspect
from typing import TYPE_CHECKING, Any, cast

from google.adk.memory.base_memory_service import BaseMemoryService, SearchMemoryResponse

from sqlspec.extensions.adk.memory.converters import (
    memory_entry_to_record,
    records_to_memory_entries,
    session_to_memory_records,
)
from sqlspec.utils.logging import get_logger
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from google.adk.events.event import Event
    from google.adk.memory.memory_entry import MemoryEntry
    from google.adk.sessions import Session

    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore

__all__ = ("SQLSpecMemoryService", "SQLSpecSyncMemoryService")

logger = get_logger("sqlspec.extensions.adk.memory.service")


class SQLSpecMemoryService(BaseMemoryService):
    """SQLSpec-backed implementation of BaseMemoryService.

    Provides memory entry storage using SQLSpec database adapters.
    Delegates all database operations to a store implementation.

    ADK BaseMemoryService defines two core methods:
        - add_session_to_memory(session) - Ingests session into memory (returns void)
        - search_memory(app_name, user_id, query) - Searches stored memories

    Args:
        store: Database store implementation.
    """

    def __init__(self, store: "BaseAsyncADKMemoryStore | BaseSyncADKMemoryStore") -> None:
        """Initialize the memory service.

        Args:
            store: Database store implementation.
        """
        self._store = store

    @property
    def store(self) -> "BaseAsyncADKMemoryStore | BaseSyncADKMemoryStore":
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
        """
        records = session_to_memory_records(session)

        if not records:
            logger.debug(
                "No content to store for session %s (app=%s, user=%s)", session.id, session.app_name, session.user_id
            )
            return

        inserted_count = await self._call_store("insert_memory_entries", records)
        logger.debug(
            "Stored %d memory entries for session %s (total events: %d)", inserted_count, session.id, len(records)
        )

    async def add_events_to_memory(
        self,
        *,
        app_name: str,
        user_id: str,
        events: "Sequence[Event]",
        session_id: "str | None" = None,
        custom_metadata: "Mapping[str, object] | None" = None,
    ) -> None:
        """Add an explicit list of events to the memory service.

        Same Event-to-MemoryRecord extraction logic as
        ``add_session_to_memory``, but operates on a sequence of Events
        directly (no Session wrapper needed).

        Args:
            app_name: The application name for memory scope.
            user_id: The user ID for memory scope.
            events: The events to add to memory.
            session_id: Optional session ID for memory scope/partitioning.
                If None, memory entries are user-scoped only.
            custom_metadata: Optional portable metadata stored in
                ``MemoryRecord.metadata_json``.
        """
        from sqlspec.extensions.adk.memory.converters import event_to_memory_record

        metadata_dict = dict(custom_metadata) if custom_metadata else None
        records = []
        for event in events:
            record = event_to_memory_record(
                event=event, session_id=session_id or "", app_name=app_name, user_id=user_id
            )
            if record is not None:
                if metadata_dict:
                    record["metadata_json"] = metadata_dict
                records.append(record)

        if not records:
            logger.debug(
                "No content to store for events (app=%s, user=%s, count=%d)", app_name, user_id, len(list(events))
            )
            return

        inserted_count = await self._call_store("insert_memory_entries", records)
        logger.debug(
            "Stored %d memory entries from %d events (app=%s, user=%s)", inserted_count, len(records), app_name, user_id
        )

    async def add_memory(
        self,
        *,
        app_name: str,
        user_id: str,
        memories: "Sequence[MemoryEntry]",
        custom_metadata: "Mapping[str, object] | None" = None,
    ) -> None:
        """Add explicit memory items directly to the memory service.

        Each entry's ``content`` is serialized to ``content_json``, text is
        extracted from ``content.parts`` for ``content_text``, and
        ``custom_metadata`` merges the entry-level ``entry.custom_metadata``
        with the call-level ``custom_metadata`` parameter.

        Args:
            app_name: The application name for memory scope.
            user_id: The user ID for memory scope.
            memories: Explicit memory items to add.
            custom_metadata: Optional portable metadata for memory writes.
                Merged with each entry's ``custom_metadata``.
        """
        call_metadata = dict(custom_metadata) if custom_metadata else {}
        records = []
        for entry in memories:
            record = memory_entry_to_record(
                entry=entry, app_name=app_name, user_id=user_id, extra_metadata=call_metadata
            )
            if record is not None:
                records.append(record)

        if not records:
            logger.debug("No content to store for memories (app=%s, user=%s)", app_name, user_id)
            return

        inserted_count = await self._call_store("insert_memory_entries", records)
        logger.debug(
            "Stored %d memory entries from %d memories (app=%s, user=%s)",
            inserted_count,
            len(records),
            app_name,
            user_id,
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
        records = await self._call_store("search_entries", query=query, app_name=app_name, user_id=user_id)

        memories = records_to_memory_entries(records)

        logger.debug("Found %d memories for query '%s' (app=%s, user=%s)", len(memories), query[:50], app_name, user_id)

        return SearchMemoryResponse(memories=memories)

    async def _call_store(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        """Call an async store method or bridge a sync store method."""
        method = getattr(self._store, method_name)
        if inspect.iscoroutinefunction(method):
            return await method(*args, **kwargs)
        sync_method = method
        if TYPE_CHECKING:
            sync_method = cast("Callable[..., Any]", method)
        return await async_(sync_method)(*args, **kwargs)


class SQLSpecSyncMemoryService:
    """Synchronous SQLSpec-backed memory service.

    Provides memory entry storage using SQLSpec sync database adapters.
    This is a sync-compatible version for use with sync drivers like SQLite.

    Note: This does NOT inherit from BaseMemoryService since ADK's base class
    requires async methods. Use this for sync-only deployments.

    Args:
        store: Sync database store implementation.
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
