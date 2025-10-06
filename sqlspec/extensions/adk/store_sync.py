"""Sync base ADK store for Google Agent Development Kit session/event storage."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from sqlspec.extensions.adk._types import EventRecord, SessionRecord

if TYPE_CHECKING:
    from sqlspec.protocols import DatabaseConfig

__all__ = ("BaseSyncADKStore",)

ConfigT = TypeVar("ConfigT", bound="DatabaseConfig[Any, Any, Any]")


class BaseSyncADKStore(ABC, Generic[ConfigT]):
    """Abstract base class for sync ADK session/event storage.

    Provides interface for storing and retrieving Google ADK sessions and events
    in database-backed storage. Implementations must provide DDL methods and
    CRUD operations for both sessions and events tables.

    Args:
        config: Database configuration instance.
        sessions_table: Name of the sessions table.
        events_table: Name of the events table.
    """

    __slots__ = ("_config", "_events_table", "_sessions_table")

    def __init__(
        self,
        config: ConfigT,
        sessions_table: str = "adk_sessions",
        events_table: str = "adk_events",
    ) -> None:
        """Initialize sync ADK store.

        Args:
            config: Database configuration instance.
            sessions_table: Name of the sessions table.
            events_table: Name of the events table.
        """
        self._config = config
        self._sessions_table = sessions_table
        self._events_table = events_table

    @abstractmethod
    def _get_create_sessions_table_sql(self) -> str:
        """Get SQL to create sessions table.

        Returns:
            SQL statement to create adk_sessions table with indexes.
        """
        ...

    @abstractmethod
    def _get_create_events_table_sql(self) -> str:
        """Get SQL to create events table.

        Returns:
            SQL statement to create adk_events table with indexes.
        """
        ...

    @abstractmethod
    def _get_drop_tables_sql(self) -> "list[str]":
        """Get SQL to drop tables.

        Returns:
            List of SQL statements to drop tables and indexes.
        """
        ...

    @abstractmethod
    def create_tables(self) -> None:
        """Create both sessions and events tables if they don't exist."""
        ...

    @abstractmethod
    def drop_tables(self) -> None:
        """Drop both sessions and events tables."""
        ...

    @abstractmethod
    def create_session(
        self,
        *,
        session_id: str,
        app_name: str,
        user_id: str,
        state: "dict[str, Any] | None" = None,
    ) -> SessionRecord:
        """Create a new session.

        Args:
            session_id: Unique session identifier.
            app_name: Application name.
            user_id: User identifier.
            state: Initial session state.

        Returns:
            Created session record.
        """
        ...

    @abstractmethod
    def get_session(
        self,
        *,
        session_id: str,
    ) -> "SessionRecord | None":
        """Get session by ID.

        Args:
            session_id: Session identifier.

        Returns:
            Session record or None if not found.
        """
        ...

    @abstractmethod
    def update_session_state(
        self,
        *,
        session_id: str,
        state: "dict[str, Any]",
    ) -> SessionRecord:
        """Update session state (merge with existing).

        Args:
            session_id: Session identifier.
            state: State update to merge.

        Returns:
            Updated session record.
        """
        ...

    @abstractmethod
    def delete_session(
        self,
        *,
        session_id: str,
    ) -> None:
        """Delete session and all associated events (cascade).

        Args:
            session_id: Session identifier.
        """
        ...

    @abstractmethod
    def list_sessions(
        self,
        *,
        app_name: str,
        user_id: str,
    ) -> "list[SessionRecord]":
        """List all sessions for a user in an app.

        Args:
            app_name: Application name.
            user_id: User identifier.

        Returns:
            List of session records.
        """
        ...

    @abstractmethod
    def create_event(
        self,
        *,
        event_id: str,
        session_id: str,
        app_name: str,
        user_id: str,
        author: "str | None" = None,
        actions: "bytes | None" = None,
        content: "dict[str, Any] | None" = None,
        **kwargs: Any,
    ) -> EventRecord:
        """Create a new event.

        Args:
            event_id: Unique event identifier.
            session_id: Session identifier.
            app_name: Application name.
            user_id: User identifier.
            author: Event author (user/assistant/system).
            actions: Pickled actions object.
            content: Event content (JSONB).
            **kwargs: Additional optional fields.

        Returns:
            Created event record.
        """
        ...

    @abstractmethod
    def list_events(
        self,
        *,
        session_id: str,
    ) -> "list[EventRecord]":
        """List events for a session ordered by timestamp.

        Args:
            session_id: Session identifier.

        Returns:
            List of event records ordered by timestamp ASC.
        """
        ...

    @abstractmethod
    def delete_events_by_session(
        self,
        *,
        session_id: str,
    ) -> None:
        """Delete all events for a session.

        Args:
            session_id: Session identifier.
        """
        ...
