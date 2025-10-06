"""Base store classes for ADK session backend (sync and async)."""

import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Final, Generic, TypeVar

from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from datetime import datetime

    from sqlspec.extensions.adk._types import EventRecord, SessionRecord

ConfigT = TypeVar("ConfigT")

logger = get_logger("extensions.adk.store")

__all__ = ("BaseADKStore", "BaseSyncADKStore")

VALID_TABLE_NAME_PATTERN: Final = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
MAX_TABLE_NAME_LENGTH: Final = 63


class BaseADKStore(ABC, Generic[ConfigT]):
    """Base class for async SQLSpec-backed ADK session stores.

    Implements storage operations for Google ADK sessions and events using
    SQLSpec database adapters with async/await.

    This abstract base class provides common functionality for all database-specific
    store implementations including:
    - Connection management via SQLSpec configs
    - Table name validation
    - Session and event CRUD operations

    Subclasses must implement dialect-specific SQL queries and will be created
    in each adapter directory (e.g., sqlspec/adapters/asyncpg/adk/store.py).

    Args:
        config: SQLSpec database configuration (async).
        session_table: Name of the sessions table. Defaults to "adk_sessions".
        events_table: Name of the events table. Defaults to "adk_events".
    """

    __slots__ = ("_config", "_events_table", "_session_table")

    def __init__(
        self,
        config: ConfigT,
        session_table: str = "adk_sessions",
        events_table: str = "adk_events",
    ) -> None:
        """Initialize the ADK store.

        Args:
            config: SQLSpec database configuration.
            session_table: Name of the sessions table.
            events_table: Name of the events table.
        """
        self._validate_table_name(session_table)
        self._validate_table_name(events_table)
        self._config = config
        self._session_table = session_table
        self._events_table = events_table

    @property
    def config(self) -> ConfigT:
        """Return the database configuration."""
        return self._config

    @property
    def session_table(self) -> str:
        """Return the sessions table name."""
        return self._session_table

    @property
    def events_table(self) -> str:
        """Return the events table name."""
        return self._events_table

    @abstractmethod
    async def create_session(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: "dict[str, Any]",
    ) -> "SessionRecord":
        """Create a new session.

        Args:
            session_id: Unique identifier for the session.
            app_name: Name of the application.
            user_id: ID of the user.
            state: Session state dictionary.

        Returns:
            The created session record.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_session(self, session_id: str) -> "SessionRecord | None":
        """Get a session by ID.

        Args:
            session_id: Session identifier.

        Returns:
            Session record if found, None otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    async def update_session_state(
        self,
        session_id: str,
        state: "dict[str, Any]",
    ) -> None:
        """Update session state.

        Args:
            session_id: Session identifier.
            state: New state dictionary.
        """
        raise NotImplementedError

    @abstractmethod
    async def list_sessions(
        self,
        app_name: str,
        user_id: str,
    ) -> "list[SessionRecord]":
        """List all sessions for an app and user.

        Args:
            app_name: Name of the application.
            user_id: ID of the user.

        Returns:
            List of session records.
        """
        raise NotImplementedError

    @abstractmethod
    async def delete_session(self, session_id: str) -> None:
        """Delete a session and its events.

        Args:
            session_id: Session identifier.
        """
        raise NotImplementedError

    @abstractmethod
    async def append_event(self, event_record: "EventRecord") -> None:
        """Append an event to a session.

        Args:
            event_record: Event record to store.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_events(
        self,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "list[EventRecord]":
        """Get events for a session.

        Args:
            session_id: Session identifier.
            after_timestamp: Only return events after this time.
            limit: Maximum number of events to return.

        Returns:
            List of event records ordered by timestamp ascending.
        """
        raise NotImplementedError

    @abstractmethod
    async def create_tables(self) -> None:
        """Create the sessions and events tables if they don't exist."""
        raise NotImplementedError

    @abstractmethod
    def _get_create_sessions_table_sql(self) -> str:
        """Get the CREATE TABLE SQL for the sessions table.

        Returns:
            SQL statement to create the sessions table.
        """
        raise NotImplementedError

    @abstractmethod
    def _get_create_events_table_sql(self) -> str:
        """Get the CREATE TABLE SQL for the events table.

        Returns:
            SQL statement to create the events table.
        """
        raise NotImplementedError

    @abstractmethod
    def _get_drop_tables_sql(self) -> "list[str]":
        """Get the DROP TABLE SQL statements for this database dialect.

        Returns:
            List of SQL statements to drop the tables and all indexes.
            Order matters: drop events table before sessions table due to FK.

        Notes:
            Should use IF EXISTS or dialect-specific error handling
            to allow idempotent migrations.
        """
        raise NotImplementedError

    @staticmethod
    def _validate_table_name(table_name: str) -> None:
        """Validate table name for SQL safety.

        Args:
            table_name: Table name to validate.

        Raises:
            ValueError: If table name is invalid.

        Notes:
            - Must start with letter or underscore
            - Can only contain letters, numbers, and underscores
            - Maximum length is 63 characters (PostgreSQL limit)
            - Prevents SQL injection in table names
        """
        if not table_name:
            msg = "Table name cannot be empty"
            raise ValueError(msg)

        if len(table_name) > MAX_TABLE_NAME_LENGTH:
            msg = f"Table name too long: {len(table_name)} chars (max {MAX_TABLE_NAME_LENGTH})"
            raise ValueError(msg)

        if not VALID_TABLE_NAME_PATTERN.match(table_name):
            msg = (
                f"Invalid table name: {table_name!r}. "
                "Must start with letter/underscore and contain only alphanumeric characters and underscores"
            )
            raise ValueError(msg)


class BaseSyncADKStore(ABC, Generic[ConfigT]):
    """Base class for sync SQLSpec-backed ADK session stores.

    Implements storage operations for Google ADK sessions and events using
    SQLSpec database adapters with synchronous execution.

    This abstract base class provides common functionality for sync database-specific
    store implementations including:
    - Connection management via SQLSpec configs
    - Table name validation
    - Session and event CRUD operations

    Subclasses must implement dialect-specific SQL queries and will be created
    in each adapter directory (e.g., sqlspec/adapters/sqlite/adk/store.py).

    Args:
        config: SQLSpec database configuration (sync).
        session_table: Name of the sessions table. Defaults to "adk_sessions".
        events_table: Name of the events table. Defaults to "adk_events".
    """

    __slots__ = ("_config", "_events_table", "_session_table")

    def __init__(
        self,
        config: ConfigT,
        session_table: str = "adk_sessions",
        events_table: str = "adk_events",
    ) -> None:
        """Initialize the sync ADK store.

        Args:
            config: SQLSpec database configuration.
            session_table: Name of the sessions table.
            events_table: Name of the events table.
        """
        BaseADKStore._validate_table_name(session_table)
        BaseADKStore._validate_table_name(events_table)
        self._config = config
        self._session_table = session_table
        self._events_table = events_table

    @property
    def config(self) -> ConfigT:
        """Return the database configuration."""
        return self._config

    @property
    def session_table(self) -> str:
        """Return the sessions table name."""
        return self._session_table

    @property
    def events_table(self) -> str:
        """Return the events table name."""
        return self._events_table

    @abstractmethod
    def create_session(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: "dict[str, Any]",
    ) -> "SessionRecord":
        """Create a new session.

        Args:
            session_id: Unique identifier for the session.
            app_name: Name of the application.
            user_id: ID of the user.
            state: Session state dictionary.

        Returns:
            The created session record.
        """
        raise NotImplementedError

    @abstractmethod
    def get_session(self, session_id: str) -> "SessionRecord | None":
        """Get a session by ID.

        Args:
            session_id: Session identifier.

        Returns:
            Session record if found, None otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def update_session_state(
        self,
        session_id: str,
        state: "dict[str, Any]",
    ) -> None:
        """Update session state.

        Args:
            session_id: Session identifier.
            state: New state dictionary.
        """
        raise NotImplementedError

    @abstractmethod
    def list_sessions(
        self,
        app_name: str,
        user_id: str,
    ) -> "list[SessionRecord]":
        """List all sessions for an app and user.

        Args:
            app_name: Name of the application.
            user_id: ID of the user.

        Returns:
            List of session records.
        """
        raise NotImplementedError

    @abstractmethod
    def delete_session(self, session_id: str) -> None:
        """Delete a session and its events.

        Args:
            session_id: Session identifier.
        """
        raise NotImplementedError

    @abstractmethod
    def create_event(
        self,
        event_id: str,
        session_id: str,
        app_name: str,
        user_id: str,
        author: "str | None" = None,
        actions: "bytes | None" = None,
        content: "dict[str, Any] | None" = None,
        **kwargs: Any,
    ) -> "EventRecord":
        """Create a new event.

        Args:
            event_id: Unique event identifier.
            session_id: Session identifier.
            app_name: Application name.
            user_id: User identifier.
            author: Event author (user/assistant/system).
            actions: Pickled actions object.
            content: Event content (JSONB/JSON).
            **kwargs: Additional optional fields.

        Returns:
            Created event record.
        """
        raise NotImplementedError

    @abstractmethod
    def list_events(
        self,
        session_id: str,
    ) -> "list[EventRecord]":
        """List events for a session ordered by timestamp.

        Args:
            session_id: Session identifier.

        Returns:
            List of event records ordered by timestamp ASC.
        """
        raise NotImplementedError

    @abstractmethod
    def create_tables(self) -> None:
        """Create both sessions and events tables if they don't exist."""
        raise NotImplementedError

    @abstractmethod
    def _get_create_sessions_table_sql(self) -> str:
        """Get SQL to create sessions table.

        Returns:
            SQL statement to create adk_sessions table with indexes.
        """
        raise NotImplementedError

    @abstractmethod
    def _get_create_events_table_sql(self) -> str:
        """Get SQL to create events table.

        Returns:
            SQL statement to create adk_events table with indexes.
        """
        raise NotImplementedError

    @abstractmethod
    def _get_drop_tables_sql(self) -> "list[str]":
        """Get SQL to drop tables.

        Returns:
            List of SQL statements to drop tables and indexes.
            Order matters: drop events before sessions due to FK.
        """
        raise NotImplementedError
