"""Base store class for ADK session backends."""

import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Final, Generic, TypeVar

from sqlspec.extensions.adk._config_utils import _get_adk_session_store_config
from sqlspec.observability import resolve_db_system
from sqlspec.utils.identifiers import validate_identifier
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from sqlspec.config import DatabaseConfigProtocol
    from sqlspec.extensions.adk._types import EventRecord, SessionRecord

ConfigT = TypeVar("ConfigT", bound="DatabaseConfigProtocol[Any, Any, Any]")

logger = get_logger("sqlspec.extensions.adk.store")

__all__ = ("BaseAsyncADKStore",)

COLUMN_NAME_PATTERN: Final = re.compile(r"^(\w+)")


def _parse_owner_id_column(owner_id_column_ddl: str) -> str:
    """Extract column name from owner ID column DDL definition.

    Args:
        owner_id_column_ddl: Full column DDL string (e.g., "user_id INTEGER REFERENCES users(id)").

    Returns:
        Column name only (first word).

    Raises:
        ValueError: If DDL format is invalid.

    Examples:
        "account_id INTEGER NOT NULL" -> "account_id"
        "user_id UUID REFERENCES users(id)" -> "user_id"
        "tenant VARCHAR(64) DEFAULT 'public'" -> "tenant"

    Notes:
        Only the column name is parsed. The rest of the DDL is passed through
        verbatim to CREATE TABLE statements.
    """
    match = COLUMN_NAME_PATTERN.match(owner_id_column_ddl.strip())
    if not match:
        msg = f"Invalid owner_id_column DDL: {owner_id_column_ddl!r}. Must start with column name."
        raise ValueError(msg)

    return match.group(1)


class BaseAsyncADKStore(ABC, Generic[ConfigT]):
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
        config: SQLSpec database configuration with extension_config["adk"] settings.

    Notes:
        Configuration is read from config.extension_config["adk"]:
        - session_table: Sessions table name (default: "adk_sessions")
        - events_table: Events table name (default: "adk_events")
        - owner_id_column: Optional owner FK column DDL (default: None)
    """

    __slots__ = ("_config", "_events_table", "_owner_id_column_ddl", "_owner_id_column_name", "_session_table")

    def __init__(self, config: ConfigT) -> None:
        """Initialize the ADK store.

        Args:
            config: SQLSpec database configuration.

        Notes:
            Reads configuration from config.extension_config["adk"]:
            - session_table: Sessions table name (default: "adk_sessions")
            - events_table: Events table name (default: "adk_events")
            - owner_id_column: Optional owner FK column DDL (default: None)
        """
        self._config = config
        store_config = self._get_store_config_from_extension()
        self._session_table: str = str(store_config["session_table"])
        self._events_table: str = str(store_config["events_table"])
        self._owner_id_column_ddl: str | None = store_config.get("owner_id_column")
        self._owner_id_column_name: str | None = (
            _parse_owner_id_column(self._owner_id_column_ddl) if self._owner_id_column_ddl else None
        )
        validate_identifier(self._session_table, label="table name")
        validate_identifier(self._events_table, label="table name")

    def _get_store_config_from_extension(self) -> "dict[str, Any]":
        """Extract ADK store configuration from config.extension_config.

        Returns:
            Dict with session_table, events_table, and optionally owner_id_column.
        """
        return dict(_get_adk_session_store_config(self._config))

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

    @property
    def owner_id_column_ddl(self) -> "str | None":
        """Return the full owner ID column DDL (or None if not configured)."""
        return self._owner_id_column_ddl

    @property
    def owner_id_column_name(self) -> "str | None":
        """Return the owner ID column name only (or None if not configured)."""
        return self._owner_id_column_name

    def _calculate_expires_at(self, expires_in: "int | timedelta | None") -> "datetime | None":
        """Calculate expiration timestamp from expires_in.

        Args:
            expires_in: Seconds or timedelta until expiration.

        Returns:
            UTC datetime of expiration, or None if no expiration.
        """
        if expires_in is None:
            return None

        expires_in_seconds = int(expires_in.total_seconds()) if isinstance(expires_in, timedelta) else expires_in

        if expires_in_seconds <= 0:
            return None

        return datetime.now(timezone.utc) + timedelta(seconds=expires_in_seconds)

    def _value_to_bytes(self, value: "str | bytes") -> bytes:
        """Convert value to bytes if needed.

        Args:
            value: String or bytes value.

        Returns:
            Value as bytes.
        """
        if isinstance(value, str):
            return value.encode("utf-8")
        return value

    @abstractmethod
    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> "SessionRecord":
        """Create a new session.

        Args:
            session_id: Unique identifier for the session.
            app_name: Name of the application.
            user_id: ID of the user.
            state: Session state dictionary.
            owner_id: Optional owner ID value for owner_id_column (if configured).

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
    async def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state.

        Args:
            session_id: Session identifier.
            state: New state dictionary.
        """
        raise NotImplementedError

    @abstractmethod
    async def list_sessions(self, app_name: str, user_id: "str | None" = None) -> "list[SessionRecord]":
        """List all sessions for an app, optionally filtered by user.

        Args:
            app_name: Name of the application.
            user_id: ID of the user. If None, returns all sessions for the app.

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
    async def append_event_and_update_state(
        self, event_record: "EventRecord", session_id: str, state: "dict[str, Any]"
    ) -> "SessionRecord":
        """Atomically append an event and update the session's durable state.

        This is the authoritative durable write boundary for post-creation
        session mutations.  The event insert and state update must succeed
        together or fail together, and the updated session record is returned
        in the same round-trip so callers don't need a follow-up read.

        Args:
            event_record: Event record to store.
            session_id: Session identifier whose state should be updated.
            state: Post-append durable state snapshot (``temp:`` keys already
                stripped by the service layer).

        Returns:
            The updated SessionRecord reflecting the new state and update_time.

        Raises:
            ValueError: If the session row no longer exists at update time
                (raced with delete_session).
        """
        raise NotImplementedError

    @abstractmethod
    async def get_events(
        self, session_id: str, after_timestamp: "datetime | None" = None, limit: "int | None" = None
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

    async def ensure_tables(self) -> None:
        """Create tables and emit a standardized log entry."""

        await self.create_tables()
        self._log_tables_created()

    @abstractmethod
    async def _get_create_sessions_table_sql(self) -> str:
        """Get the CREATE TABLE SQL for the sessions table.

        Returns:
            SQL statement to create the sessions table.
        """
        raise NotImplementedError

    @abstractmethod
    async def _get_create_events_table_sql(self) -> str:
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

    def _log_tables_created(self) -> None:
        log_with_context(
            logger,
            logging.DEBUG,
            "adk.tables.ready",
            db_system=resolve_db_system(type(self).__name__),
            session_table=self._session_table,
            events_table=self._events_table,
        )
