"""Base store classes for ADK session backend (sync and async)."""

import logging
import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Final, Generic, TypeVar, cast

from sqlspec.observability import resolve_db_system
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from datetime import datetime

    from sqlspec.config import ADKConfig, DatabaseConfigProtocol
    from sqlspec.extensions.adk._types import EventRecord, SessionRecord

ConfigT = TypeVar("ConfigT", bound="DatabaseConfigProtocol[Any, Any, Any]")

logger = get_logger("sqlspec.extensions.adk.store")

__all__ = ("BaseAsyncADKStore", "BaseSyncADKStore")

VALID_TABLE_NAME_PATTERN: Final = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
COLUMN_NAME_PATTERN: Final = re.compile(r"^(\w+)")
MAX_TABLE_NAME_LENGTH: Final = 63


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
        _validate_table_name(self._session_table)
        _validate_table_name(self._events_table)

    def _get_store_config_from_extension(self) -> "dict[str, Any]":
        """Extract ADK store configuration from config.extension_config.

        Returns:
            Dict with session_table, events_table, and optionally owner_id_column.
        """
        extension_config = self._config.extension_config
        adk_config = cast("ADKConfig", extension_config.get("adk", {}))
        session_table = adk_config.get("session_table")
        events_table = adk_config.get("events_table")
        result: dict[str, Any] = {
            "session_table": session_table if session_table is not None else "adk_sessions",
            "events_table": events_table if events_table is not None else "adk_events",
        }
        owner_id = adk_config.get("owner_id_column")
        if owner_id is not None:
            result["owner_id_column"] = owner_id
        return result

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
        config: SQLSpec database configuration with extension_config["adk"] settings.

    Notes:
        Configuration is read from config.extension_config["adk"]:
        - session_table: Sessions table name (default: "adk_sessions")
        - events_table: Events table name (default: "adk_events")
        - owner_id_column: Optional owner FK column DDL (default: None)
    """

    __slots__ = ("_config", "_events_table", "_owner_id_column_ddl", "_owner_id_column_name", "_session_table")

    def __init__(self, config: ConfigT) -> None:
        """Initialize the sync ADK store.

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
        _validate_table_name(self._session_table)
        _validate_table_name(self._events_table)

    def _get_store_config_from_extension(self) -> "dict[str, Any]":
        """Extract ADK store configuration from config.extension_config.

        Returns:
            Dict with session_table, events_table, and optionally owner_id_column.
        """
        extension_config = self._config.extension_config
        adk_config = cast("ADKConfig", extension_config.get("adk", {}))
        session_table = adk_config.get("session_table")
        events_table = adk_config.get("events_table")
        result: dict[str, Any] = {
            "session_table": session_table if session_table is not None else "adk_sessions",
            "events_table": events_table if events_table is not None else "adk_events",
        }
        owner_id = adk_config.get("owner_id_column")
        if owner_id is not None:
            result["owner_id_column"] = owner_id
        return result

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

    @abstractmethod
    def create_session(
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
    def get_session(self, session_id: str) -> "SessionRecord | None":
        """Get a session by ID.

        Args:
            session_id: Session identifier.

        Returns:
            Session record if found, None otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state.

        Args:
            session_id: Session identifier.
            state: New state dictionary.
        """
        raise NotImplementedError

    @abstractmethod
    def list_sessions(self, app_name: str, user_id: "str | None" = None) -> "list[SessionRecord]":
        """List all sessions for an app, optionally filtered by user.

        Args:
            app_name: Name of the application.
            user_id: ID of the user. If None, returns all sessions for the app.

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
    def list_events(self, session_id: str) -> "list[EventRecord]":
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

    def ensure_tables(self) -> None:
        """Create tables and emit a standardized log entry."""

        self.create_tables()
        self._log_tables_created()

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

    def _log_tables_created(self) -> None:
        log_with_context(
            logger,
            logging.DEBUG,
            "adk.tables.ready",
            db_system=resolve_db_system(type(self).__name__),
            session_table=self._session_table,
            events_table=self._events_table,
        )

    @abstractmethod
    def _get_drop_tables_sql(self) -> "list[str]":
        """Get SQL to drop tables.

        Returns:
            List of SQL statements to drop tables and indexes.
            Order matters: drop events before sessions due to FK.
        """
        raise NotImplementedError
