"""Base store class for ADK session backends."""

import inspect
import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Final, Generic, TypeVar, cast

from sqlspec.extensions.adk._config_utils import _get_adk_session_store_config
from sqlspec.observability import resolve_db_system
from sqlspec.utils.identifiers import validate_identifier
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.sync_tools import async_

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
        - session_table: Sessions table name (default: "adk_session")
        - events_table: Events table name (default: "adk_event")
        - app_state_table: App-scoped state table name (default: "adk_app_state")
        - user_state_table: User-scoped state table name (default: "adk_user_state")
        - metadata_table: Internal metadata table name (default: "adk_metadata")
        - owner_id_column: Optional owner FK column DDL (default: None)
    """

    __slots__ = (
        "_app_state_table",
        "_config",
        "_events_table",
        "_metadata_table",
        "_owner_id_column_ddl",
        "_owner_id_column_name",
        "_session_table",
        "_user_state_table",
    )

    def __init__(self, config: ConfigT) -> None:
        """Initialize the ADK store.

        Args:
            config: SQLSpec database configuration.

        Notes:
            Reads configuration from config.extension_config["adk"]:
            - session_table: Sessions table name (default: "adk_session")
            - events_table: Events table name (default: "adk_event")
            - app_state_table: App-scoped state table name (default: "adk_app_state")
            - user_state_table: User-scoped state table name (default: "adk_user_state")
            - metadata_table: Internal metadata table name (default: "adk_metadata")
            - owner_id_column: Optional owner FK column DDL (default: None)
        """
        self._config = config
        store_config = self._get_store_config_from_extension()
        self._session_table: str = str(store_config["session_table"])
        self._events_table: str = str(store_config["events_table"])
        self._app_state_table: str = str(store_config["app_state_table"])
        self._user_state_table: str = str(store_config["user_state_table"])
        self._metadata_table: str = str(store_config["metadata_table"])
        self._owner_id_column_ddl: str | None = store_config.get("owner_id_column")
        self._owner_id_column_name: str | None = (
            _parse_owner_id_column(self._owner_id_column_ddl) if self._owner_id_column_ddl else None
        )
        validate_identifier(self._session_table, label="table name")
        validate_identifier(self._events_table, label="table name")
        validate_identifier(self._app_state_table, label="table name")
        validate_identifier(self._user_state_table, label="table name")
        validate_identifier(self._metadata_table, label="table name")

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
    def app_state_table(self) -> str:
        """Return the app-scoped state table name."""
        return self._app_state_table

    @property
    def user_state_table(self) -> str:
        """Return the user-scoped state table name."""
        return self._user_state_table

    @property
    def metadata_table(self) -> str:
        """Return the ADK metadata table name."""
        return self._metadata_table

    @property
    def owner_id_column_ddl(self) -> "str | None":
        """Return the full owner ID column DDL (or None if not configured)."""
        return self._owner_id_column_ddl

    @property
    def owner_id_column_name(self) -> "str | None":
        """Return the owner ID column name only (or None if not configured)."""
        return self._owner_id_column_name

    def _get_store_config_from_extension(self) -> "dict[str, Any]":
        """Extract ADK store configuration from config.extension_config.

        Returns:
            Dict with ADK table names and optionally owner_id_column.
        """
        return dict(_get_adk_session_store_config(self._config))

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
    async def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        """Get a session.

        Args:
            app_name: Name of the application.
            user_id: ID of the user.
            session_id: Session identifier.
            renew_for: If positive, touch the session update timestamp while reading.

        Returns:
            Session record if found, None otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state.

        Args:
            app_name: Name of the application.
            user_id: ID of the user.
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
    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session and its events.

        Args:
            app_name: Name of the application.
            user_id: ID of the user.
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
        self,
        event_record: "EventRecord",
        app_name: str,
        user_id: str,
        session_id: str,
        state: "dict[str, Any]",
        *,
        app_state: "dict[str, Any] | None" = None,
        user_state: "dict[str, Any] | None" = None,
    ) -> "SessionRecord":
        """Atomically append an event and update the session's durable state.

        This is the authoritative durable write boundary for post-creation
        session mutations.  The event insert, session state update, and the
        optional scoped-state upserts must succeed together or fail together,
        and the updated session record is returned in the same round-trip so
        callers don't need a follow-up read.

        When ``app_state`` is provided (non-None), it is a full merged
        app-scoped snapshot to replace/upsert for ``app_name``. When
        ``user_state`` is provided, it is a full merged user-scoped snapshot to
        replace/upsert for ``(app_name, user_id)``. ``None`` means that scope
        was untouched by the event and must not be written.

        Args:
            event_record: Event record to store.
            app_name: Application name for routing scoped-state upserts.
            user_id: User identifier for routing user-scoped upserts.
            session_id: Session identifier whose state should be updated.
            state: Post-append durable session-scoped state snapshot
                (``temp:`` keys already stripped by the service layer).
            app_state: Full app-scoped state snapshot (``app:*`` keys) to
                upsert atomically, or ``None`` when untouched.
            user_state: Full user-scoped state snapshot (``user:*`` keys) to
                upsert atomically, or ``None`` when untouched.

        Returns:
            The updated SessionRecord reflecting the new state and update_time.

        Raises:
            ValueError: If the session row no longer exists at update time
                (raced with delete_session).
        """
        raise NotImplementedError

    @abstractmethod
    async def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "list[EventRecord]":
        """Get events for a session.

        Args:
            app_name: Name of the application.
            user_id: ID of the user.
            session_id: Session identifier.
            after_timestamp: Only return events after this time.
            limit: Maximum number of events to return.

        Returns:
            List of event records ordered by timestamp ascending.
        """
        raise NotImplementedError

    @abstractmethod
    async def delete_expired_events(self, before: datetime) -> int:
        """Delete events older than the given timestamp.

        Args:
            before: Timestamp threshold; events with timestamp earlier than this value are deleted.

        Returns:
            Number of event rows deleted.
        """
        raise NotImplementedError

    @abstractmethod
    async def delete_idle_sessions(self, updated_before: datetime) -> int:
        """Delete sessions whose update_time predates the given threshold.

        Args:
            updated_before: Timestamp threshold; sessions updated earlier than this value are deleted.

        Returns:
            Number of session rows deleted.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        """Return app-scoped state for an application.

        Args:
            app_name: Application name.

        Returns:
            App-scoped state mapping if present, otherwise ``None``.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        """Return user-scoped state for an application user.

        Args:
            app_name: Application name.
            user_id: User identifier.

        Returns:
            User-scoped state mapping if present, otherwise ``None``.
        """
        raise NotImplementedError

    @abstractmethod
    async def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        """Insert or replace app-scoped state for an application.

        Args:
            app_name: Application name.
            state: App-scoped state mapping.
        """
        raise NotImplementedError

    @abstractmethod
    async def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        """Insert or replace user-scoped state for an application user.

        Args:
            app_name: Application name.
            user_id: User identifier.
            state: User-scoped state mapping.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_metadata(self, key: str) -> "str | None":
        """Return a value from the ADK internal metadata table.

        Args:
            key: Metadata key.

        Returns:
            Metadata value if present, otherwise ``None``.
        """
        raise NotImplementedError

    @abstractmethod
    async def set_metadata(self, key: str, value: str) -> None:
        """Set a value in the ADK internal metadata table.

        Args:
            key: Metadata key.
            value: Metadata value.
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

    async def drop_tables(self) -> None:
        """Drop all ADK tables managed by this store in FK-safe order."""
        await self._execute_lifecycle_scripts(self._get_drop_tables_sql())
        self._log_tables_dropped()

    async def recreate_tables(self) -> None:
        """Drop and recreate all ADK tables managed by this store."""
        await self.drop_tables()
        await self.ensure_tables()
        self._log_tables_recreated()

    async def _execute_lifecycle_scripts(self, statements: list[str]) -> None:
        """Execute lifecycle DDL scripts for async and sync-backed configs."""
        session_context = self._config.provide_session()
        if hasattr(session_context, "__aenter__"):
            async with cast("Any", session_context) as driver:
                for statement in statements:
                    result = driver.execute_script(statement)
                    if inspect.isawaitable(result):
                        await result
                commit = getattr(driver, "commit", None)
                if callable(commit):
                    result = commit()
                    if inspect.isawaitable(result):
                        await result
            return

        def _execute_sync() -> None:
            with cast("Any", self._config.provide_session()) as driver:
                for statement in statements:
                    driver.execute_script(statement)
                commit = getattr(driver, "commit", None)
                if callable(commit):
                    commit()

        await async_(_execute_sync)()

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
    async def _get_create_app_states_table_sql(self) -> str:
        """Get the CREATE TABLE SQL for the app-scoped state table.

        Returns:
            SQL statement to create the app-scoped state table.
        """
        raise NotImplementedError

    @abstractmethod
    async def _get_create_user_states_table_sql(self) -> str:
        """Get the CREATE TABLE SQL for the user-scoped state table.

        Returns:
            SQL statement to create the user-scoped state table.
        """
        raise NotImplementedError

    @abstractmethod
    async def _get_create_metadata_table_sql(self) -> str:
        """Get the CREATE TABLE SQL for the ADK internal metadata table.

        Returns:
            SQL statement to create the ADK internal metadata table.
        """
        raise NotImplementedError

    @abstractmethod
    async def _get_seed_metadata_sql(self) -> str:
        """Get the SQL statement that seeds the ADK schema-version metadata row.

        Returns:
            SQL statement that records ``schema_version = 1``.
        """
        raise NotImplementedError

    @abstractmethod
    def _get_drop_app_states_table_sql(self) -> str:
        """Get the DROP TABLE SQL statement for the app-scoped state table.

        Returns:
            SQL statement to drop the app-scoped state table.
        """
        raise NotImplementedError

    @abstractmethod
    def _get_drop_user_states_table_sql(self) -> str:
        """Get the DROP TABLE SQL statement for the user-scoped state table.

        Returns:
            SQL statement to drop the user-scoped state table.
        """
        raise NotImplementedError

    @abstractmethod
    def _get_drop_metadata_table_sql(self) -> str:
        """Get the DROP TABLE SQL statement for the ADK internal metadata table.

        Returns:
            SQL statement to drop the ADK internal metadata table.
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

    def _log_tables_dropped(self) -> None:
        log_with_context(
            logger,
            logging.DEBUG,
            "adk.tables.dropped",
            db_system=resolve_db_system(type(self).__name__),
            session_table=self._session_table,
            events_table=self._events_table,
        )

    def _log_tables_recreated(self) -> None:
        log_with_context(
            logger,
            logging.DEBUG,
            "adk.tables.recreated",
            db_system=resolve_db_system(type(self).__name__),
            session_table=self._session_table,
            events_table=self._events_table,
        )
