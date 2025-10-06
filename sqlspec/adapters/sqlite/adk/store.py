"""SQLite sync ADK store for Google Agent Development Kit session/event storage."""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlspec.extensions.adk._types import EventRecord, SessionRecord
from sqlspec.extensions.adk.store import BaseAsyncADKStore
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.adapters.sqlite.config import SqliteConfig

logger = get_logger("adapters.sqlite.adk.store")

SECONDS_PER_DAY = 86400.0
JULIAN_EPOCH = 2440587.5

__all__ = ("SqliteADKStore",)


def _datetime_to_julian(dt: datetime) -> float:
    """Convert datetime to Julian Day number for SQLite storage.

    Args:
        dt: Datetime to convert (must be UTC-aware).

    Returns:
        Julian Day number as REAL.

    Notes:
        Julian Day number is days since November 24, 4714 BCE (proleptic Gregorian).
        This enables direct comparison with julianday('now') in SQL queries.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta_days = (dt - epoch).total_seconds() / SECONDS_PER_DAY
    return JULIAN_EPOCH + delta_days


def _julian_to_datetime(julian: float) -> datetime:
    """Convert Julian Day number back to datetime.

    Args:
        julian: Julian Day number.

    Returns:
        UTC-aware datetime.
    """
    days_since_epoch = julian - JULIAN_EPOCH
    timestamp = days_since_epoch * SECONDS_PER_DAY
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def _to_sqlite_bool(value: "bool | None") -> "int | None":
    """Convert Python bool to SQLite INTEGER.

    Args:
        value: Boolean value or None.

    Returns:
        1 for True, 0 for False, None for None.
    """
    if value is None:
        return None
    return 1 if value else 0


def _from_sqlite_bool(value: "int | None") -> "bool | None":
    """Convert SQLite INTEGER to Python bool.

    Args:
        value: Integer value (0/1) or None.

    Returns:
        True for 1, False for 0, None for None.
    """
    if value is None:
        return None
    return bool(value)




class SqliteADKStore(BaseAsyncADKStore["SqliteConfig"]):
    """SQLite ADK store using synchronous SQLite driver.

    Implements session and event storage for Google Agent Development Kit
    using SQLite via the synchronous sqlite3 driver. Uses Litestar's sync_to_thread
    utility to provide an async interface compatible with the Store protocol.

    Provides:
    - Session state management with JSON storage (as TEXT)
    - Event history tracking with BLOB-serialized actions
    - Julian Day timestamps (REAL) for efficient date operations
    - Foreign key constraints with cascade delete
    - Efficient upserts using INSERT OR REPLACE

    Args:
        config: SqliteConfig instance.
        session_table: Name of the sessions table. Defaults to "adk_sessions".
        events_table: Name of the events table. Defaults to "adk_events".

    Example:
        from sqlspec.adapters.sqlite import SqliteConfig
        from sqlspec.adapters.sqlite.adk import SqliteADKStore

        config = SqliteConfig(database=":memory:")
        store = SqliteADKStore(config)
        await store.create_tables()

    Notes:
        - JSON stored as TEXT with SQLSpec serializers (msgspec/orjson/stdlib)
        - BOOLEAN as INTEGER (0/1, with None for NULL)
        - Timestamps as REAL (Julian day: julianday('now'))
        - BLOB for pre-serialized actions from Google ADK
        - PRAGMA foreign_keys = ON (enable per connection)
    """

    __slots__ = ()

    def __init__(
        self, config: "SqliteConfig", session_table: str = "adk_sessions", events_table: str = "adk_events"
    ) -> None:
        """Initialize SQLite ADK store.

        Args:
            config: SqliteConfig instance.
            session_table: Name of the sessions table.
            events_table: Name of the events table.
        """
        super().__init__(config, session_table, events_table)

    def _get_create_sessions_table_sql(self) -> str:
        """Get SQLite CREATE TABLE SQL for sessions.

        Returns:
            SQL statement to create adk_sessions table with indexes.

        Notes:
            - TEXT for IDs, names, and JSON state
            - REAL for Julian Day timestamps
            - Composite index on (app_name, user_id)
            - Index on update_time DESC for recent session queries
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id TEXT PRIMARY KEY,
            app_name TEXT NOT NULL,
            user_id TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT '{{}}',
            create_time REAL NOT NULL,
            update_time REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user
            ON {self._session_table}(app_name, user_id);
        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time
            ON {self._session_table}(update_time DESC);
        """

    def _get_create_events_table_sql(self) -> str:
        """Get SQLite CREATE TABLE SQL for events.

        Returns:
            SQL statement to create adk_events table with indexes.

        Notes:
            - TEXT for IDs, strings, and JSON content
            - BLOB for pickled actions
            - INTEGER for booleans (0/1/NULL)
            - REAL for Julian Day timestamps
            - Foreign key to sessions with CASCADE delete
            - Index on (session_id, timestamp ASC)
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            app_name TEXT NOT NULL,
            user_id TEXT NOT NULL,
            invocation_id TEXT NOT NULL,
            author TEXT NOT NULL,
            actions BLOB NOT NULL,
            long_running_tool_ids_json TEXT,
            branch TEXT,
            timestamp REAL NOT NULL,
            content TEXT,
            grounding_metadata TEXT,
            custom_metadata TEXT,
            partial INTEGER,
            turn_complete INTEGER,
            interrupted INTEGER,
            error_code TEXT,
            error_message TEXT,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session
            ON {self._events_table}(session_id, timestamp ASC);
        """

    def _get_drop_tables_sql(self) -> "list[str]":
        """Get SQLite DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop tables and indexes.

        Notes:
            Order matters: drop events table (child) before sessions (parent).
            SQLite automatically drops indexes when dropping tables.
        """
        return [f"DROP TABLE IF EXISTS {self._events_table}", f"DROP TABLE IF EXISTS {self._session_table}"]

    def _enable_foreign_keys(self, connection: Any) -> None:
        """Enable foreign key constraints for this connection.

        Args:
            connection: SQLite connection.

        Notes:
            SQLite requires PRAGMA foreign_keys = ON per connection.
        """
        connection.execute("PRAGMA foreign_keys = ON")

    def _create_tables(self) -> None:
        """Synchronous implementation of create_tables."""
        with self._config.provide_connection() as conn:
            self._enable_foreign_keys(conn)
            conn.executescript(self._get_create_sessions_table_sql())
            conn.executescript(self._get_create_events_table_sql())
        logger.debug("Created ADK tables: %s, %s", self._session_table, self._events_table)

    async def create_tables(self) -> None:
        """Create both sessions and events tables if they don't exist."""
        await async_(self._create_tables)()

    def _create_session(self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]") -> SessionRecord:
        """Synchronous implementation of create_session."""
        now = datetime.now(timezone.utc)
        now_julian = _datetime_to_julian(now)
        state_json = to_json(state) if state else None

        sql = f"""
        INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
        VALUES (?, ?, ?, ?, ?, ?)
        """

        with self._config.provide_connection() as conn:
            self._enable_foreign_keys(conn)
            conn.execute(sql, (session_id, app_name, user_id, state_json, now_julian, now_julian))
            conn.commit()

        return SessionRecord(
            id=session_id, app_name=app_name, user_id=user_id, state=state, create_time=now, update_time=now
        )

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]"
    ) -> SessionRecord:
        """Create a new session.

        Args:
            session_id: Unique session identifier.
            app_name: Application name.
            user_id: User identifier.
            state: Initial session state.

        Returns:
            Created session record.

        Notes:
            Uses Julian Day for create_time and update_time.
            State is JSON-serialized before insertion.
        """
        return await async_(self._create_session)(session_id, app_name, user_id, state)

    def _get_session(self, session_id: str) -> "SessionRecord | None":
        """Synchronous implementation of get_session."""
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = ?
        """

        with self._config.provide_connection() as conn:
            self._enable_foreign_keys(conn)
            cursor = conn.execute(sql, (session_id,))
            row = cursor.fetchone()

            if row is None:
                return None

            return SessionRecord(
                id=row[0],
                app_name=row[1],
                user_id=row[2],
                state=from_json(row[3]) if row[3] else {},
                create_time=_julian_to_datetime(row[4]),
                update_time=_julian_to_datetime(row[5]),
            )

    async def get_session(self, session_id: str) -> "SessionRecord | None":
        """Get session by ID.

        Args:
            session_id: Session identifier.

        Returns:
            Session record or None if not found.

        Notes:
            SQLite returns Julian Day (REAL) for timestamps.
            JSON is parsed from TEXT storage.
        """
        return await async_(self._get_session)(session_id)

    def _update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Synchronous implementation of update_session_state."""
        now_julian = _datetime_to_julian(datetime.now(timezone.utc))
        state_json = to_json(state) if state else None

        sql = f"""
        UPDATE {self._session_table}
        SET state = ?, update_time = ?
        WHERE id = ?
        """

        with self._config.provide_connection() as conn:
            self._enable_foreign_keys(conn)
            conn.execute(sql, (state_json, now_julian, session_id))
            conn.commit()

    async def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state.

        Args:
            session_id: Session identifier.
            state: New state dictionary (replaces existing state).

        Notes:
            This replaces the entire state dictionary.
            Updates update_time to current Julian Day.
        """
        await async_(self._update_session_state)(session_id, state)

    def _list_sessions(self, app_name: str, user_id: str) -> "list[SessionRecord]":
        """Synchronous implementation of list_sessions."""
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE app_name = ? AND user_id = ?
        ORDER BY update_time DESC
        """

        with self._config.provide_connection() as conn:
            self._enable_foreign_keys(conn)
            cursor = conn.execute(sql, (app_name, user_id))
            rows = cursor.fetchall()

            return [
                SessionRecord(
                    id=row[0],
                    app_name=row[1],
                    user_id=row[2],
                    state=from_json(row[3]) if row[3] else {},
                    create_time=_julian_to_datetime(row[4]),
                    update_time=_julian_to_datetime(row[5]),
                )
                for row in rows
            ]

    async def list_sessions(self, app_name: str, user_id: str) -> "list[SessionRecord]":
        """List all sessions for a user in an app.

        Args:
            app_name: Application name.
            user_id: User identifier.

        Returns:
            List of session records ordered by update_time DESC.

        Notes:
            Uses composite index on (app_name, user_id).
        """
        return await async_(self._list_sessions)(app_name, user_id)

    def _delete_session(self, session_id: str) -> None:
        """Synchronous implementation of delete_session."""
        sql = f"DELETE FROM {self._session_table} WHERE id = ?"

        with self._config.provide_connection() as conn:
            self._enable_foreign_keys(conn)
            conn.execute(sql, (session_id,))
            conn.commit()

    async def delete_session(self, session_id: str) -> None:
        """Delete session and all associated events (cascade).

        Args:
            session_id: Session identifier.

        Notes:
            Foreign key constraint ensures events are cascade-deleted.
        """
        await async_(self._delete_session)(session_id)

    def _append_event(self, event_record: EventRecord) -> None:
        """Synchronous implementation of append_event."""
        timestamp_julian = _datetime_to_julian(event_record["timestamp"])

        content_json = to_json(event_record.get("content")) if event_record.get("content") else None
        grounding_metadata_json = to_json(event_record.get("grounding_metadata")) if event_record.get("grounding_metadata") else None
        custom_metadata_json = to_json(event_record.get("custom_metadata")) if event_record.get("custom_metadata") else None

        partial_int = _to_sqlite_bool(event_record.get("partial"))
        turn_complete_int = _to_sqlite_bool(event_record.get("turn_complete"))
        interrupted_int = _to_sqlite_bool(event_record.get("interrupted"))

        sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, app_name, user_id, invocation_id, author, actions,
            long_running_tool_ids_json, branch, timestamp, content,
            grounding_metadata, custom_metadata, partial, turn_complete,
            interrupted, error_code, error_message
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """

        with self._config.provide_connection() as conn:
            self._enable_foreign_keys(conn)
            conn.execute(
                sql,
                (
                    event_record["id"],
                    event_record["session_id"],
                    event_record["app_name"],
                    event_record["user_id"],
                    event_record["invocation_id"],
                    event_record["author"],
                    event_record["actions"],
                    event_record.get("long_running_tool_ids_json"),
                    event_record.get("branch"),
                    timestamp_julian,
                    content_json,
                    grounding_metadata_json,
                    custom_metadata_json,
                    partial_int,
                    turn_complete_int,
                    interrupted_int,
                    event_record.get("error_code"),
                    event_record.get("error_message"),
                ),
            )
            conn.commit()

    async def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session.

        Args:
            event_record: Event record to store.

        Notes:
            Uses Julian Day for timestamp.
            JSON fields are serialized to TEXT.
            Boolean fields converted to INTEGER (0/1/NULL).
        """
        await async_(self._append_event)(event_record)

    def _get_events(
        self, session_id: str, after_timestamp: "datetime | None" = None, limit: "int | None" = None
    ) -> "list[EventRecord]":
        """Synchronous implementation of get_events."""
        where_clauses = ["session_id = ?"]
        params: list[Any] = [session_id]

        if after_timestamp is not None:
            where_clauses.append("timestamp > ?")
            params.append(_datetime_to_julian(after_timestamp))

        where_clause = " AND ".join(where_clauses)
        limit_clause = f" LIMIT {limit}" if limit else ""

        sql = f"""
        SELECT id, session_id, app_name, user_id, invocation_id, author, actions,
               long_running_tool_ids_json, branch, timestamp, content,
               grounding_metadata, custom_metadata, partial, turn_complete,
               interrupted, error_code, error_message
        FROM {self._events_table}
        WHERE {where_clause}
        ORDER BY timestamp ASC{limit_clause}
        """

        with self._config.provide_connection() as conn:
            self._enable_foreign_keys(conn)
            cursor = conn.execute(sql, params)
            rows = cursor.fetchall()

            return [
                EventRecord(
                    id=row[0],
                    session_id=row[1],
                    app_name=row[2],
                    user_id=row[3],
                    invocation_id=row[4],
                    author=row[5],
                    actions=bytes(row[6]),
                    long_running_tool_ids_json=row[7],
                    branch=row[8],
                    timestamp=_julian_to_datetime(row[9]),
                    content=from_json(row[10]) if row[10] else None,
                    grounding_metadata=from_json(row[11]) if row[11] else None,
                    custom_metadata=from_json(row[12]) if row[12] else None,
                    partial=_from_sqlite_bool(row[13]),
                    turn_complete=_from_sqlite_bool(row[14]),
                    interrupted=_from_sqlite_bool(row[15]),
                    error_code=row[16],
                    error_message=row[17],
                )
                for row in rows
            ]

    async def get_events(
        self, session_id: str, after_timestamp: "datetime | None" = None, limit: "int | None" = None
    ) -> "list[EventRecord]":
        """Get events for a session.

        Args:
            session_id: Session identifier.
            after_timestamp: Only return events after this time.
            limit: Maximum number of events to return.

        Returns:
            List of event records ordered by timestamp ASC.

        Notes:
            Uses index on (session_id, timestamp ASC).
            Parses JSON fields and converts BLOB actions to bytes.
            Converts INTEGER booleans back to bool/None.
        """
        return await async_(self._get_events)(session_id, after_timestamp, limit)
