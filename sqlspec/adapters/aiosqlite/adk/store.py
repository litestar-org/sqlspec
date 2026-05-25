"""Aiosqlite async ADK store for Google Agent Development Kit session/event storage."""

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Final, cast

from sqlspec.extensions.adk import BaseAsyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
    from sqlspec.extensions.adk import MemoryRecord


SECONDS_PER_DAY = 86400.0
JULIAN_EPOCH = 2440587.5
SQLITE_TABLE_NOT_FOUND_ERROR: Final = "no such table"

__all__ = ("AiosqliteADKMemoryStore", "AiosqliteADKStore")


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


class AiosqliteADKStore(BaseAsyncADKStore["AiosqliteConfig"]):
    """Aiosqlite ADK store using asynchronous SQLite driver.

    Implements session and event storage for Google Agent Development Kit
    using SQLite via the asynchronous aiosqlite driver.

    Provides:
    - Session state management with JSON storage (as TEXT)
    - Event history tracking with full-event JSON storage
    - Julian Day timestamps (REAL) for efficient date operations
    - Foreign key constraints with cascade delete
    - Atomic event+state writes via append_event_and_update_state
    - PRAGMA optimization profile for file-based databases

    Args:
        config: AiosqliteConfig with extension_config["adk"] settings.

    Example:
        from sqlspec.adapters.aiosqlite import AiosqliteConfig
        from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore

        config = AiosqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={
                "adk": {
                    "session_table": "my_sessions",
                    "events_table": "my_events"
                }
            }
        )
        store = AiosqliteADKStore(config)
        await store.ensure_tables()

    Notes:
        - JSON stored as TEXT with SQLSpec serializers (msgspec/orjson/stdlib)
        - Timestamps as REAL (Julian day: julianday('now'))
        - Full event stored as JSON TEXT in event_data column
        - PRAGMA foreign_keys = ON (enable per connection)
        - Configuration is read from config.extension_config["adk"]
    """

    __slots__ = ()

    def __init__(self, config: "AiosqliteConfig") -> None:
        """Initialize Aiosqlite ADK store.

        Args:
            config: AiosqliteConfig instance.

        Notes:
            Configuration is read from config.extension_config["adk"]:
            - session_table: Sessions table name (default: "adk_session")
            - events_table: Events table name (default: "adk_event")
        """
        super().__init__(config)

    async def create_tables(self) -> None:
        """Create both sessions and events tables if they don't exist."""
        async with self._config.provide_session() as driver:
            await self._apply_pragmas(driver.connection)
            await driver.execute_script(await self._get_create_sessions_table_sql())
            await driver.execute_script(await self._get_create_events_table_sql())
            await driver.execute_script(await self._get_create_app_states_table_sql())
            await driver.execute_script(await self._get_create_user_states_table_sql())
            await driver.execute_script(await self._get_create_metadata_table_sql())
            await driver.execute_script(await self._get_seed_metadata_sql())

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        """Create a new session.

        Args:
            session_id: Unique session identifier.
            app_name: Application name.
            user_id: User identifier.
            state: Initial session state.
            owner_id: Optional owner ID value for owner_id_column.

        Returns:
            Created session record.

        Notes:
            Uses Julian Day for create_time and update_time.
            State is always JSON-serialized (empty dict becomes '{}', never NULL).
        """
        now = datetime.now(timezone.utc)
        now_julian = _datetime_to_julian(now)
        state_json = to_json(state)

        params: tuple[Any, ...]
        if self._owner_id_column_name:
            sql = f"""
            INSERT INTO {self._session_table}
            (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            params = (session_id, app_name, user_id, owner_id, state_json, now_julian, now_julian)
        else:
            sql = f"""
            INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
            VALUES (?, ?, ?, ?, ?, ?)
            """
            params = (session_id, app_name, user_id, state_json, now_julian, now_julian)

        async with self._config.provide_connection() as conn:
            await self._apply_pragmas(conn)
            await conn.execute(sql, params)
            await conn.commit()

        return SessionRecord(
            id=session_id, app_name=app_name, user_id=user_id, state=state, create_time=now, update_time=now
        )

    async def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        """Get session.

        Args:
            app_name: Name of the application.
            user_id: ID of the user.
            session_id: Session identifier.
            renew_for: If positive, touch update_time while reading.

        Returns:
            Session record or None if not found.

        Notes:
            SQLite returns Julian Day (REAL) for timestamps.
            JSON is parsed from TEXT storage.
        """
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE app_name = ? AND user_id = ? AND id = ?
        """

        try:
            async with self._config.provide_connection() as conn:
                await self._apply_pragmas(conn)
                if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
                    update_sql = f"UPDATE {self._session_table} SET update_time = ? WHERE app_name = ? AND user_id = ? AND id = ?"
                    await conn.execute(
                        update_sql, (_datetime_to_julian(datetime.now(timezone.utc)), app_name, user_id, session_id)
                    )
                    await conn.commit()

                cursor = await conn.execute(sql, (app_name, user_id, session_id))
                row = await cursor.fetchone()

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
        except sqlite3.OperationalError as exc:
            if SQLITE_TABLE_NOT_FOUND_ERROR in str(exc):
                return None
            raise

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state.

        Args:
            app_name: Name of the application.
            user_id: ID of the user.
            session_id: Session identifier.
            state: New state dictionary (replaces existing state).

        Notes:
            This replaces the entire state dictionary.
            Updates update_time to current Julian Day.
            Empty dict is serialized as '{}', never NULL.
        """
        now_julian = _datetime_to_julian(datetime.now(timezone.utc))
        state_json = to_json(state)

        sql = f"""
        UPDATE {self._session_table}
        SET state = ?, update_time = ?
        WHERE app_name = ? AND user_id = ? AND id = ?
        """

        async with self._config.provide_connection() as conn:
            await self._apply_pragmas(conn)
            await conn.execute(sql, (state_json, now_julian, app_name, user_id, session_id))
            await conn.commit()

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        """List sessions for an app, optionally filtered by user.

        Args:
            app_name: Application name.
            user_id: User identifier. If None, lists all sessions for the app.

        Returns:
            List of session records ordered by update_time DESC.

        Notes:
            Uses composite index on (app_name, user_id) when user_id is provided.
        """
        if user_id is None:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = ?
            ORDER BY update_time DESC
            """
            params: tuple[str, ...] = (app_name,)
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = ? AND user_id = ?
            ORDER BY update_time DESC
            """
            params = (app_name, user_id)

        try:
            async with self._config.provide_connection() as conn:
                await self._apply_pragmas(conn)
                cursor = await conn.execute(sql, params)
                rows = await cursor.fetchall()

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
        except sqlite3.OperationalError as exc:
            if SQLITE_TABLE_NOT_FOUND_ERROR in str(exc):
                return []
            raise

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete session and all associated events (cascade).

        Args:
            app_name: Name of the application.
            user_id: ID of the user.
            session_id: Session identifier.

        Notes:
            Foreign key constraint ensures events are cascade-deleted.
        """
        sql = f"DELETE FROM {self._session_table} WHERE app_name = ? AND user_id = ? AND id = ?"

        async with self._config.provide_connection() as conn:
            await self._apply_pragmas(conn)
            await conn.execute(sql, (app_name, user_id, session_id))
            await conn.commit()

    async def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session.

        Args:
            event_record: Event record.

        Notes:
            Uses Julian Day for timestamp.
            event_data dict is serialized to TEXT as event_data column.
        """
        timestamp_julian = _datetime_to_julian(event_record["timestamp"])
        event_data_json = to_json(event_record["event_data"])

        sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, invocation_id, timestamp, event_data
        ) VALUES (?, ?, ?, ?, ?)
        """

        async with self._config.provide_connection() as conn:
            await self._apply_pragmas(conn)
            await conn.execute(
                sql,
                (
                    event_record["id"],
                    event_record["session_id"],
                    event_record["invocation_id"],
                    timestamp_julian,
                    event_data_json,
                ),
            )
            await conn.commit()

    async def append_event_and_update_state(
        self,
        event_record: EventRecord,
        app_name: str,
        user_id: str,
        session_id: str,
        state: "dict[str, Any]",
        *,
        app_state: "dict[str, Any] | None" = None,
        user_state: "dict[str, Any] | None" = None,
    ) -> SessionRecord:
        """Atomically append an event and update session + scoped state."""
        timestamp_julian = _datetime_to_julian(event_record["timestamp"])
        event_data_json = to_json(event_record["event_data"])
        now_julian = _datetime_to_julian(datetime.now(timezone.utc))
        state_json = to_json(state)

        insert_sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, invocation_id, timestamp, event_data
        ) VALUES (?, ?, ?, ?, ?)
        """

        update_sql = f"""
        UPDATE {self._session_table}
        SET state = ?, update_time = ?
        WHERE app_name = ? AND user_id = ? AND id = ?
        RETURNING id, app_name, user_id, state, create_time, update_time
        """

        app_upsert_sql = f"""
        INSERT INTO {self._app_state_table} (app_name, state, update_time)
        VALUES (?, ?, ?)
        ON CONFLICT(app_name) DO UPDATE SET
            state = excluded.state,
            update_time = excluded.update_time
        """

        user_upsert_sql = f"""
        INSERT INTO {self._user_state_table} (app_name, user_id, state, update_time)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(app_name, user_id) DO UPDATE SET
            state = excluded.state,
            update_time = excluded.update_time
        """

        async with self._config.provide_connection() as conn:
            await self._apply_pragmas(conn)
            await conn.execute(
                insert_sql,
                (
                    event_record["id"],
                    event_record["session_id"],
                    event_record["invocation_id"],
                    timestamp_julian,
                    event_data_json,
                ),
            )
            cursor = await conn.execute(update_sql, (state_json, now_julian, app_name, user_id, session_id))
            row = await cursor.fetchone()
            if app_state:
                await conn.execute(app_upsert_sql, (app_name, to_json(app_state), now_julian))
            if user_state:
                await conn.execute(user_upsert_sql, (app_name, user_id, to_json(user_state), now_julian))
            await conn.commit()

        if row is None:
            msg = f"Session {session_id} not found during append_event_and_update_state."
            raise ValueError(msg)

        return SessionRecord(
            id=row[0],
            app_name=row[1],
            user_id=row[2],
            state=from_json(row[3]) if row[3] else {},
            create_time=_julian_to_datetime(row[4]),
            update_time=_julian_to_datetime(row[5]),
        )

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
            List of event records ordered by timestamp ASC.

        Notes:
            Uses index on (session_id, timestamp ASC).
            Parses event_data TEXT back to dict for event_data field.
        """
        where_clauses = ["s.app_name = ?", "s.user_id = ?", "e.session_id = ?"]
        params: list[Any] = [app_name, user_id, session_id]

        if after_timestamp is not None:
            where_clauses.append("e.timestamp > ?")
            params.append(_datetime_to_julian(after_timestamp))

        where_clause = " AND ".join(where_clauses)
        limit_clause = f" LIMIT {limit}" if limit else ""

        sql = f"""
        SELECT e.id, e.session_id, e.invocation_id, e.timestamp, e.event_data, s.app_name, s.user_id
        FROM {self._events_table} e
        JOIN {self._session_table} s ON e.session_id = s.id
        WHERE {where_clause}
        ORDER BY e.timestamp ASC{limit_clause}
        """

        try:
            async with self._config.provide_connection() as conn:
                await self._apply_pragmas(conn)
                cursor = await conn.execute(sql, params)
                rows = await cursor.fetchall()

                return [
                    EventRecord(
                        id=row[0],
                        session_id=row[1],
                        invocation_id=row[2],
                        timestamp=_julian_to_datetime(row[3]),
                        event_data=from_json(row[4]) if row[4] else {},
                        app_name=row[5],
                        user_id=row[6],
                    )
                    for row in rows
                ]
        except sqlite3.OperationalError as exc:
            if SQLITE_TABLE_NOT_FOUND_ERROR in str(exc):
                return []
            raise

    async def delete_expired_events(self, before: datetime) -> int:
        """Delete events older than the given timestamp."""
        sql = f"DELETE FROM {self._events_table} WHERE timestamp < ?"

        async with self._config.provide_connection() as conn:
            await self._apply_pragmas(conn)
            cursor = await conn.execute(sql, (_datetime_to_julian(before),))
            await conn.commit()
            return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

    async def delete_idle_sessions(self, updated_before: datetime) -> int:
        """Delete sessions whose update_time predates the given threshold."""
        sql = f"DELETE FROM {self._session_table} WHERE update_time < ?"

        async with self._config.provide_connection() as conn:
            await self._apply_pragmas(conn)
            cursor = await conn.execute(sql, (_datetime_to_julian(updated_before),))
            await conn.commit()
            return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

    async def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        """Return app-scoped state for an application."""
        sql = f"SELECT state FROM {self._app_state_table} WHERE app_name = ?"

        try:
            async with self._config.provide_connection() as conn:
                await self._apply_pragmas(conn)
                cursor = await conn.execute(sql, (app_name,))
                row = await cursor.fetchone()
                return from_json(row[0]) if row is not None and row[0] else None
        except sqlite3.OperationalError as exc:
            if SQLITE_TABLE_NOT_FOUND_ERROR in str(exc):
                return None
            raise

    async def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        """Return user-scoped state for an application user."""
        sql = f"SELECT state FROM {self._user_state_table} WHERE app_name = ? AND user_id = ?"

        try:
            async with self._config.provide_connection() as conn:
                await self._apply_pragmas(conn)
                cursor = await conn.execute(sql, (app_name, user_id))
                row = await cursor.fetchone()
                return from_json(row[0]) if row is not None and row[0] else None
        except sqlite3.OperationalError as exc:
            if SQLITE_TABLE_NOT_FOUND_ERROR in str(exc):
                return None
            raise

    async def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        """Insert or replace app-scoped state for an application."""
        sql = f"""
        INSERT INTO {self._app_state_table} (app_name, state, update_time)
        VALUES (?, ?, ?)
        ON CONFLICT(app_name) DO UPDATE SET
            state = excluded.state,
            update_time = excluded.update_time
        """

        async with self._config.provide_connection() as conn:
            await self._apply_pragmas(conn)
            await conn.execute(sql, (app_name, to_json(state), _datetime_to_julian(datetime.now(timezone.utc))))
            await conn.commit()

    async def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        """Insert or replace user-scoped state for an application user."""
        sql = f"""
        INSERT INTO {self._user_state_table} (app_name, user_id, state, update_time)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(app_name, user_id) DO UPDATE SET
            state = excluded.state,
            update_time = excluded.update_time
        """

        async with self._config.provide_connection() as conn:
            await self._apply_pragmas(conn)
            await conn.execute(
                sql, (app_name, user_id, to_json(state), _datetime_to_julian(datetime.now(timezone.utc)))
            )
            await conn.commit()

    async def get_metadata(self, key: str) -> "str | None":
        """Return a value from the ADK internal metadata table."""
        sql = f"SELECT value FROM {self._metadata_table} WHERE key = ?"

        try:
            async with self._config.provide_connection() as conn:
                await self._apply_pragmas(conn)
                cursor = await conn.execute(sql, (key,))
                row = await cursor.fetchone()
                return row[0] if row is not None else None
        except sqlite3.OperationalError as exc:
            if SQLITE_TABLE_NOT_FOUND_ERROR in str(exc):
                return None
            raise

    async def set_metadata(self, key: str, value: str) -> None:
        """Set a value in the ADK internal metadata table."""
        sql = f"""
        INSERT INTO {self._metadata_table} (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """

        async with self._config.provide_connection() as conn:
            await self._apply_pragmas(conn)
            await conn.execute(sql, (key, value))
            await conn.commit()

    async def _apply_pragmas(self, connection: Any) -> None:
        """Apply PRAGMA optimization profile for this connection.

        Args:
            connection: Aiosqlite connection.

        Notes:
            Enables foreign keys and applies performance PRAGMAs.
            For file-based databases, adds cache_size, mmap_size,
            and journal_size_limit optimizations.
        """
        await connection.execute("PRAGMA foreign_keys = ON")
        await connection.execute("PRAGMA cache_size = -64000")
        await connection.execute("PRAGMA mmap_size = 30000000")
        await connection.execute("PRAGMA journal_size_limit = 67108864")

    async def _get_create_sessions_table_sql(self) -> str:
        """Get SQLite CREATE TABLE SQL for sessions.

        Returns:
            SQL statement to create adk_session table with indexes.

        Notes:
            - TEXT for IDs, names, and JSON state
            - REAL for Julian Day timestamps
            - Optional owner ID column for multi-tenant scenarios
            - Composite index on (app_name, user_id)
            - Index on update_time DESC for recent session queries
        """
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id TEXT PRIMARY KEY,
            app_name TEXT NOT NULL,
            user_id TEXT NOT NULL{owner_id_line},
            state TEXT NOT NULL DEFAULT '{{}}',
            create_time REAL NOT NULL,
            update_time REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user
            ON {self._session_table}(app_name, user_id);
        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time
            ON {self._session_table}(update_time DESC);
        """

    async def _get_create_events_table_sql(self) -> str:
        """Get SQLite CREATE TABLE SQL for events.

        Returns:
            SQL statement to create adk_event table with indexes.

        Notes:
            - TEXT for IDs and indexed scalars
            - TEXT for full event JSON (event_data)
            - REAL for Julian Day timestamps
            - Foreign key to sessions with CASCADE delete
            - Index on (session_id, timestamp ASC)
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            invocation_id TEXT,
            timestamp REAL NOT NULL,
            event_data TEXT NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session
            ON {self._events_table}(session_id, timestamp ASC);
        """

    async def _get_create_app_states_table_sql(self) -> str:
        """Get SQLite CREATE TABLE SQL for app-scoped state."""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._app_state_table} (
            app_name TEXT PRIMARY KEY,
            state TEXT NOT NULL DEFAULT '{{}}',
            update_time REAL NOT NULL
        );
        """

    async def _get_create_user_states_table_sql(self) -> str:
        """Get SQLite CREATE TABLE SQL for user-scoped state."""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._user_state_table} (
            app_name TEXT NOT NULL,
            user_id TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT '{{}}',
            update_time REAL NOT NULL,
            PRIMARY KEY (app_name, user_id)
        );
        """

    async def _get_create_metadata_table_sql(self) -> str:
        """Get SQLite CREATE TABLE SQL for ADK internal metadata."""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._metadata_table} (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """

    async def _get_seed_metadata_sql(self) -> str:
        """Get SQLite SQL that seeds the ADK schema version metadata row."""
        return f"""
        INSERT OR IGNORE INTO {self._metadata_table} (key, value)
        VALUES ('schema_version', '1')
        """

    def _get_drop_app_states_table_sql(self) -> str:
        """Get SQLite DROP TABLE SQL for app-scoped state."""
        return f"DROP TABLE IF EXISTS {self._app_state_table}"

    def _get_drop_user_states_table_sql(self) -> str:
        """Get SQLite DROP TABLE SQL for user-scoped state."""
        return f"DROP TABLE IF EXISTS {self._user_state_table}"

    def _get_drop_metadata_table_sql(self) -> str:
        """Get SQLite DROP TABLE SQL for ADK internal metadata."""
        return f"DROP TABLE IF EXISTS {self._metadata_table}"

    def _get_drop_tables_sql(self) -> "list[str]":
        """Get SQLite DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop tables and indexes.

        Notes:
            Order matters: drop events table (child) before sessions (parent).
            SQLite automatically drops indexes when dropping tables.
        """
        return [
            self._get_drop_metadata_table_sql(),
            self._get_drop_user_states_table_sql(),
            self._get_drop_app_states_table_sql(),
            f"DROP TABLE IF EXISTS {self._events_table}",
            f"DROP TABLE IF EXISTS {self._session_table}",
        ]


class AiosqliteADKMemoryStore(BaseAsyncADKMemoryStore["AiosqliteConfig"]):
    """Aiosqlite ADK memory store using asynchronous SQLite driver.

    Implements memory entry storage for Google Agent Development Kit
    using SQLite via the asynchronous aiosqlite driver. Provides:
    - Session memory storage with JSON as TEXT
    - Simple LIKE search (simple strategy)
    - Optional FTS5 full-text search (sqlite_fts5 strategy)
    - Julian Day timestamps (REAL) for efficient date operations
    - Deduplication via event_id unique constraint
    - Efficient upserts using INSERT OR IGNORE

    Args:
        config: AiosqliteConfig with extension_config["adk"] settings.

    Example:
        from sqlspec.adapters.aiosqlite import AiosqliteConfig
        from sqlspec.adapters.aiosqlite.adk import AiosqliteADKMemoryStore

        config = AiosqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={
                "adk": {
                    "memory_table": "adk_memory_entries",
                    "memory_use_fts": False,
                    "memory_max_results": 20,
                }
            }
        )
        store = AiosqliteADKMemoryStore(config)
        await store.ensure_tables()

    Notes:
        - JSON stored as TEXT with SQLSpec serializers
        - REAL for Julian Day timestamps
        - event_id UNIQUE constraint for deduplication
        - Composite index on (app_name, user_id, timestamp DESC)
        - Optional FTS5 virtual table for full-text search
        - Configuration is read from config.extension_config["adk"]
    """

    __slots__ = ()

    def __init__(self, config: "AiosqliteConfig") -> None:
        """Initialize Aiosqlite ADK memory store.

        Args:
            config: AiosqliteConfig instance.

        Notes:
            Configuration is read from config.extension_config["adk"]:
            - memory_table: Memory table name (default: "adk_memory_entries")
            - memory_use_fts: Enable full-text search when supported (default: False)
            - memory_max_results: Max search results (default: 20)
            - owner_id_column: Optional owner FK column DDL (default: None)
            - enable_memory: Whether memory is enabled (default: True)
        """
        super().__init__(config)

    async def create_tables(self) -> None:
        """Create the memory table and indexes if they don't exist.

        Skips table creation if memory store is disabled.
        """
        if not self._enabled:
            return

        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._get_create_memory_table_sql())

    async def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication.

        Uses INSERT OR IGNORE to skip duplicates based on event_id unique constraint.
        """
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        inserted_count = 0
        async with self._config.provide_connection() as conn:
            for entry in entries:
                params: tuple[Any, ...]
                if self._owner_id_column_name:
                    sql = f"""
                    INSERT OR IGNORE INTO {self._memory_table}
                    (id, session_id, app_name, user_id, event_id, author,
                     {self._owner_id_column_name}, timestamp, content_json,
                     content_text, metadata_json, inserted_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    params = (
                        entry["id"],
                        entry["session_id"],
                        entry["app_name"],
                        entry["user_id"],
                        entry["event_id"],
                        entry["author"],
                        owner_id,
                        _datetime_to_julian(entry["timestamp"]),
                        to_json(entry["content_json"]),
                        entry["content_text"],
                        to_json(entry["metadata_json"]),
                        _datetime_to_julian(entry["inserted_at"]),
                    )
                else:
                    sql = f"""
                    INSERT OR IGNORE INTO {self._memory_table}
                    (id, session_id, app_name, user_id, event_id, author,
                     timestamp, content_json, content_text, metadata_json, inserted_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    params = (
                        entry["id"],
                        entry["session_id"],
                        entry["app_name"],
                        entry["user_id"],
                        entry["event_id"],
                        entry["author"],
                        _datetime_to_julian(entry["timestamp"]),
                        to_json(entry["content_json"]),
                        entry["content_text"],
                        to_json(entry["metadata_json"]),
                        _datetime_to_julian(entry["inserted_at"]),
                    )
                cursor = await conn.execute(sql, params)
                inserted_count += cursor.rowcount
                await cursor.close()
            await conn.commit()
        return inserted_count

    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not query:
            return []

        limit_value = limit or self._max_results
        if self._use_fts:
            sql = f"""
            SELECT m.* FROM {self._memory_table} AS m
            JOIN {self._memory_table}_fts AS fts ON m.rowid = fts.rowid
            WHERE m.app_name = ? AND m.user_id = ? AND fts.content_text MATCH ?
            ORDER BY m.timestamp DESC
            LIMIT ?
            """
            params = (app_name, user_id, query, limit_value)
        else:
            sql = f"""
            SELECT * FROM {self._memory_table}
            WHERE app_name = ? AND user_id = ? AND content_text LIKE ?
            ORDER BY timestamp DESC
            LIMIT ?
            """
            params = (app_name, user_id, f"%{query}%", limit_value)

        async with self._config.provide_connection() as conn:
            cursor = await conn.execute(sql, params)
            rows = await cursor.fetchall()
            columns = [col[0] for col in cursor.description or []]
            await cursor.close()
        records: list[MemoryRecord] = []
        for row in rows:
            raw = dict(zip(columns, row, strict=False))
            raw["timestamp"] = _julian_to_datetime(raw["timestamp"])
            raw["inserted_at"] = _julian_to_datetime(raw["inserted_at"])
            raw["content_json"] = from_json(raw["content_json"])
            raw["metadata_json"] = from_json(raw["metadata_json"]) if raw["metadata_json"] else None
            records.append(cast("MemoryRecord", raw))
        return records

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"DELETE FROM {self._memory_table} WHERE session_id = ?"
        async with self._config.provide_connection() as conn:
            cursor = await conn.execute(sql, (session_id,))
            await conn.commit()
            return cursor.rowcount

    async def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        cutoff = _datetime_to_julian(datetime.now(timezone.utc)) - days
        sql = f"DELETE FROM {self._memory_table} WHERE inserted_at < ?"
        async with self._config.provide_connection() as conn:
            cursor = await conn.execute(sql, (cutoff,))
            await conn.commit()
            return cursor.rowcount

    async def _get_create_memory_table_sql(self) -> str:
        """Get SQLite CREATE TABLE SQL for memory entries.

        Returns:
            SQL statement to create memory table with indexes.

        Notes:
            - TEXT for IDs, names, and JSON content
            - REAL for Julian Day timestamps
            - UNIQUE constraint on event_id for deduplication
            - Composite index on (app_name, user_id, timestamp DESC)
            - Optional owner ID column for multi-tenancy
            - Optional FTS5 virtual table for full-text search
        """
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        fts_table = ""
        if self._use_fts:
            fts_table = f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS {self._memory_table}_fts USING fts5(
            content_text,
            content={self._memory_table},
            content_rowid=rowid
        );

        CREATE TRIGGER IF NOT EXISTS {self._memory_table}_ai AFTER INSERT ON {self._memory_table} BEGIN
            INSERT INTO {self._memory_table}_fts(rowid, content_text) VALUES (new.rowid, new.content_text);
        END;

        CREATE TRIGGER IF NOT EXISTS {self._memory_table}_ad AFTER DELETE ON {self._memory_table} BEGIN
            INSERT INTO {self._memory_table}_fts({self._memory_table}_fts, rowid, content_text)
            VALUES('delete', old.rowid, old.content_text);
        END;

        CREATE TRIGGER IF NOT EXISTS {self._memory_table}_au AFTER UPDATE ON {self._memory_table} BEGIN
            INSERT INTO {self._memory_table}_fts({self._memory_table}_fts, rowid, content_text)
            VALUES('delete', old.rowid, old.content_text);
            INSERT INTO {self._memory_table}_fts(rowid, content_text) VALUES (new.rowid, new.content_text);
        END;
            """

        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            app_name TEXT NOT NULL,
            user_id TEXT NOT NULL,
            event_id TEXT NOT NULL UNIQUE,
            author TEXT{owner_id_line},
            timestamp REAL NOT NULL,
            content_json TEXT NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json TEXT,
            inserted_at REAL NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_app_user_time
            ON {self._memory_table}(app_name, user_id, timestamp DESC);

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_session
            ON {self._memory_table}(session_id);
        {fts_table}
        """

    def _get_drop_memory_table_sql(self) -> "list[str]":
        """Get SQLite DROP TABLE SQL statements."""
        statements = [f"DROP TABLE IF EXISTS {self._memory_table}"]
        if self._use_fts:
            statements.extend([
                f"DROP TABLE IF EXISTS {self._memory_table}_fts",
                f"DROP TRIGGER IF EXISTS {self._memory_table}_ai",
                f"DROP TRIGGER IF EXISTS {self._memory_table}_ad",
                f"DROP TRIGGER IF EXISTS {self._memory_table}_au",
            ])
        return statements
