"""SQLite sync ADK store for Google Agent Development Kit session/event storage."""

import re
import sqlite3
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Final, Literal, cast

from typing_extensions import NotRequired

from sqlspec.adapters.sqlite.config import _render_pragmas
from sqlspec.config import ADKConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.adk import BaseSyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    import logging

    from sqlspec.adapters.sqlite.config import SqliteConfig
    from sqlspec.extensions.adk import MemoryRecord

__all__ = ("SqliteADKConfig", "SqliteADKMemoryStore", "SqliteADKStore")


SECONDS_PER_DAY = 86400.0
JULIAN_EPOCH = 2440587.5
SQLITE_TABLE_NOT_FOUND_ERROR: Final = "no such table"
_FTS_DETAIL_VALUES: Final = frozenset({"full", "column", "none"})
_FTS_TOKENIZE_PATTERN: Final = re.compile(r"^[A-Za-z0-9_ -]+$")


logger: "logging.Logger" = get_logger("sqlspec.adapters.sqlite.adk.store")


class SqliteADKConfig(ADKConfig):
    """SQLite-specific ADK extension settings.

    Use these keys inside ``extension_config["adk"]`` with SQLite ADK stores.
    """

    pragma_overrides: "NotRequired[Mapping[str, str | int | bool]]"
    """Additional validated PRAGMA settings applied after the built-in ADK profile."""

    fts_tokenize: NotRequired[str]
    """Optional FTS5 tokenizer spec used when ``memory_use_fts`` is enabled."""

    fts_detail: NotRequired[Literal["full", "column", "none"]]
    """Optional FTS5 detail mode used when ``memory_use_fts`` is enabled."""


class SqliteADKStore(BaseSyncADKStore["SqliteConfig"]):
    """SQLite ADK store using synchronous SQLite driver.

    Implements session and event storage for Google Agent Development Kit
    using SQLite via the synchronous sqlite3 driver. Uses Litestar's sync_to_thread
    utility to provide an async interface compatible with the Store protocol.

    Provides:
        - Session state management with JSON storage (as TEXT)
        - Event history tracking with full-event JSON storage
        - Julian Day timestamps (REAL) for efficient date operations
        - Foreign key constraints with cascade delete
        - Atomic event+state writes via append_event_and_update_state
        - PRAGMA optimization profile for file-based databases

    Args:
        config: SqliteConfig instance with extension_config["adk"] settings.
    """

    __slots__ = ("_pragma_overrides",)

    def __init__(self, config: "SqliteConfig") -> None:
        """Initialize SQLite ADK store.

        Args:
            config: SqliteConfig instance.
        """
        super().__init__(config)
        self._pragma_overrides = _pragma_overrides(config)

    def create_tables(self) -> None:
        """Create both sessions and events tables if they don't exist."""
        """Synchronous implementation of create_tables."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        with self._config.provide_session() as driver:
            self._apply_pragmas(driver.connection)
            driver.execute_script(self._sessions_table_ddl())
            driver.execute_script(self._events_table_ddl())
            driver.execute_script(self._app_states_table_ddl())
            driver.execute_script(self._user_states_table_ddl())
            driver.execute_script(self._metadata_table_ddl())

    def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        """Create a new session.

        Args:
            session_id: Unique session identifier.
            app_name: Application name.
            user_id: User identifier.
            state: Initial session state.
            owner_id: Optional owner ID value for owner ID column.

        Returns:
            Created session record.
        """
        """Synchronous implementation of create_session."""
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

        with self._config.provide_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(sql, params)
            conn.commit()

        return SessionRecord(
            id=session_id, app_name=app_name, user_id=user_id, state=state, create_time=now, update_time=now
        )

    def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        """Get session by ID.

        Args:
            app_name: Application name.
            user_id: User identifier.
            session_id: Session identifier.
            renew_for: If positive, touch the session update timestamp while reading.

        Returns:
            Session record or None if not found.
        """
        """Synchronous implementation of get_session."""
        params = (app_name, user_id, session_id)
        update_params: tuple[Any, ...]
        if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
            update_sql = f"""
            UPDATE {self._session_table}
            SET update_time = ?
            WHERE app_name = ? AND user_id = ? AND id = ?
            """
            now_julian = _datetime_to_julian(datetime.now(timezone.utc))
            update_params = (now_julian, app_name, user_id, session_id)
        else:
            update_sql = ""
            update_params = ()

        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE app_name = ? AND user_id = ? AND id = ?
        """

        try:
            with self._config.provide_connection() as conn:
                self._apply_pragmas(conn)
                if update_sql:
                    conn.execute(update_sql, update_params)
                    conn.commit()
                cursor = conn.execute(sql, params)
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
        except sqlite3.OperationalError as exc:
            if SQLITE_TABLE_NOT_FOUND_ERROR in str(exc):
                return None
            raise

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state.

        Args:
            app_name: Application name.
            user_id: User identifier.
            session_id: Session identifier.
            state: New state dictionary (replaces existing state).
        """
        """Synchronous implementation of update_session_state."""
        now_julian = _datetime_to_julian(datetime.now(timezone.utc))
        state_json = to_json(state)

        sql = f"""
        UPDATE {self._session_table}
        SET state = ?, update_time = ?
        WHERE app_name = ? AND user_id = ? AND id = ?
        """

        with self._config.provide_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(sql, (state_json, now_julian, app_name, user_id, session_id))
            conn.commit()

    def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        """List sessions for an app, optionally filtered by user.

        Args:
            app_name: Application name.
            user_id: User identifier. If None, lists all sessions for the app.

        Returns:
            List of session records ordered by update_time DESC.
        """
        """Synchronous implementation of list_sessions."""
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
            with self._config.provide_connection() as conn:
                self._apply_pragmas(conn)
                cursor = conn.execute(sql, params)
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
        except sqlite3.OperationalError as exc:
            if SQLITE_TABLE_NOT_FOUND_ERROR in str(exc):
                return []
            raise

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete session and all associated events (cascade).

        Args:
            app_name: Application name.
            user_id: User identifier.
            session_id: Session identifier.
        """
        """Synchronous implementation of delete_session."""
        sql = f"DELETE FROM {self._session_table} WHERE app_name = ? AND user_id = ? AND id = ?"

        with self._config.provide_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(sql, (app_name, user_id, session_id))
            conn.commit()

    def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session.

        Args:
            event_record: Event record to store.
        """
        """Synchronous implementation of append_event."""
        timestamp_julian = _datetime_to_julian(event_record["timestamp"])
        event_data_json = to_json(event_record["event_data"])

        sql = f"""
        INSERT INTO {self._events_table} (
            id, app_name, user_id, session_id, invocation_id, timestamp, event_data
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        with self._config.provide_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(
                sql,
                (
                    event_record["id"],
                    event_record["app_name"],
                    event_record["user_id"],
                    event_record["session_id"],
                    event_record["invocation_id"],
                    timestamp_julian,
                    event_data_json,
                ),
            )
            conn.commit()

    def append_event_and_update_state(
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
        """Atomically append an event and update the session's durable state.

        Inserts the event and updates the session state + update_time in a
        single transaction, returning the updated SessionRecord via RETURNING.

        Args:
            event_record: Event record to store.
            app_name: Application name for scoped state.
            user_id: User identifier for scoped state.
            session_id: Session identifier whose state should be updated.
            state: Post-append durable state snapshot (temp: keys already
                stripped by the service layer).
            app_state: App-scoped state snapshot to upsert when changed.
            user_state: User-scoped state snapshot to upsert when changed.
        """
        """Synchronous implementation of append_event_and_update_state."""
        timestamp_julian = _datetime_to_julian(event_record["timestamp"])
        event_data_json = to_json(event_record["event_data"])
        now_julian = _datetime_to_julian(datetime.now(timezone.utc))
        state_json = to_json(state)

        insert_sql = f"""
        INSERT INTO {self._events_table} (
            id, app_name, user_id, session_id, invocation_id, timestamp, event_data
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
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

        with self._config.provide_connection() as conn:
            self._apply_pragmas(conn)
            try:
                cursor = conn.execute(update_sql, (state_json, now_julian, app_name, user_id, session_id))
                row = cursor.fetchone()
                if row is not None:
                    conn.execute(
                        insert_sql,
                        (
                            event_record["id"],
                            app_name,
                            user_id,
                            event_record["session_id"],
                            event_record["invocation_id"],
                            timestamp_julian,
                            event_data_json,
                        ),
                    )
                    if app_state is not None:
                        conn.execute(app_upsert_sql, (app_name, to_json(app_state), now_julian))
                    if user_state is not None:
                        conn.execute(user_upsert_sql, (app_name, user_id, to_json(user_state), now_julian))
            except Exception:
                conn.rollback()
                raise
            else:
                if row is None:
                    conn.rollback()
                else:
                    conn.commit()

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

    def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "list[EventRecord]":
        """Get events for a session.

        Args:
            app_name: Application name.
            user_id: User identifier.
            session_id: Session identifier.
            after_timestamp: Only return events after this time.
            limit: Maximum number of events to return.

        Returns:
            List of event records ordered by timestamp ASC.
        """
        """Synchronous implementation of get_events."""
        if limit == 0:
            return []

        where_clauses = ["app_name = ?", "user_id = ?", "session_id = ?"]
        params: list[Any] = [app_name, user_id, session_id]

        if after_timestamp is not None:
            where_clauses.append("timestamp > ?")
            params.append(_datetime_to_julian(after_timestamp))

        where_clause = " AND ".join(where_clauses)
        limit_clause = f" LIMIT {limit}" if limit else ""

        sql = f"""
        SELECT id, app_name, user_id, session_id, invocation_id, timestamp, event_data
        FROM {self._events_table}
        WHERE {where_clause}
        ORDER BY timestamp ASC{limit_clause}
        """

        try:
            with self._config.provide_connection() as conn:
                self._apply_pragmas(conn)
                cursor = conn.execute(sql, params)
                rows = cursor.fetchall()

                return [
                    EventRecord(
                        id=row[0],
                        app_name=row[1],
                        user_id=row[2],
                        session_id=row[3],
                        invocation_id=row[4],
                        timestamp=_julian_to_datetime(row[5]),
                        event_data=from_json(row[6]) if row[6] else {},
                    )
                    for row in rows
                ]
        except sqlite3.OperationalError as exc:
            if SQLITE_TABLE_NOT_FOUND_ERROR in str(exc):
                return []
            raise

    def delete_expired_events(self, before: datetime) -> int:
        """Delete events older than the given timestamp."""
        """Synchronous implementation of delete_expired_events."""
        sql = f"DELETE FROM {self._events_table} WHERE timestamp < ?"

        try:
            with self._config.provide_connection() as conn:
                self._apply_pragmas(conn)
                cursor = conn.execute(sql, (_datetime_to_julian(before),))
                deleted_count = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
                conn.commit()
                return deleted_count
        except sqlite3.OperationalError as exc:
            if SQLITE_TABLE_NOT_FOUND_ERROR in str(exc):
                return 0
            raise

    def delete_idle_sessions(self, updated_before: datetime) -> int:
        """Delete sessions whose update_time predates the given threshold."""
        """Synchronous implementation of delete_idle_sessions."""
        sql = f"DELETE FROM {self._session_table} WHERE update_time < ?"

        try:
            with self._config.provide_connection() as conn:
                self._apply_pragmas(conn)
                cursor = conn.execute(sql, (_datetime_to_julian(updated_before),))
                deleted_count = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
                conn.commit()
                return deleted_count
        except sqlite3.OperationalError as exc:
            if SQLITE_TABLE_NOT_FOUND_ERROR in str(exc):
                return 0
            raise

    def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        """Return app-scoped state for an application."""
        """Synchronous implementation of get_app_state."""
        sql = f"SELECT state FROM {self._app_state_table} WHERE app_name = ?"

        try:
            with self._config.provide_connection() as conn:
                self._apply_pragmas(conn)
                cursor = conn.execute(sql, (app_name,))
                row = cursor.fetchone()
                return from_json(row[0]) if row is not None and row[0] else None
        except sqlite3.OperationalError as exc:
            if SQLITE_TABLE_NOT_FOUND_ERROR in str(exc):
                return None
            raise

    def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        """Return user-scoped state for an application user."""
        """Synchronous implementation of get_user_state."""
        sql = f"""
        SELECT state
        FROM {self._user_state_table}
        WHERE app_name = ? AND user_id = ?
        """

        try:
            with self._config.provide_connection() as conn:
                self._apply_pragmas(conn)
                cursor = conn.execute(sql, (app_name, user_id))
                row = cursor.fetchone()
                return from_json(row[0]) if row is not None and row[0] else None
        except sqlite3.OperationalError as exc:
            if SQLITE_TABLE_NOT_FOUND_ERROR in str(exc):
                return None
            raise

    def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        """Insert or replace app-scoped state for an application."""
        """Synchronous implementation of upsert_app_state."""
        sql = f"""
        INSERT INTO {self._app_state_table} (app_name, state, update_time)
        VALUES (?, ?, ?)
        ON CONFLICT(app_name) DO UPDATE SET
            state = excluded.state,
            update_time = excluded.update_time
        """

        with self._config.provide_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(sql, (app_name, to_json(state), _datetime_to_julian(datetime.now(timezone.utc))))
            conn.commit()

    def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        """Insert or replace user-scoped state for an application user."""
        """Synchronous implementation of upsert_user_state."""
        sql = f"""
        INSERT INTO {self._user_state_table} (app_name, user_id, state, update_time)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(app_name, user_id) DO UPDATE SET
            state = excluded.state,
            update_time = excluded.update_time
        """

        with self._config.provide_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(sql, (app_name, user_id, to_json(state), _datetime_to_julian(datetime.now(timezone.utc))))
            conn.commit()

    def get_metadata(self, key: str) -> "str | None":
        """Return a value from the ADK internal metadata table."""
        """Synchronous implementation of get_metadata."""
        sql = f"SELECT value FROM {self._metadata_table} WHERE key = ?"

        try:
            with self._config.provide_connection() as conn:
                self._apply_pragmas(conn)
                cursor = conn.execute(sql, (key,))
                row = cursor.fetchone()
                return str(row[0]) if row is not None else None
        except sqlite3.OperationalError as exc:
            if SQLITE_TABLE_NOT_FOUND_ERROR in str(exc):
                return None
            raise

    def set_metadata(self, key: str, value: str) -> None:
        """Set a value in the ADK internal metadata table."""
        """Synchronous implementation of set_metadata."""
        sql = f"""
        INSERT INTO {self._metadata_table} (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """

        with self._config.provide_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(sql, (key, value))
            conn.commit()

    def _apply_pragmas(self, connection: Any) -> None:
        """Apply PRAGMA optimization profile for this connection.

        Args:
            connection: SQLite connection.
        """
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA cache_size = -64000")
        connection.execute("PRAGMA mmap_size = 30000000")
        connection.execute("PRAGMA journal_size_limit = 67108864")
        for pragma_name, pragma_value in self._pragma_overrides:
            connection.execute(f"PRAGMA {pragma_name} = {pragma_value}")

    def _sessions_table_ddl(self) -> str:
        """Get SQLite CREATE TABLE SQL for sessions.

        Returns:
            SQL statement to create adk_session table with indexes.
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

    def _events_table_ddl(self) -> str:
        """Get SQLite CREATE TABLE SQL for events."""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id TEXT PRIMARY KEY,
            app_name TEXT NOT NULL,
            user_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            invocation_id TEXT,
            timestamp REAL NOT NULL,
            event_data TEXT NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session
            ON {self._events_table}(app_name, user_id, session_id, timestamp ASC);
        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_invocation
            ON {self._events_table}(invocation_id);
        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_timestamp
            ON {self._events_table}(timestamp ASC);
        """

    def _app_states_table_ddl(self) -> str:
        """Get SQLite CREATE TABLE SQL for app-scoped state."""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._app_state_table} (
            app_name TEXT PRIMARY KEY,
            state TEXT NOT NULL DEFAULT '{{}}',
            update_time REAL NOT NULL
        );
        """

    def _user_states_table_ddl(self) -> str:
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

    def _metadata_table_ddl(self) -> str:
        """Get SQLite CREATE TABLE SQL for ADK internal metadata."""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._metadata_table} (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """

    def _drop_app_states_table_sql(self) -> str:
        """Get SQLite DROP TABLE SQL for app-scoped state."""
        return f"DROP TABLE IF EXISTS {self._app_state_table}"

    def _drop_user_states_table_sql(self) -> str:
        """Get SQLite DROP TABLE SQL for user-scoped state."""
        return f"DROP TABLE IF EXISTS {self._user_state_table}"

    def _drop_metadata_table_sql(self) -> str:
        """Get SQLite DROP TABLE SQL for ADK internal metadata."""
        return f"DROP TABLE IF EXISTS {self._metadata_table}"

    def _drop_tables_sql(self) -> "list[str]":
        """Get SQLite DROP TABLE SQL statements."""
        return [
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            f"DROP TABLE IF EXISTS {self._events_table}",
            f"DROP TABLE IF EXISTS {self._session_table}",
        ]


class SqliteADKMemoryStore(BaseSyncADKMemoryStore["SqliteConfig"]):
    """SQLite ADK memory store using synchronous SQLite driver.

    Implements memory entry storage for Google Agent Development Kit
    using SQLite via the synchronous sqlite3 driver. Provides:
    - Session memory storage with JSON as TEXT
    - Simple LIKE search (simple strategy)
    - Optional FTS5 full-text search (sqlite_fts5 strategy)
    - Julian Day timestamps (REAL) for efficient date operations
    - Deduplication via event_id unique constraint
    - Efficient upserts using INSERT OR IGNORE

    Args:
        config: SqliteConfig with extension_config["adk"] settings.
    """

    __slots__ = ("_fts_options",)

    def __init__(self, config: "SqliteConfig") -> None:
        """Initialize SQLite ADK memory store.

        Args:
            config: SqliteConfig instance.
        """
        super().__init__(config)
        self._fts_options = _fts_options(config)

    def create_tables(self) -> None:
        """Create tables if they don't exist."""
        """Create the memory table and indexes if they don't exist.

        Skips table creation if memory store is disabled.
        """
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        if not self._enabled:
            return

        with self._config.provide_session() as driver:
            self._enable_foreign_keys(driver.connection)
            driver.execute_script(self._memory_table_ddl())

    def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication."""
        """Bulk insert memory entries with deduplication.

        Uses INSERT OR IGNORE to skip duplicates based on event_id
        unique constraint.

        Args:
            entries: List of memory records to insert.
            owner_id: Optional owner ID value for owner_id_column (if configured).

        Returns:
            Number of entries actually inserted (excludes duplicates).

        Raises:
            RuntimeError: If memory store is disabled.
        """
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        inserted_count = 0
        with self._config.provide_connection() as conn:
            self._enable_foreign_keys(conn)

            for entry in entries:
                timestamp_julian = _datetime_to_julian(entry["timestamp"])
                inserted_at_julian = _datetime_to_julian(entry["inserted_at"])
                content_json_str = to_json(entry["content_json"])
                metadata_json_str = to_json(entry["metadata_json"]) if entry["metadata_json"] else None

                if self._owner_id_column_name:
                    sql = f"""
                    INSERT OR IGNORE INTO {self._memory_table}
                    (id, session_id, app_name, user_id, event_id, author,
                     {self._owner_id_column_name}, timestamp, content_json,
                     content_text, metadata_json, inserted_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    params: tuple[Any, ...] = (
                        entry["id"],
                        entry["session_id"],
                        entry["app_name"],
                        entry["user_id"],
                        entry["event_id"],
                        entry["author"],
                        owner_id,
                        timestamp_julian,
                        content_json_str,
                        entry["content_text"],
                        metadata_json_str,
                        inserted_at_julian,
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
                        timestamp_julian,
                        content_json_str,
                        entry["content_text"],
                        metadata_json_str,
                        inserted_at_julian,
                    )

                cursor = conn.execute(sql, params)
                if cursor.rowcount > 0:
                    inserted_count += 1

            conn.commit()

        return inserted_count

    def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query."""
        """Search memory entries by text query.

        Args:
            query: Text query to search for.
            app_name: Application name to filter by.
            user_id: User ID to filter by.
            limit: Maximum number of results (defaults to max_results config).

        Returns:
            List of matching memory records ordered by relevance/timestamp.

        Raises:
            RuntimeError: If memory store is disabled.
        """
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        effective_limit = limit if limit is not None else self._max_results

        if self._use_fts:
            try:
                return self._search_entries_fts(query, app_name, user_id, effective_limit)
            except Exception as exc:  # pragma: no cover
                logger.warning("FTS search failed; falling back to simple search: %s", exc)
        return self._search_entries_simple(query, app_name, user_id, effective_limit)

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        """Delete all memory entries for a specific session.

        Args:
            session_id: Session ID to delete entries for.

        Returns:
            Number of entries deleted.
        """
        sql = f"DELETE FROM {self._memory_table} WHERE session_id = ?"

        with self._config.provide_connection() as conn:
            self._enable_foreign_keys(conn)
            cursor = conn.execute(sql, (session_id,))
            deleted_count = cursor.rowcount
            conn.commit()

        return deleted_count

    def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        """Delete memory entries older than specified days.

        Used for TTL cleanup operations.

        Args:
            days: Number of days to retain entries.

        Returns:
            Number of entries deleted.
        """
        cutoff_julian = _datetime_to_julian(datetime.now(timezone.utc)) - days

        sql = f"DELETE FROM {self._memory_table} WHERE inserted_at < ?"

        with self._config.provide_connection() as conn:
            self._enable_foreign_keys(conn)
            cursor = conn.execute(sql, (cutoff_julian,))
            deleted_count = cursor.rowcount
            conn.commit()

        return deleted_count

    def _memory_table_ddl(self) -> str:
        """Get SQLite CREATE TABLE SQL for memory entries.

        Returns:
            SQL statement to create memory table with indexes.
        """
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        fts_table = ""
        if self._use_fts:
            fts_options = _format_fts_options(self._fts_options)
            fts_table = f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS {self._memory_table}_fts USING fts5(
            content_text,
            content={self._memory_table},
            content_rowid=rowid{fts_options}
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

    def _drop_memory_table_sql(self) -> "list[str]":
        """Get SQLite DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop the memory table and FTS table.
        """
        statements = [f"DROP TABLE IF EXISTS {self._memory_table}"]
        if self._use_fts:
            statements.insert(0, f"DROP TABLE IF EXISTS {self._memory_table}_fts")
        return statements

    def _enable_foreign_keys(self, connection: Any) -> None:
        """Enable foreign key constraints for this connection.

        Args:
            connection: SQLite connection.
        """
        connection.execute("PRAGMA foreign_keys = ON")

    def _search_entries_fts(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = f"""
        SELECT m.id, m.session_id, m.app_name, m.user_id, m.event_id, m.author,
               m.timestamp, m.content_json, m.content_text, m.metadata_json, m.inserted_at
        FROM {self._memory_table} m
        JOIN {self._memory_table}_fts fts ON m.rowid = fts.rowid
        WHERE m.app_name = ?
          AND m.user_id = ?
          AND fts.content_text MATCH ?
        ORDER BY m.timestamp DESC
        LIMIT ?
        """
        params: tuple[Any, ...] = (app_name, user_id, query, limit)
        return self._fetch_records(sql, params)

    def _search_entries_simple(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = f"""
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at
        FROM {self._memory_table}
        WHERE app_name = ?
          AND user_id = ?
          AND content_text LIKE ?
        ORDER BY timestamp DESC
        LIMIT ?
        """
        pattern = f"%{query}%"
        params = (app_name, user_id, pattern, limit)
        return self._fetch_records(sql, params)

    def _fetch_records(self, sql: str, params: "tuple[Any, ...]") -> "list[MemoryRecord]":
        with self._config.provide_connection() as conn:
            self._enable_foreign_keys(conn)
            cursor = conn.execute(sql, params)
            rows = cursor.fetchall()
        return [
            {
                "id": row[0],
                "session_id": row[1],
                "app_name": row[2],
                "user_id": row[3],
                "event_id": row[4],
                "author": row[5],
                "timestamp": _julian_to_datetime(row[6]),
                "content_json": from_json(row[7]) if row[7] else {},
                "content_text": row[8],
                "metadata_json": from_json(row[9]) if row[9] else None,
                "inserted_at": _julian_to_datetime(row[10]),
            }
            for row in rows
        ]


def _adk_config(config: "SqliteConfig") -> "dict[str, Any]":
    """Return the adapter-local ADK extension configuration."""

    return dict(cast("dict[str, Any]", config.extension_config.get("adk", {})))


def _pragma_overrides(config: "SqliteConfig") -> "list[tuple[str, str]]":
    """Return validated ADK PRAGMA overrides for SQLite stores."""

    adk_config = _adk_config(config)
    pragma_overrides = adk_config.get("pragma_overrides")
    if pragma_overrides is None:
        return []
    if not isinstance(pragma_overrides, Mapping):
        msg = "extension_config['adk']['pragma_overrides'] must be a mapping of PRAGMA names to values"
        raise ImproperConfigurationError(msg)
    try:
        return _render_pragmas(pragma_overrides)
    except ImproperConfigurationError as exc:
        msg = str(exc).replace("driver_features['pragmas']", "extension_config['adk']['pragma_overrides']")
        raise ImproperConfigurationError(msg) from exc


def _fts_options(config: "SqliteConfig") -> "tuple[str, ...]":
    """Return validated FTS5 options for SQLite memory DDL."""

    adk_config = _adk_config(config)
    options: list[str] = []

    fts_tokenize = adk_config.get("fts_tokenize")
    if fts_tokenize is not None:
        if not isinstance(fts_tokenize, str) or _FTS_TOKENIZE_PATTERN.match(fts_tokenize) is None:
            msg = "extension_config['adk']['fts_tokenize'] must contain only safe FTS5 tokenizer characters"
            raise ImproperConfigurationError(msg)
        options.append(f"tokenize = '{fts_tokenize}'")

    fts_detail = adk_config.get("fts_detail")
    if fts_detail is not None:
        if not isinstance(fts_detail, str) or fts_detail not in _FTS_DETAIL_VALUES:
            msg = "extension_config['adk']['fts_detail'] must be 'full', 'column', or 'none'"
            raise ImproperConfigurationError(msg)
        options.append(f"detail = {fts_detail}")

    return tuple(options)


def _format_fts_options(options: "tuple[str, ...]") -> str:
    """Format validated FTS5 options for a CREATE VIRTUAL TABLE statement."""

    if not options:
        return ""
    return ",\n            " + ",\n            ".join(options)


def _datetime_to_julian(dt: datetime) -> float:
    """Convert datetime to Julian Day number for SQLite storage.

    Args:
        dt: Datetime to convert (must be UTC-aware).

    Returns:
        Julian Day number as REAL.
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
