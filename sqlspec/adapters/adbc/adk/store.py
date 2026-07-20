"""ADBC ADK store for Google Agent Development Kit session/event storage."""

import contextlib
import re
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Final

from sqlspec.extensions.adk import BaseSyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from sqlspec.adapters.adbc.config import AdbcConfig
    from sqlspec.extensions.adk import MemoryRecord

__all__ = ("AdbcADKMemoryStore", "AdbcADKStore")

logger = get_logger("sqlspec.adapters.adbc.adk.store")


DIALECT_POSTGRESQL: Final = "postgresql"
DIALECT_SQLITE: Final = "sqlite"
DIALECT_DUCKDB: Final = "duckdb"
DIALECT_SNOWFLAKE: Final = "snowflake"
DIALECT_GENERIC: Final = "generic"

ADBC_TABLE_NOT_FOUND_PATTERNS: Final = (
    "no such table",
    "table or view does not exist",
    "relation does not exist",
    "does not exist",
    "table with name",
)


class AdbcADKStore(BaseSyncADKStore["AdbcConfig"]):
    """ADBC synchronous ADK store for Arrow Database Connectivity.

    Implements session and event storage for Google Agent Development Kit
    using ADBC. ADBC provides a vendor-neutral API with Arrow-native data
    transfer across multiple databases (PostgreSQL, SQLite, DuckDB, etc.).

    Events use the clean-break contract: id, session_id, invocation_id,
    timestamp, and event_data. The full ADK Event payload is stored as a
    single JSON blob in event_data using a dialect-appropriate column type
    (JSONB for PostgreSQL, JSON for DuckDB, VARIANT for Snowflake, TEXT for
    SQLite and generic fallback).

    Provides:
        - Session state management with JSON serialization
        - Event history tracking via single event_data blob
        - Atomic event insert + session state update
        - Timezone-aware timestamps
        - Foreign key constraints with cascade delete
        - Database-agnostic SQL (supports multiple backends)

    Args:
        config: AdbcConfig with extension_config["adk"] settings.
    """

    __slots__ = ("_dialect",)

    def __init__(self, config: "AdbcConfig") -> None:
        """Initialize ADBC ADK store.

        Args:
            config: AdbcConfig instance (any ADBC driver).
        """
        super().__init__(config)
        self._dialect = self._detect_dialect()

    def create_tables(self) -> None:
        """Create tables if they don't exist."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        self._create_tables()

    def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        """Create a new session."""
        return self._create_session(session_id, app_name, user_id, state, owner_id)

    def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        """Get session by ID."""
        return self._get_session(app_name, user_id, session_id, renew_for=renew_for)

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state."""
        self._update_session_state(app_name, user_id, session_id, state)

    def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        """List sessions for an app."""
        return self._list_sessions(app_name, user_id)

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete session and associated events."""
        self._delete_session(app_name, user_id, session_id)

    def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session."""
        self._append_event(event_record)

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
        """Atomically append an event and update the session's durable state."""
        return self._append_event_and_update_state(
            event_record, app_name, user_id, session_id, state, app_state=app_state, user_state=user_state
        )

    def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "list[EventRecord]":
        """Get events for a session."""
        return self._get_events(app_name, user_id, session_id, after_timestamp, limit)

    def delete_expired_events(self, before: datetime) -> int:
        """Delete events older than a timestamp."""
        return self._delete_expired_events(before)

    def delete_idle_sessions(self, updated_before: datetime) -> int:
        """Delete sessions older than a timestamp."""
        return self._delete_idle_sessions(updated_before)

    def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        """Return app-scoped state."""
        return self._get_app_state(app_name)

    def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        """Return user-scoped state."""
        return self._get_user_state(app_name, user_id)

    def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        """Insert or replace app-scoped state."""
        self._upsert_app_state(app_name, state)

    def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        """Insert or replace user-scoped state."""
        self._upsert_user_state(app_name, user_id, state)

    def get_metadata(self, key: str) -> "str | None":
        """Return a metadata value."""
        return self._get_metadata(key)

    def set_metadata(self, key: str, value: str) -> None:
        """Set a metadata value."""
        self._set_metadata(key, value)

    def dialect(self) -> str:
        """Return the detected database dialect."""
        return self._dialect

    def _detect_dialect(self) -> str:
        """Detect ADBC driver dialect from connection config.

        Returns:
            Dialect identifier for DDL generation.
        """
        driver_name = self._config.connection_config.get("driver_name", "").lower()

        if "postgres" in driver_name:
            return DIALECT_POSTGRESQL
        if "sqlite" in driver_name:
            return DIALECT_SQLITE
        if "duckdb" in driver_name:
            return DIALECT_DUCKDB
        if "snowflake" in driver_name:
            return DIALECT_SNOWFLAKE

        logger.warning(
            "Unknown ADBC driver: %s. Using generic SQL dialect. "
            "Consider using a direct adapter for better performance.",
            driver_name,
        )
        return DIALECT_GENERIC

    def _serialize_state(self, state: "dict[str, Any]") -> str:
        """Serialize state dictionary to JSON string.

        Args:
            state: State dictionary to serialize.

        Returns:
            JSON string.
        """
        return to_json(state)

    def _deserialize_state(self, data: Any) -> "dict[str, Any]":
        """Deserialize state data from JSON string.

        Args:
            data: JSON string from database.

        Returns:
            Deserialized state dictionary.
        """
        if data is None:
            return {}
        return from_json(str(data))  # type: ignore[no-any-return]

    def _serialize_json_field(self, value: Any) -> "str | None":
        """Serialize optional JSON field for event storage.

        Args:
            value: Value to serialize (dict or None).

        Returns:
            Serialized JSON string or None.
        """
        if value is None:
            return None
        return to_json(value)

    def _json_storage_type(self) -> str:
        if self._dialect == DIALECT_POSTGRESQL:
            return "JSONB"
        if self._dialect == DIALECT_DUCKDB:
            return "JSON"
        if self._dialect == DIALECT_SNOWFLAKE:
            return "VARIANT"
        return "TEXT"

    def _timestamp_storage_type(self) -> str:
        if self._dialect == DIALECT_POSTGRESQL:
            return "TIMESTAMPTZ"
        if self._dialect == DIALECT_SNOWFLAKE:
            return "TIMESTAMP_TZ"
        return "TIMESTAMP"

    def _deserialize_json_field(self, data: Any) -> "dict[str, Any] | None":
        """Deserialize optional JSON field from database.

        Args:
            data: JSON string from database or None.

        Returns:
            Deserialized dictionary or None.
        """
        if data is None:
            return None
        return from_json(str(data))  # type: ignore[no-any-return]

    def _sessions_table_ddl(self) -> str:
        """Get CREATE TABLE SQL for sessions with dialect dispatch.

        Returns:
            SQL statement to create adk_sessions table.
        """
        if self._dialect == DIALECT_POSTGRESQL:
            return self._sessions_ddl_postgresql()
        if self._dialect == DIALECT_SQLITE:
            return self._sessions_ddl_sqlite()
        if self._dialect == DIALECT_DUCKDB:
            return self._sessions_ddl_duckdb()
        if self._dialect == DIALECT_SNOWFLAKE:
            return self._sessions_ddl_snowflake()
        return self._sessions_ddl_generic()

    def _sessions_ddl_postgresql(self) -> str:
        """PostgreSQL DDL with JSONB and TIMESTAMPTZ.

        Returns:
            SQL to create sessions table optimized for PostgreSQL.
        """
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id VARCHAR(128) PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL{owner_id_ddl},
            state JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """

    def _sessions_ddl_sqlite(self) -> str:
        """SQLite DDL with TEXT and REAL timestamps.

        Returns:
            SQL to create sessions table optimized for SQLite.
        """
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id TEXT PRIMARY KEY,
            app_name TEXT NOT NULL,
            user_id TEXT NOT NULL{owner_id_ddl},
            state TEXT NOT NULL DEFAULT '{{}}',
            create_time REAL NOT NULL,
            update_time REAL NOT NULL
        )
        """

    def _sessions_ddl_duckdb(self) -> str:
        """DuckDB DDL with native JSON type.

        Returns:
            SQL to create sessions table optimized for DuckDB.
        """
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id VARCHAR(128) PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL{owner_id_ddl},
            state JSON NOT NULL,
            create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """

    def _sessions_ddl_snowflake(self) -> str:
        """Snowflake DDL with VARIANT type.

        Returns:
            SQL to create sessions table optimized for Snowflake.
        """
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id VARCHAR PRIMARY KEY,
            app_name VARCHAR NOT NULL,
            user_id VARCHAR NOT NULL{owner_id_ddl},
            state VARIANT NOT NULL,
            create_time TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            update_time TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
        )
        """

    def _sessions_ddl_generic(self) -> str:
        """Generic SQL-92 compatible DDL fallback.

        Returns:
            SQL to create sessions table using generic types.
        """
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id VARCHAR(128) PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL{owner_id_ddl},
            state TEXT NOT NULL DEFAULT '{{}}',
            create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """

    def _events_table_ddl(self) -> str:
        """Get CREATE TABLE SQL for events with dialect dispatch.

        Returns:
            SQL statement to create adk_events table.
        """
        if self._dialect == DIALECT_POSTGRESQL:
            return self._events_ddl_postgresql()
        if self._dialect == DIALECT_SQLITE:
            return self._events_ddl_sqlite()
        if self._dialect == DIALECT_DUCKDB:
            return self._events_ddl_duckdb()
        if self._dialect == DIALECT_SNOWFLAKE:
            return self._events_ddl_snowflake()
        return self._events_ddl_generic()

    def _events_ddl_postgresql(self) -> str:
        """PostgreSQL DDL for events table.

        Returns:
            SQL to create events table optimized for PostgreSQL.
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256),
            timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_data JSONB NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        )
        """

    def _events_ddl_sqlite(self) -> str:
        """SQLite DDL for events table.

        Returns:
            SQL to create events table optimized for SQLite.
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            invocation_id TEXT,
            timestamp REAL NOT NULL,
            event_data TEXT NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        )
        """

    def _events_ddl_duckdb(self) -> str:
        """DuckDB DDL for events table.

        Returns:
            SQL to create events table optimized for DuckDB.
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256),
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_data JSON NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id)
        )
        """

    def _events_ddl_snowflake(self) -> str:
        """Snowflake DDL for events table.

        Returns:
            SQL to create events table optimized for Snowflake.
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR PRIMARY KEY,
            session_id VARCHAR NOT NULL,
            invocation_id VARCHAR,
            timestamp TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            event_data VARIANT NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id)
        )
        """

    def _events_ddl_generic(self) -> str:
        """Generic SQL-92 compatible DDL for events table.

        Returns:
            SQL to create events table using generic types.
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256),
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_data TEXT NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        )
        """

    def _app_states_table_ddl(self) -> str:
        json_type = self._json_storage_type()
        timestamp_type = self._timestamp_storage_type()
        default = "DEFAULT CURRENT_TIMESTAMP()" if self._dialect == DIALECT_SNOWFLAKE else "DEFAULT CURRENT_TIMESTAMP"
        return f"""
        CREATE TABLE IF NOT EXISTS {self._app_state_table} (
            app_name VARCHAR(128) PRIMARY KEY,
            state {json_type} NOT NULL,
            update_time {timestamp_type} NOT NULL {default}
        )
        """

    def _user_states_table_ddl(self) -> str:
        json_type = self._json_storage_type()
        timestamp_type = self._timestamp_storage_type()
        default = "DEFAULT CURRENT_TIMESTAMP()" if self._dialect == DIALECT_SNOWFLAKE else "DEFAULT CURRENT_TIMESTAMP"
        return f"""
        CREATE TABLE IF NOT EXISTS {self._user_state_table} (
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            state {json_type} NOT NULL,
            update_time {timestamp_type} NOT NULL {default},
            PRIMARY KEY (app_name, user_id)
        )
        """

    def _metadata_table_ddl(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._metadata_table} (
            key VARCHAR(128) PRIMARY KEY,
            value VARCHAR(512) NOT NULL
        )
        """

    def _drop_app_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._app_state_table}"

    def _drop_user_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._user_state_table}"

    def _drop_metadata_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._metadata_table}"

    def _drop_tables_sql(self) -> "list[str]":
        """Get DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop tables and indexes.
        """
        return [
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            f"DROP TABLE IF EXISTS {self._events_table}",
            f"DROP TABLE IF EXISTS {self._session_table}",
        ]

    def _create_tables(self) -> None:
        """Create both sessions and events tables if they don't exist."""
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._enable_foreign_keys(cursor, conn)

                cursor.execute(self._sessions_table_ddl())
                conn.commit()

                sessions_idx_app_user = f"CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user ON {self._session_table}(app_name, user_id)"
                cursor.execute(sessions_idx_app_user)
                conn.commit()

                sessions_idx_update = f"CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time ON {self._session_table}(update_time DESC)"
                cursor.execute(sessions_idx_update)
                conn.commit()

                cursor.execute(self._events_table_ddl())
                conn.commit()

                events_idx = f"CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session ON {self._events_table}(session_id, timestamp ASC)"
                cursor.execute(events_idx)
                conn.commit()

                cursor.execute(self._app_states_table_ddl())
                conn.commit()

                cursor.execute(self._user_states_table_ddl())
                conn.commit()

                cursor.execute(self._metadata_table_ddl())
                conn.commit()
            finally:
                cursor.close()

    def _enable_foreign_keys(self, cursor: Any, conn: Any) -> None:
        """Enable foreign key constraints for SQLite.

        Args:
            cursor: Database cursor.
            conn: Database connection.
        """
        if self._dialect != DIALECT_SQLITE:
            return
        try:
            cursor.execute("PRAGMA foreign_keys = ON")
            conn.commit()
        except Exception:
            logger.debug("Foreign key enforcement not supported or already enabled")

    def _format_sql(self, sql: str) -> str:
        """Return SQL with dialect-appropriate positional placeholders."""
        if self._dialect != DIALECT_POSTGRESQL:
            return sql
        index = 0

        def replace_placeholder(_match: Any) -> str:
            nonlocal index
            index += 1
            return f"${index}"

        return re.sub(r"\?", replace_placeholder, sql)

    def _execute(self, cursor: Any, sql: str, params: "tuple[Any, ...] | list[Any]") -> Any:
        """Execute parameterized SQL using the current ADBC dialect's placeholder style."""
        return cursor.execute(self._format_sql(sql), params)

    def _json_placeholder(self) -> str:
        """Return a JSON parameter placeholder for the current dialect."""
        if self._dialect == DIALECT_POSTGRESQL:
            return "?::jsonb"
        return "?"

    def _create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        """Create a new session.

        Args:
            session_id: Unique session identifier.
            app_name: Application name.
            user_id: User identifier.
            state: Initial session state.
            owner_id: Optional owner ID value for owner_id_column (can be None for nullable columns).

        Returns:
            Created session record.
        """
        state_json = self._serialize_state(state)
        state_placeholder = self._json_placeholder()

        params: tuple[Any, ...]
        if self._owner_id_column_name:
            sql = f"""
            INSERT INTO {self._session_table}
            (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
            VALUES (?, ?, ?, ?, {state_placeholder}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
            params = (session_id, app_name, user_id, owner_id, state_json)
        else:
            sql = f"""
            INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
            VALUES (?, ?, ?, {state_placeholder}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
            params = (session_id, app_name, user_id, state_json)

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._execute(cursor, sql, params)
                conn.commit()
            finally:
                cursor.close()

        result = self._get_session(app_name, user_id, session_id)
        if result is None:
            msg = "Failed to fetch created session"
            raise RuntimeError(msg)
        return result

    def _get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        """Get session by ID.

        Args:
            app_name: Application name.
            user_id: User identifier.
            session_id: Session identifier.
            renew_for: If positive, touch the session update timestamp.

        Returns:
            Session record or None if not found.
        """
        if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
            sql = f"""
            UPDATE {self._session_table}
            SET update_time = CURRENT_TIMESTAMP
            WHERE app_name = ? AND user_id = ? AND id = ?
            """
            params: tuple[Any, ...] = (app_name, user_id, session_id)
            select_after_update = True
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = ? AND user_id = ? AND id = ?
            """
            params = (app_name, user_id, session_id)
            select_after_update = False

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    self._execute(cursor, sql, params)
                    if select_after_update:
                        conn.commit()
                        self._execute(
                            cursor,
                            f"""
                            SELECT id, app_name, user_id, state, create_time, update_time
                            FROM {self._session_table}
                            WHERE app_name = ? AND user_id = ? AND id = ?
                            """,
                            params,
                        )
                    row = cursor.fetchone()

                    if row is None:
                        return None

                    return SessionRecord(
                        id=row[0],
                        app_name=row[1],
                        user_id=row[2],
                        state=self._deserialize_state(row[3]),
                        create_time=row[4],
                        update_time=row[5],
                    )
                finally:
                    cursor.close()
        except Exception as e:
            error_msg = str(e).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return None
            raise

    def _update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state.

        Args:
            app_name: Application name.
            user_id: User identifier.
            session_id: Session identifier.
            state: New state dictionary (replaces existing state).
        """
        state_json = self._serialize_state(state)
        sql = f"""
        UPDATE {self._session_table}
        SET state = {self._json_placeholder()}, update_time = CURRENT_TIMESTAMP
        WHERE app_name = ? AND user_id = ? AND id = ?
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._execute(cursor, sql, (state_json, app_name, user_id, session_id))
                conn.commit()
            finally:
                cursor.close()

    def _delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete session and all associated events (cascade).

        Args:
            app_name: Application name.
            user_id: User identifier.
            session_id: Session identifier.
        """
        delete_events_sql = f"DELETE FROM {self._events_table} WHERE session_id = ?"
        delete_session_sql = f"DELETE FROM {self._session_table} WHERE app_name = ? AND user_id = ? AND id = ?"

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._enable_foreign_keys(cursor, conn)
                self._execute(cursor, delete_events_sql, (session_id,))
                if self._dialect == DIALECT_DUCKDB:
                    conn.commit()
                self._execute(cursor, delete_session_sql, (app_name, user_id, session_id))
                conn.commit()
            finally:
                cursor.close()

    def _list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        """List sessions for an app, optionally filtered by user.

        Args:
            app_name: Application name.
            user_id: User identifier. If None, lists all sessions for the app.

        Returns:
            List of session records ordered by update_time DESC.
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
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    self._execute(cursor, sql, params)
                    rows = cursor.fetchall()

                    return [
                        SessionRecord(
                            id=row[0],
                            app_name=row[1],
                            user_id=row[2],
                            state=self._deserialize_state(row[3]),
                            create_time=row[4],
                            update_time=row[5],
                        )
                        for row in rows
                    ]
                finally:
                    cursor.close()
        except Exception as e:
            error_msg = str(e).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return []
            raise

    def _insert_event(self, event_record: "EventRecord") -> None:
        """Insert an event record into the events table.

        Args:
            event_record: Event record to store.
        """
        event_data = self._serialize_json_field(event_record["event_data"])
        sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, invocation_id, timestamp, event_data
        ) VALUES (?, ?, ?, ?, {self._json_placeholder()})
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._execute(
                    cursor,
                    sql,
                    (
                        event_record["id"],
                        event_record["session_id"],
                        event_record["invocation_id"],
                        event_record["timestamp"],
                        event_data,
                    ),
                )
                conn.commit()
            finally:
                cursor.close()

    def _append_event_and_update_state(
        self,
        event_record: "EventRecord",
        app_name: str,
        user_id: str,
        session_id: str,
        state: "dict[str, Any]",
        *,
        app_state: "dict[str, Any] | None" = None,
        user_state: "dict[str, Any] | None" = None,
    ) -> SessionRecord:
        """Atomically insert an event and update the session's durable state.

        The event insert, state update, and refresh-SELECT are executed within
        a single connection and committed together. ADBC drivers wrap a
        variety of backends (Postgres, SQLite, DuckDB, ...) so we use a
        SELECT-after-UPDATE rather than relying on RETURNING which not every
        backend supports.

        Args:
            event_record: Event record to store.
            app_name: Application name.
            user_id: User identifier.
            session_id: Session identifier whose state should be updated.
            state: Post-append durable state snapshot (``temp:`` keys already
                stripped by the service layer).
            app_state: Optional app-scoped state snapshot.
            user_state: Optional user-scoped state snapshot.
        """
        insert_sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, invocation_id, timestamp, event_data
        ) VALUES (?, ?, ?, ?, {self._json_placeholder()})
        """
        update_sql = f"""
        UPDATE {self._session_table}
        SET state = {self._json_placeholder()}, update_time = CURRENT_TIMESTAMP
        WHERE app_name = ? AND user_id = ? AND id = ?
        """
        select_sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE app_name = ? AND user_id = ? AND id = ?
        """
        delete_app_state_sql = f"DELETE FROM {self._app_state_table} WHERE app_name = ?"
        insert_app_state_sql = f"""
        INSERT INTO {self._app_state_table} (app_name, state, update_time)
        VALUES (?, {self._json_placeholder()}, ?)
        """
        delete_user_state_sql = f"DELETE FROM {self._user_state_table} WHERE app_name = ? AND user_id = ?"
        insert_user_state_sql = f"""
        INSERT INTO {self._user_state_table} (app_name, user_id, state, update_time)
        VALUES (?, ?, {self._json_placeholder()}, ?)
        """
        state_json = self._serialize_state(state)
        event_data = self._serialize_json_field(event_record["event_data"])

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._execute(
                    cursor,
                    insert_sql,
                    (
                        event_record["id"],
                        event_record["session_id"],
                        event_record["invocation_id"],
                        event_record["timestamp"],
                        event_data,
                    ),
                )
                self._execute(cursor, update_sql, (state_json, app_name, user_id, session_id))
                if app_state is not None:
                    self._execute(cursor, delete_app_state_sql, (app_name,))
                    self._execute(
                        cursor,
                        insert_app_state_sql,
                        (app_name, self._serialize_state(app_state), datetime.now(timezone.utc)),
                    )
                if user_state is not None:
                    self._execute(cursor, delete_user_state_sql, (app_name, user_id))
                    self._execute(
                        cursor,
                        insert_user_state_sql,
                        (app_name, user_id, self._serialize_state(user_state), datetime.now(timezone.utc)),
                    )
                self._execute(cursor, select_sql, (app_name, user_id, session_id))
                row = cursor.fetchone()
                conn.commit()
            except Exception:
                with contextlib.suppress(Exception):
                    conn.rollback()
                raise
            finally:
                cursor.close()

        if row is None:
            msg = f"Session {session_id} not found during append_event_and_update_state."
            raise ValueError(msg)

        return SessionRecord(
            id=row[0],
            app_name=row[1],
            user_id=row[2],
            state=self._deserialize_state(row[3]),
            create_time=row[4],
            update_time=row[5],
        )

    def _get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "list[EventRecord]":
        """List events for a session ordered by timestamp.

        Args:
            app_name: Application name.
            user_id: User identifier.
            session_id: Session identifier.
            after_timestamp: Only return events after this time.
            limit: Maximum number of events to return.

        Returns:
            List of event records ordered by timestamp ASC.
        """
        if limit == 0:
            return []

        where_clauses = ["s.app_name = ?", "s.user_id = ?", "e.session_id = ?"]
        params: list[Any] = [app_name, user_id, session_id]

        if after_timestamp is not None:
            where_clauses.append("e.timestamp > ?")
            params.append(after_timestamp)

        where_clause = " AND ".join(where_clauses)
        limit_clause = f" LIMIT {limit}" if limit is not None else ""
        sql = f"""
        SELECT e.id, e.session_id, e.invocation_id, e.timestamp, e.event_data, s.app_name, s.user_id
        FROM {self._events_table} e
        JOIN {self._session_table} s ON e.session_id = s.id
        WHERE {where_clause}
        ORDER BY e.timestamp ASC{limit_clause}
        """

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    self._execute(cursor, sql, params)
                    rows = cursor.fetchall()

                    return [
                        EventRecord(
                            id=row[0],
                            session_id=row[1],
                            invocation_id=row[2] or "",
                            timestamp=row[3],
                            event_data=self._deserialize_json_field(row[4]) or {},
                            app_name=row[5],
                            user_id=row[6],
                        )
                        for row in rows
                    ]
                finally:
                    cursor.close()
        except Exception as e:
            error_msg = str(e).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return []
            raise

    def _delete_expired_events(self, before: datetime) -> int:
        count_sql = f"SELECT COUNT(*) FROM {self._events_table} WHERE timestamp < ?"
        delete_sql = f"DELETE FROM {self._events_table} WHERE timestamp < ?"

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    self._execute(cursor, count_sql, (before,))
                    row = cursor.fetchone()
                    count = int(row[0]) if row is not None else 0
                    self._execute(cursor, delete_sql, (before,))
                    conn.commit()
                    return count
                finally:
                    cursor.close()
        except Exception as exc:
            error_msg = str(exc).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return 0
            raise

    def _delete_idle_sessions(self, updated_before: datetime) -> int:
        count_sql = f"SELECT COUNT(*) FROM {self._session_table} WHERE update_time < ?"
        delete_events_sql = f"""
        DELETE FROM {self._events_table}
        WHERE session_id IN (SELECT id FROM {self._session_table} WHERE update_time < ?)
        """
        delete_sessions_sql = f"DELETE FROM {self._session_table} WHERE update_time < ?"

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    self._execute(cursor, count_sql, (updated_before,))
                    row = cursor.fetchone()
                    count = int(row[0]) if row is not None else 0
                    self._execute(cursor, delete_events_sql, (updated_before,))
                    self._execute(cursor, delete_sessions_sql, (updated_before,))
                    conn.commit()
                    return count
                finally:
                    cursor.close()
        except Exception as exc:
            error_msg = str(exc).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return 0
            raise

    def _get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        sql = f"SELECT state FROM {self._app_state_table} WHERE app_name = ?"
        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    self._execute(cursor, sql, (app_name,))
                    row = cursor.fetchone()
                    return self._deserialize_state(row[0]) if row is not None else None
                finally:
                    cursor.close()
        except Exception as exc:
            error_msg = str(exc).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return None
            raise

    def _get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        sql = f"SELECT state FROM {self._user_state_table} WHERE app_name = ? AND user_id = ?"
        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    self._execute(cursor, sql, (app_name, user_id))
                    row = cursor.fetchone()
                    return self._deserialize_state(row[0]) if row is not None else None
                finally:
                    cursor.close()
        except Exception as exc:
            error_msg = str(exc).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return None
            raise

    def _upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        delete_sql = f"DELETE FROM {self._app_state_table} WHERE app_name = ?"
        insert_sql = f"INSERT INTO {self._app_state_table} (app_name, state, update_time) VALUES (?, {self._json_placeholder()}, ?)"
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._execute(cursor, delete_sql, (app_name,))
                self._execute(cursor, insert_sql, (app_name, self._serialize_state(state), datetime.now(timezone.utc)))
                conn.commit()
            finally:
                cursor.close()

    def _upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        delete_sql = f"DELETE FROM {self._user_state_table} WHERE app_name = ? AND user_id = ?"
        insert_sql = f"""
        INSERT INTO {self._user_state_table} (app_name, user_id, state, update_time)
        VALUES (?, ?, {self._json_placeholder()}, ?)
        """
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._execute(cursor, delete_sql, (app_name, user_id))
                self._execute(
                    cursor, insert_sql, (app_name, user_id, self._serialize_state(state), datetime.now(timezone.utc))
                )
                conn.commit()
            finally:
                cursor.close()

    def _get_metadata(self, key: str) -> "str | None":
        sql = f"SELECT value FROM {self._metadata_table} WHERE key = ?"
        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    self._execute(cursor, sql, (key,))
                    row = cursor.fetchone()
                    return row[0] if row is not None else None
                finally:
                    cursor.close()
        except Exception as exc:
            error_msg = str(exc).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return None
            raise

    def _set_metadata(self, key: str, value: str) -> None:
        delete_sql = f"DELETE FROM {self._metadata_table} WHERE key = ?"
        insert_sql = f"INSERT INTO {self._metadata_table} (key, value) VALUES (?, ?)"
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._execute(cursor, delete_sql, (key,))
                self._execute(cursor, insert_sql, (key, value))
                conn.commit()
            finally:
                cursor.close()

    def _append_event(self, event_record: EventRecord) -> None:
        """Synchronous implementation of append_event."""
        self._insert_event(event_record)


class AdbcADKMemoryStore(BaseSyncADKMemoryStore["AdbcConfig"]):
    """ADBC synchronous ADK memory store for Arrow Database Connectivity."""

    __slots__ = ("_dialect",)

    def __init__(self, config: "AdbcConfig") -> None:
        super().__init__(config)
        self._dialect = self._detect_dialect()

    @property
    def dialect(self) -> str:
        return self._dialect

    def create_tables(self) -> None:
        """Create tables if they don't exist."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        self._create_tables()

    def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication."""
        return self._insert_memory_entries(entries, owner_id)

    def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query."""
        return self._search_entries(query, app_name, user_id, limit)

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        return self._delete_entries_by_session(session_id)

    def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        return self._delete_entries_older_than(days)

    def _detect_dialect(self) -> str:
        driver_name = self._config.connection_config.get("driver_name", "").lower()
        if "postgres" in driver_name:
            return DIALECT_POSTGRESQL
        if "sqlite" in driver_name:
            return DIALECT_SQLITE
        if "duckdb" in driver_name:
            return DIALECT_DUCKDB
        if "snowflake" in driver_name:
            return DIALECT_SNOWFLAKE
        logger.warning("Unknown ADBC driver: %s. Using generic SQL dialect.", driver_name)
        return DIALECT_GENERIC

    def _serialize_json_field(self, value: Any) -> "str | None":
        if value is None:
            return None
        return to_json(value)

    def _encode_timestamp(self, value: datetime) -> Any:
        if self._dialect == DIALECT_SQLITE:
            return value.timestamp()
        return value

    def _decode_timestamp(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        return datetime.fromisoformat(str(value))

    def _memory_table_ddl(self) -> str:
        if self._dialect == DIALECT_POSTGRESQL:
            return self._memory_ddl_postgresql()
        if self._dialect == DIALECT_SQLITE:
            return self._memory_ddl_sqlite()
        if self._dialect == DIALECT_DUCKDB:
            return self._memory_ddl_duckdb()
        if self._dialect == DIALECT_SNOWFLAKE:
            return self._memory_ddl_snowflake()
        return self._memory_ddl_generic()

    def _memory_ddl_postgresql(self) -> str:
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_ddl},
            timestamp TIMESTAMPTZ NOT NULL,
            content_json JSONB NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json JSONB,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """

    def _memory_ddl_sqlite(self) -> str:
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            app_name TEXT NOT NULL,
            user_id TEXT NOT NULL,
            event_id TEXT NOT NULL UNIQUE,
            author TEXT{owner_id_ddl},
            timestamp REAL NOT NULL,
            content_json TEXT NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json TEXT,
            inserted_at REAL NOT NULL
        )
        """

    def _memory_ddl_duckdb(self) -> str:
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_ddl},
            timestamp TIMESTAMP NOT NULL,
            content_json JSON NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json JSON,
            inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """

    def _memory_ddl_snowflake(self) -> str:
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR PRIMARY KEY,
            session_id VARCHAR NOT NULL,
            app_name VARCHAR NOT NULL,
            user_id VARCHAR NOT NULL,
            event_id VARCHAR NOT NULL UNIQUE,
            author VARCHAR{owner_id_ddl},
            timestamp TIMESTAMP_TZ NOT NULL,
            content_json VARIANT NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json VARIANT,
            inserted_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
        )
        """

    def _memory_ddl_generic(self) -> str:
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_ddl},
            timestamp TIMESTAMP NOT NULL,
            content_json TEXT NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json TEXT,
            inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """

    def _drop_memory_table_sql(self) -> "list[str]":
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    def _create_tables(self) -> None:
        if not self._enabled:
            return

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(self._memory_table_ddl())
                conn.commit()

                idx_app_user = f"CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_app_user_time ON {self._memory_table}(app_name, user_id, timestamp DESC)"
                cursor.execute(idx_app_user)
                conn.commit()

                idx_session = (
                    f"CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_session ON {self._memory_table}(session_id)"
                )
                cursor.execute(idx_session)
                conn.commit()
            finally:
                cursor.close()

    def _insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        inserted_count = 0
        use_returning = self._dialect in {DIALECT_SQLITE, DIALECT_POSTGRESQL, DIALECT_DUCKDB}

        if self._owner_id_column_name:
            if use_returning:
                sql = f"""
                INSERT INTO {self._memory_table} (
                    id, session_id, app_name, user_id, event_id, author,
                    {self._owner_id_column_name}, timestamp, content_json, content_text,
                    metadata_json, inserted_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                ) ON CONFLICT(event_id) DO NOTHING RETURNING 1
                """
            else:
                sql = f"""
                INSERT INTO {self._memory_table} (
                    id, session_id, app_name, user_id, event_id, author,
                    {self._owner_id_column_name}, timestamp, content_json, content_text,
                    metadata_json, inserted_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """
        elif use_returning:
            sql = f"""
                INSERT INTO {self._memory_table} (
                    id, session_id, app_name, user_id, event_id, author,
                    timestamp, content_json, content_text, metadata_json, inserted_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                ) ON CONFLICT(event_id) DO NOTHING RETURNING 1
                """
        else:
            sql = f"""
                INSERT INTO {self._memory_table} (
                    id, session_id, app_name, user_id, event_id, author,
                    timestamp, content_json, content_text, metadata_json, inserted_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                for entry in entries:
                    content_json = self._serialize_json_field(entry["content_json"])
                    metadata_json = self._serialize_json_field(entry["metadata_json"])
                    params: tuple[Any, ...]
                    if self._owner_id_column_name:
                        params = (
                            entry["id"],
                            entry["session_id"],
                            entry["app_name"],
                            entry["user_id"],
                            entry["event_id"],
                            entry["author"],
                            owner_id,
                            self._encode_timestamp(entry["timestamp"]),
                            content_json,
                            entry["content_text"],
                            metadata_json,
                            self._encode_timestamp(entry["inserted_at"]),
                        )
                    else:
                        params = (
                            entry["id"],
                            entry["session_id"],
                            entry["app_name"],
                            entry["user_id"],
                            entry["event_id"],
                            entry["author"],
                            self._encode_timestamp(entry["timestamp"]),
                            content_json,
                            entry["content_text"],
                            metadata_json,
                            self._encode_timestamp(entry["inserted_at"]),
                        )
                    if use_returning:
                        cursor.execute(sql, params)
                        if cursor.fetchone():
                            inserted_count += 1
                    else:
                        try:
                            cursor.execute(sql, params)
                            inserted_count += 1
                        except Exception as exc:
                            exc_str = str(exc).lower()
                            if "unique" in exc_str or "constraint" in exc_str or "duplicate" in exc_str:
                                continue
                            raise
                conn.commit()
            finally:
                cursor.close()

        return inserted_count

    def _search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if self._use_fts:
            logger.warning("ADBC memory store does not support FTS, falling back to simple search")

        effective_limit = limit if limit is not None else self._max_results
        pattern = f"%{query}%"

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

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, (app_name, user_id, pattern, effective_limit))
                    rows = cursor.fetchall()
                finally:
                    cursor.close()
        except Exception as exc:
            error_msg = str(exc).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return []
            raise

        return self._rows_to_records(rows)

    def _delete_entries_by_session(self, session_id: str) -> int:
        use_returning = self._dialect in {DIALECT_SQLITE, DIALECT_POSTGRESQL, DIALECT_DUCKDB}
        if use_returning:
            sql = f"DELETE FROM {self._memory_table} WHERE session_id = ? RETURNING 1"
        else:
            sql = f"DELETE FROM {self._memory_table} WHERE session_id = ?"
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (session_id,))
                if use_returning:
                    deleted_rows = cursor.fetchall()
                    conn.commit()
                    return len(deleted_rows)
                conn.commit()
                return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            finally:
                cursor.close()

    def _delete_entries_older_than(self, days: int) -> int:
        cutoff = self._encode_timestamp(datetime.now(timezone.utc) - timedelta(days=days))
        use_returning = self._dialect in {DIALECT_SQLITE, DIALECT_POSTGRESQL, DIALECT_DUCKDB}
        if use_returning:
            sql = f"DELETE FROM {self._memory_table} WHERE inserted_at < ? RETURNING 1"
        else:
            sql = f"DELETE FROM {self._memory_table} WHERE inserted_at < ?"
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (cutoff,))
                if use_returning:
                    deleted_rows = cursor.fetchall()
                    conn.commit()
                    return len(deleted_rows)
                conn.commit()
                return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            finally:
                cursor.close()

    def _rows_to_records(self, rows: "list[Any]") -> "list[MemoryRecord]":
        records: list[MemoryRecord] = []
        for row in rows:
            content_json = row[7]
            if isinstance(content_json, dict):
                content_value = content_json
            else:
                content_value = from_json(content_json if isinstance(content_json, (str, bytes)) else str(content_json))

            metadata_json = row[9]
            if metadata_json is None:
                metadata_value = None
            elif isinstance(metadata_json, dict):
                metadata_value = metadata_json
            else:
                metadata_value = from_json(
                    metadata_json if isinstance(metadata_json, (str, bytes)) else str(metadata_json)
                )

            records.append({
                "id": row[0],
                "session_id": row[1],
                "app_name": row[2],
                "user_id": row[3],
                "event_id": row[4],
                "author": row[5],
                "timestamp": self._decode_timestamp(row[6]),
                "content_json": content_value,
                "content_text": row[8],
                "metadata_json": metadata_value,
                "inserted_at": self._decode_timestamp(row[10]),
            })
        return records
