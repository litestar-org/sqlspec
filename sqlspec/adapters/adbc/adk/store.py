"""ADBC ADK store for Google Agent Development Kit session/event storage."""

import contextlib
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Final

from sqlspec.extensions.adk import BaseAsyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.adapters.adbc.config import AdbcConfig
    from sqlspec.extensions.adk import MemoryRecord

logger = get_logger("sqlspec.adapters.adbc.adk.store")

__all__ = ("AdbcADKMemoryStore", "AdbcADKStore")

DIALECT_POSTGRESQL: Final = "postgresql"
DIALECT_SQLITE: Final = "sqlite"
DIALECT_DUCKDB: Final = "duckdb"
DIALECT_SNOWFLAKE: Final = "snowflake"
DIALECT_GENERIC: Final = "generic"

ADBC_TABLE_NOT_FOUND_PATTERNS: Final = ("no such table", "table or view does not exist", "relation does not exist")


class AdbcADKStore(BaseAsyncADKStore["AdbcConfig"]):
    """ADBC synchronous ADK store for Arrow Database Connectivity.

    Implements session and event storage for Google Agent Development Kit
    using ADBC. ADBC provides a vendor-neutral API with Arrow-native data
    transfer across multiple databases (PostgreSQL, SQLite, DuckDB, etc.).

    Events use the new 5-column contract: session_id, invocation_id, author,
    timestamp, and event_json.  The full ADK Event payload is stored as a
    single JSON blob in event_json using a dialect-appropriate column type
    (JSONB for PostgreSQL, JSON for DuckDB, VARIANT for Snowflake, TEXT for
    SQLite and generic fallback).

    Provides:
    - Session state management with JSON serialization
    - Event history tracking via single event_json blob
    - Atomic event insert + session state update
    - Timezone-aware timestamps
    - Foreign key constraints with cascade delete
    - Database-agnostic SQL (supports multiple backends)

    Args:
        config: AdbcConfig with extension_config["adk"] settings.

    Example:
        from sqlspec.adapters.adbc import AdbcConfig
        from sqlspec.adapters.adbc.adk import AdbcADKStore

        config = AdbcConfig(
            connection_config={"driver_name": "sqlite", "uri": ":memory:"},
            extension_config={
                "adk": {
                    "session_table": "my_sessions",
                    "events_table": "my_events",
                    "owner_id_column": "tenant_id INTEGER REFERENCES tenants(id)"
                }
            }
        )
        store = AdbcADKStore(config)
        store.ensure_tables()

    Notes:
        - Dialect-appropriate JSON type for event_json storage
        - TIMESTAMP for timezone-aware timestamps (driver-dependent precision)
        - Parameter style: ``?`` universally across ADBC backends
        - State and JSON fields use to_json/from_json for serialization
        - ADBC drivers handle parameter binding automatically
        - Configuration is read from config.extension_config["adk"]
    """

    __slots__ = ("_dialect",)

    def __init__(self, config: "AdbcConfig") -> None:
        """Initialize ADBC ADK store.

        Args:
            config: AdbcConfig instance (any ADBC driver).

        Notes:
            Configuration is read from config.extension_config["adk"]:
            - session_table: Sessions table name (default: "adk_sessions")
            - events_table: Events table name (default: "adk_events")
            - owner_id_column: Optional owner FK column DDL (default: None)
        """
        super().__init__(config)
        self._dialect = self._detect_dialect()

    @property
    def dialect(self) -> str:
        """Return the detected database dialect."""
        return self._dialect

    def _detect_dialect(self) -> str:
        """Detect ADBC driver dialect from connection config.

        Returns:
            Dialect identifier for DDL generation.

        Notes:
            Reads from config.connection_config driver_name.
            Falls back to generic for unknown drivers.
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

    async def _get_create_sessions_table_sql(self) -> str:
        """Get CREATE TABLE SQL for sessions with dialect dispatch.

        Returns:
            SQL statement to create adk_sessions table.
        """
        if self._dialect == DIALECT_POSTGRESQL:
            return self._get_sessions_ddl_postgresql()
        if self._dialect == DIALECT_SQLITE:
            return self._get_sessions_ddl_sqlite()
        if self._dialect == DIALECT_DUCKDB:
            return self._get_sessions_ddl_duckdb()
        if self._dialect == DIALECT_SNOWFLAKE:
            return self._get_sessions_ddl_snowflake()
        return self._get_sessions_ddl_generic()

    def _get_sessions_ddl_postgresql(self) -> str:
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

    def _get_sessions_ddl_sqlite(self) -> str:
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

    def _get_sessions_ddl_duckdb(self) -> str:
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

    def _get_sessions_ddl_snowflake(self) -> str:
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

    def _get_sessions_ddl_generic(self) -> str:
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

    async def _get_create_events_table_sql(self) -> str:
        """Get CREATE TABLE SQL for events with dialect dispatch.

        Returns:
            SQL statement to create adk_events table.
        """
        if self._dialect == DIALECT_POSTGRESQL:
            return self._get_events_ddl_postgresql()
        if self._dialect == DIALECT_SQLITE:
            return self._get_events_ddl_sqlite()
        if self._dialect == DIALECT_DUCKDB:
            return self._get_events_ddl_duckdb()
        if self._dialect == DIALECT_SNOWFLAKE:
            return self._get_events_ddl_snowflake()
        return self._get_events_ddl_generic()

    def _get_events_ddl_postgresql(self) -> str:
        """PostgreSQL DDL for events table.

        Returns:
            SQL to create events table optimized for PostgreSQL.

        Notes:
            Uses JSONB for event_json to enable indexing and query support.
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            session_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256) NOT NULL,
            author VARCHAR(256) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_json JSONB NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        )
        """

    def _get_events_ddl_sqlite(self) -> str:
        """SQLite DDL for events table.

        Returns:
            SQL to create events table optimized for SQLite.

        Notes:
            Uses TEXT for event_json (SQLite has no native JSON column type).
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            session_id TEXT NOT NULL,
            invocation_id TEXT NOT NULL,
            author TEXT NOT NULL,
            timestamp REAL NOT NULL,
            event_json TEXT NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        )
        """

    def _get_events_ddl_duckdb(self) -> str:
        """DuckDB DDL for events table.

        Returns:
            SQL to create events table optimized for DuckDB.

        Notes:
            Uses JSON for event_json (DuckDB native JSON type).
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            session_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256) NOT NULL,
            author VARCHAR(256) NOT NULL,
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_json JSON NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        )
        """

    def _get_events_ddl_snowflake(self) -> str:
        """Snowflake DDL for events table.

        Returns:
            SQL to create events table optimized for Snowflake.

        Notes:
            Uses VARIANT for event_json (Snowflake semi-structured type).
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            session_id VARCHAR NOT NULL,
            invocation_id VARCHAR NOT NULL,
            author VARCHAR NOT NULL,
            timestamp TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            event_json VARIANT NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id)
        )
        """

    def _get_events_ddl_generic(self) -> str:
        """Generic SQL-92 compatible DDL for events table.

        Returns:
            SQL to create events table using generic types.

        Notes:
            Uses TEXT for event_json (maximum portability).
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            session_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256) NOT NULL,
            author VARCHAR(256) NOT NULL,
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_json TEXT NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        )
        """

    def _get_drop_tables_sql(self) -> "list[str]":
        """Get DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop tables and indexes.

        Notes:
            Order matters: drop events table (child) before sessions (parent).
            Most databases automatically drop indexes when dropping tables.
        """
        return [f"DROP TABLE IF EXISTS {self._events_table}", f"DROP TABLE IF EXISTS {self._session_table}"]

    def _create_tables(self) -> None:
        """Create both sessions and events tables if they don't exist."""
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._enable_foreign_keys(cursor, conn)

                cursor.execute(self._get_create_sessions_table_sql())
                conn.commit()

                sessions_idx_app_user = (
                    f"CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user "
                    f"ON {self._session_table}(app_name, user_id)"
                )
                cursor.execute(sessions_idx_app_user)
                conn.commit()

                sessions_idx_update = (
                    f"CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time "
                    f"ON {self._session_table}(update_time DESC)"
                )
                cursor.execute(sessions_idx_update)
                conn.commit()

                cursor.execute(self._get_create_events_table_sql())
                conn.commit()

                events_idx = (
                    f"CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session "
                    f"ON {self._events_table}(session_id, timestamp ASC)"
                )
                cursor.execute(events_idx)
                conn.commit()
            finally:
                cursor.close()  # type: ignore[no-untyped-call]


    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        await async_(self._create_tables)()

    def _enable_foreign_keys(self, cursor: Any, conn: Any) -> None:
        """Enable foreign key constraints for SQLite.

        Args:
            cursor: Database cursor.
            conn: Database connection.

        Notes:
            SQLite requires PRAGMA foreign_keys = ON to be set per connection.
            This is a no-op for other databases.
        """
        try:
            cursor.execute("PRAGMA foreign_keys = ON")
            conn.commit()
        except Exception:
            logger.debug("Foreign key enforcement not supported or already enabled")

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

        params: tuple[Any, ...]
        if self._owner_id_column_name:
            sql = f"""
            INSERT INTO {self._session_table}
            (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
            params = (session_id, app_name, user_id, owner_id, state_json)
        else:
            sql = f"""
            INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
            params = (session_id, app_name, user_id, state_json)

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params)
                conn.commit()
            finally:
                cursor.close()  # type: ignore[no-untyped-call]

        return self.get_session(session_id)  # type: ignore[return-value]


    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        """Create a new session."""
        return await async_(self._create_session)(session_id, app_name, user_id, state, owner_id)

    def _get_session(self, session_id: str) -> "SessionRecord | None":
        """Get session by ID.

        Args:
            session_id: Session identifier.

        Returns:
            Session record or None if not found.

        Notes:
            State is deserialized from JSON string.
        """
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = ?
        """

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, (session_id,))
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
                    cursor.close()  # type: ignore[no-untyped-call]
        except Exception as e:
            error_msg = str(e).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return None
            raise


    async def get_session(self, session_id: str) -> "SessionRecord | None":
        """Get session by ID."""
        return await async_(self._get_session)(session_id)

    def _update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state.

        Args:
            session_id: Session identifier.
            state: New state dictionary (replaces existing state).

        Notes:
            This replaces the entire state dictionary.
            Updates update_time to current timestamp.
        """
        state_json = self._serialize_state(state)
        sql = f"""
        UPDATE {self._session_table}
        SET state = ?, update_time = CURRENT_TIMESTAMP
        WHERE id = ?
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (state_json, session_id))
                conn.commit()
            finally:
                cursor.close()  # type: ignore[no-untyped-call]


    async def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state."""
        await async_(self._update_session_state)(session_id, state)

    def _delete_session(self, session_id: str) -> None:
        """Delete session and all associated events (cascade).

        Args:
            session_id: Session identifier.

        Notes:
            Foreign key constraint ensures events are cascade-deleted.
        """
        sql = f"DELETE FROM {self._session_table} WHERE id = ?"

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._enable_foreign_keys(cursor, conn)
                cursor.execute(sql, (session_id,))
                conn.commit()
            finally:
                cursor.close()  # type: ignore[no-untyped-call]


    async def delete_session(self, session_id: str) -> None:
        """Delete session and associated events."""
        await async_(self._delete_session)(session_id)

    def _list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
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
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, params)
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
                    cursor.close()  # type: ignore[no-untyped-call]
        except Exception as e:
            error_msg = str(e).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return []
            raise


    async def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        """List sessions for an app."""
        return await async_(self._list_sessions)(app_name, user_id)

    def _insert_event(self, event_record: "EventRecord") -> None:
        """Insert an event record into the events table.

        Args:
            event_record: Event record to store.
        """
        sql = f"""
        INSERT INTO {self._events_table} (
            session_id, invocation_id, author, timestamp, event_json
        ) VALUES (?, ?, ?, ?, ?)
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    sql,
                    (
                        event_record["session_id"],
                        event_record["invocation_id"],
                        event_record["author"],
                        event_record["timestamp"],
                        event_record["event_json"],
                    ),
                )
                conn.commit()
            finally:
                cursor.close()  # type: ignore[no-untyped-call]

    def _append_event_and_update_state(
        self, event_record: "EventRecord", session_id: str, state: "dict[str, Any]"
    ) -> None:
        """Atomically insert an event and update the session's durable state.

        The event insert and state update are executed within a single
        connection and committed together.  If either statement fails the
        transaction is rolled back so the two writes remain consistent.

        Args:
            event_record: Event record to store.
            session_id: Session identifier whose state should be updated.
            state: Post-append durable state snapshot (``temp:`` keys already
                stripped by the service layer).
        """
        insert_sql = f"""
        INSERT INTO {self._events_table} (
            session_id, invocation_id, author, timestamp, event_json
        ) VALUES (?, ?, ?, ?, ?)
        """
        update_sql = f"""
        UPDATE {self._session_table}
        SET state = ?, update_time = CURRENT_TIMESTAMP
        WHERE id = ?
        """
        state_json = self._serialize_state(state)

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    insert_sql,
                    (
                        event_record["session_id"],
                        event_record["invocation_id"],
                        event_record["author"],
                        event_record["timestamp"],
                        event_record["event_json"],
                    ),
                )
                cursor.execute(update_sql, (state_json, session_id))
                conn.commit()
            except Exception:
                with contextlib.suppress(Exception):
                    conn.rollback()
                raise
            finally:
                cursor.close()  # type: ignore[no-untyped-call]


    async def append_event_and_update_state(
        self, event_record: EventRecord, session_id: str, state: "dict[str, Any]"
    ) -> None:
        """Atomically append an event and update the session's durable state."""
        await async_(self._append_event_and_update_state)(event_record, session_id, state)

    def _get_events(
        self, session_id: str, after_timestamp: "datetime | None" = None, limit: "int | None" = None
    ) -> "list[EventRecord]":
        """List events for a session ordered by timestamp.

        Args:
            session_id: Session identifier.

        Returns:
            List of event records ordered by timestamp ASC.

        Notes:
            Uses index on (session_id, timestamp ASC).
            Returns the 5-column EventRecord (session_id, invocation_id,
            author, timestamp, event_json).
        """
        sql = f"""
        SELECT session_id, invocation_id, author, timestamp, event_json
        FROM {self._events_table}
        WHERE session_id = ?
        ORDER BY timestamp ASC
        """

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, (session_id,))
                    rows = cursor.fetchall()

                    return [
                        EventRecord(
                            session_id=row[0],
                            invocation_id=row[1],
                            author=row[2],
                            timestamp=row[3],
                            event_json=str(row[4]) if row[4] is not None else "{}",
                        )
                        for row in rows
                    ]
                finally:
                    cursor.close()  # type: ignore[no-untyped-call]
        except Exception as e:
            error_msg = str(e).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return []
            raise



    async def get_events(
        self, session_id: str, after_timestamp: "datetime | None" = None, limit: "int | None" = None
    ) -> "list[EventRecord]":
        """Get events for a session."""
        return await async_(self._get_events)(session_id, after_timestamp, limit)

    def _append_event(self, event_record: EventRecord) -> None:
        """Synchronous implementation of append_event."""
        self._append_event_and_update_state(event_record, event_record["session_id"], {})

    async def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session."""
        await async_(self._append_event)(event_record)

class AdbcADKMemoryStore(BaseAsyncADKMemoryStore["AdbcConfig"]):
    """ADBC synchronous ADK memory store for Arrow Database Connectivity."""

    __slots__ = ("_dialect",)

    def __init__(self, config: "AdbcConfig") -> None:
        super().__init__(config)
        self._dialect = self._detect_dialect()

    @property
    def dialect(self) -> str:
        return self._dialect

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

    async def _get_create_memory_table_sql(self) -> str:
        if self._dialect == DIALECT_POSTGRESQL:
            return self._get_memory_ddl_postgresql()
        if self._dialect == DIALECT_SQLITE:
            return self._get_memory_ddl_sqlite()
        if self._dialect == DIALECT_DUCKDB:
            return self._get_memory_ddl_duckdb()
        if self._dialect == DIALECT_SNOWFLAKE:
            return self._get_memory_ddl_snowflake()
        return self._get_memory_ddl_generic()

    def _get_memory_ddl_postgresql(self) -> str:
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

    def _get_memory_ddl_sqlite(self) -> str:
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

    def _get_memory_ddl_duckdb(self) -> str:
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

    def _get_memory_ddl_snowflake(self) -> str:
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

    def _get_memory_ddl_generic(self) -> str:
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

    def _get_drop_memory_table_sql(self) -> "list[str]":
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    def _create_tables(self) -> None:
        if not self._enabled:
            return

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(self._get_create_memory_table_sql())
                conn.commit()

                idx_app_user = (
                    f"CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_app_user_time "
                    f"ON {self._memory_table}(app_name, user_id, timestamp DESC)"
                )
                cursor.execute(idx_app_user)
                conn.commit()

                idx_session = (
                    f"CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_session ON {self._memory_table}(session_id)"
                )
                cursor.execute(idx_session)
                conn.commit()
            finally:
                cursor.close()  # type: ignore[no-untyped-call]


    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        await async_(self._create_tables)()

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
                cursor.close()  # type: ignore[no-untyped-call]

        return inserted_count


    async def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication."""
        return await async_(self._insert_memory_entries)(entries, owner_id)

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
                    cursor.close()  # type: ignore[no-untyped-call]
        except Exception as exc:
            error_msg = str(exc).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return []
            raise

        return self._rows_to_records(rows)


    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query."""
        return await async_(self._search_entries)(query, app_name, user_id, limit)

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
                cursor.close()  # type: ignore[no-untyped-call]


    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        return await async_(self._delete_entries_by_session)(session_id)

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
                cursor.close()  # type: ignore[no-untyped-call]


    async def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        return await async_(self._delete_entries_older_than)(days)

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
