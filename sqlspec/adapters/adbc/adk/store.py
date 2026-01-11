"""ADBC ADK store for Google Agent Development Kit session/event storage."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Final

from sqlspec.extensions.adk import BaseSyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json

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


class AdbcADKStore(BaseSyncADKStore["AdbcConfig"]):
    """ADBC synchronous ADK store for Arrow Database Connectivity.

    Implements session and event storage for Google Agent Development Kit
    using ADBC. ADBC provides a vendor-neutral API with Arrow-native data
    transfer across multiple databases (PostgreSQL, SQLite, DuckDB, etc.).

    Provides:
    - Session state management with JSON serialization (TEXT storage)
    - Event history tracking with BLOB-serialized actions
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
        - TEXT for JSON storage (compatible across all ADBC backends)
        - BLOB for pre-serialized actions from Google ADK
        - TIMESTAMP for timezone-aware timestamps (driver-dependent precision)
        - INTEGER for booleans (0/1/NULL)
        - Parameter style varies by backend (?, $1, :name, etc.)
        - Uses dialect-agnostic SQL for maximum compatibility
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

    def _get_create_sessions_table_sql(self) -> str:
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

    def _get_create_events_table_sql(self) -> str:
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
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256),
            author VARCHAR(256),
            actions BYTEA,
            long_running_tool_ids_json TEXT,
            branch VARCHAR(256),
            timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            content JSONB,
            grounding_metadata JSONB,
            custom_metadata JSONB,
            partial BOOLEAN,
            turn_complete BOOLEAN,
            interrupted BOOLEAN,
            error_code VARCHAR(256),
            error_message VARCHAR(1024),
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        )
        """

    def _get_events_ddl_sqlite(self) -> str:
        """SQLite DDL for events table.

        Returns:
            SQL to create events table optimized for SQLite.
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            app_name TEXT NOT NULL,
            user_id TEXT NOT NULL,
            invocation_id TEXT,
            author TEXT,
            actions BLOB,
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
        )
        """

    def _get_events_ddl_duckdb(self) -> str:
        """DuckDB DDL for events table.

        Returns:
            SQL to create events table optimized for DuckDB.
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256),
            author VARCHAR(256),
            actions BLOB,
            long_running_tool_ids_json VARCHAR,
            branch VARCHAR(256),
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            content JSON,
            grounding_metadata JSON,
            custom_metadata JSON,
            partial BOOLEAN,
            turn_complete BOOLEAN,
            interrupted BOOLEAN,
            error_code VARCHAR(256),
            error_message VARCHAR(1024),
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        )
        """

    def _get_events_ddl_snowflake(self) -> str:
        """Snowflake DDL for events table.

        Returns:
            SQL to create events table optimized for Snowflake.
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR PRIMARY KEY,
            session_id VARCHAR NOT NULL,
            app_name VARCHAR NOT NULL,
            user_id VARCHAR NOT NULL,
            invocation_id VARCHAR,
            author VARCHAR,
            actions BINARY,
            long_running_tool_ids_json VARCHAR,
            branch VARCHAR,
            timestamp TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            content VARIANT,
            grounding_metadata VARIANT,
            custom_metadata VARIANT,
            partial BOOLEAN,
            turn_complete BOOLEAN,
            interrupted BOOLEAN,
            error_code VARCHAR,
            error_message VARCHAR,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id)
        )
        """

    def _get_events_ddl_generic(self) -> str:
        """Generic SQL-92 compatible DDL for events table.

        Returns:
            SQL to create events table using generic types.
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256),
            author VARCHAR(256),
            actions BLOB,
            long_running_tool_ids_json TEXT,
            branch VARCHAR(256),
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            content TEXT,
            grounding_metadata TEXT,
            custom_metadata TEXT,
            partial INTEGER,
            turn_complete INTEGER,
            interrupted INTEGER,
            error_code VARCHAR(256),
            error_message VARCHAR(1024),
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

    def create_tables(self) -> None:
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

    def create_session(
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

    def get_session(self, session_id: str) -> "SessionRecord | None":
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

    def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
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

    def delete_session(self, session_id: str) -> None:
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

    def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
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
            content: Event content (JSON).
            **kwargs: Additional optional fields.

        Returns:
            Created event record.

        Notes:
            Uses CURRENT_TIMESTAMP for timestamp if not provided.
            JSON fields are serialized to JSON strings.
            Boolean fields are converted to INTEGER (0/1).
        """
        content_json = self._serialize_json_field(content)
        grounding_metadata_json = self._serialize_json_field(kwargs.get("grounding_metadata"))
        custom_metadata_json = self._serialize_json_field(kwargs.get("custom_metadata"))

        partial_int = self._to_int_bool(kwargs.get("partial"))
        turn_complete_int = self._to_int_bool(kwargs.get("turn_complete"))
        interrupted_int = self._to_int_bool(kwargs.get("interrupted"))

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

        timestamp = kwargs.get("timestamp")
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    sql,
                    (
                        event_id,
                        session_id,
                        app_name,
                        user_id,
                        kwargs.get("invocation_id"),
                        author,
                        actions,
                        kwargs.get("long_running_tool_ids_json"),
                        kwargs.get("branch"),
                        timestamp,
                        content_json,
                        grounding_metadata_json,
                        custom_metadata_json,
                        partial_int,
                        turn_complete_int,
                        interrupted_int,
                        kwargs.get("error_code"),
                        kwargs.get("error_message"),
                    ),
                )
                conn.commit()
            finally:
                cursor.close()  # type: ignore[no-untyped-call]

        events = self.list_events(session_id)
        for event in events:
            if event["id"] == event_id:
                return event

        msg = f"Failed to retrieve created event {event_id}"
        raise RuntimeError(msg)

    def list_events(self, session_id: str) -> "list[EventRecord]":
        """List events for a session ordered by timestamp.

        Args:
            session_id: Session identifier.

        Returns:
            List of event records ordered by timestamp ASC.

        Notes:
            Uses index on (session_id, timestamp ASC).
            JSON fields deserialized from JSON strings.
            Converts INTEGER booleans to Python bool.
        """
        sql = f"""
        SELECT id, session_id, app_name, user_id, invocation_id, author, actions,
               long_running_tool_ids_json, branch, timestamp, content,
               grounding_metadata, custom_metadata, partial, turn_complete,
               interrupted, error_code, error_message
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
                            id=row[0],
                            session_id=row[1],
                            app_name=row[2],
                            user_id=row[3],
                            invocation_id=row[4],
                            author=row[5],
                            actions=bytes(row[6]) if row[6] is not None else b"",
                            long_running_tool_ids_json=row[7],
                            branch=row[8],
                            timestamp=row[9],
                            content=self._deserialize_json_field(row[10]),
                            grounding_metadata=self._deserialize_json_field(row[11]),
                            custom_metadata=self._deserialize_json_field(row[12]),
                            partial=self._from_int_bool(row[13]),
                            turn_complete=self._from_int_bool(row[14]),
                            interrupted=self._from_int_bool(row[15]),
                            error_code=row[16],
                            error_message=row[17],
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

    @staticmethod
    def _to_int_bool(value: "bool | None") -> "int | None":
        """Convert Python boolean to INTEGER (0/1).

        Args:
            value: Python boolean value or None.

        Returns:
            1 for True, 0 for False, None for None.
        """
        if value is None:
            return None
        return 1 if value else 0

    @staticmethod
    def _from_int_bool(value: "int | None") -> "bool | None":
        """Convert INTEGER to Python boolean.

        Args:
            value: INTEGER value (0, 1, or None).

        Returns:
            Python boolean or None.
        """
        if value is None:
            return None
        return bool(value)


class AdbcADKMemoryStore(BaseSyncADKMemoryStore["AdbcConfig"]):
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

    def _get_create_memory_table_sql(self) -> str:
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

    def create_tables(self) -> None:
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

    def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
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

    def search_entries(
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

    def delete_entries_by_session(self, session_id: str) -> int:
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

    def delete_entries_older_than(self, days: int) -> int:
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
