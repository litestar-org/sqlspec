"""DuckDB ADK store for Google Agent Development Kit - DEV/TEST ONLY.

WARNING: DuckDB is an OLAP database optimized for analytical queries,
not OLTP workloads. This adapter is suitable for:
- Local development and testing
- Analytical workloads on session data
- Prototyping

NOT recommended for:
- Production session storage
- High-concurrency write workloads
- Real-time session management
"""

import json
from typing import TYPE_CHECKING, Any, Final

from sqlspec.extensions.adk._types import EventRecord, SessionRecord
from sqlspec.extensions.adk.store import BaseSyncADKStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.duckdb.config import DuckDBConfig

logger = get_logger("adapters.duckdb.adk.store")

__all__ = ("DuckdbADKStore",)

DUCKDB_TABLE_NOT_FOUND_ERROR: Final = "does not exist"


class DuckdbADKStore(BaseSyncADKStore["DuckDBConfig"]):
    """DuckDB ADK store - DEV/TEST ONLY.

    WARNING: DuckDB is an OLAP database optimized for analytical queries,
    not OLTP workloads. This adapter is suitable for:
    - Local development and testing
    - Analytical workloads on session data
    - Prototyping

    NOT recommended for:
    - Production session storage
    - High-concurrency write workloads
    - Real-time session management

    Implements session and event storage for Google Agent Development Kit
    using DuckDB via the synchronous driver. Uses async_() wrapper to
    provide async interface. Provides:
    - Session state management with native JSON type
    - Event history tracking with BLOB-serialized actions
    - Native TIMESTAMP type support
    - Foreign key constraints with cascade delete
    - Columnar storage for analytical queries

    Args:
        config: DuckDBConfig instance.
        session_table: Name of the sessions table. Defaults to "adk_sessions".
        events_table: Name of the events table. Defaults to "adk_events".

    Example:
        from sqlspec.adapters.duckdb import DuckDBConfig
        from sqlspec.adapters.duckdb.adk import DuckdbADKStore

        config = DuckDBConfig()
        store = DuckdbADKStore(config)
        store.create_tables()

    Notes:
        - DuckDB JSON type (not JSONB)
        - TIMESTAMP provides date/time storage
        - BLOB for binary actions data
        - BOOLEAN native type support
        - Columnar storage optimized for analytics
        - Limited write concurrency
    """

    __slots__ = ()

    def __init__(
        self, config: "DuckDBConfig", session_table: str = "adk_sessions", events_table: str = "adk_events"
    ) -> None:
        """Initialize DuckDB ADK store.

        Args:
            config: DuckDBConfig instance.
            session_table: Name of the sessions table.
            events_table: Name of the events table.
        """
        super().__init__(config, session_table, events_table)

    def _get_create_sessions_table_sql(self) -> str:
        """Get DuckDB CREATE TABLE SQL for sessions.

        Returns:
            SQL statement to create adk_sessions table with indexes.

        Notes:
            - VARCHAR for IDs and names
            - JSON type for state storage (DuckDB native)
            - TIMESTAMP for create_time and update_time
            - CURRENT_TIMESTAMP for defaults
            - Composite index on (app_name, user_id) for listing
            - Index on update_time DESC for recent session queries
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id VARCHAR PRIMARY KEY,
            app_name VARCHAR NOT NULL,
            user_id VARCHAR NOT NULL,
            state JSON NOT NULL,
            create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user ON {self._session_table}(app_name, user_id);
        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time ON {self._session_table}(update_time DESC);
        """

    def _get_create_events_table_sql(self) -> str:
        """Get DuckDB CREATE TABLE SQL for events.

        Returns:
            SQL statement to create adk_events table with indexes.

        Notes:
            - VARCHAR for string fields
            - BLOB for pickled actions
            - JSON for content, grounding_metadata, custom_metadata
            - BOOLEAN for flags
            - Foreign key constraint (DuckDB doesn't support CASCADE)
            - Index on (session_id, timestamp ASC) for ordered event retrieval
            - Manual cascade delete required in delete_session method
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR PRIMARY KEY,
            session_id VARCHAR NOT NULL,
            app_name VARCHAR NOT NULL,
            user_id VARCHAR NOT NULL,
            invocation_id VARCHAR,
            author VARCHAR,
            actions BLOB,
            long_running_tool_ids_json VARCHAR,
            branch VARCHAR,
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            content JSON,
            grounding_metadata JSON,
            custom_metadata JSON,
            partial BOOLEAN,
            turn_complete BOOLEAN,
            interrupted BOOLEAN,
            error_code VARCHAR,
            error_message VARCHAR,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id)
        );
        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session ON {self._events_table}(session_id, timestamp ASC);
        """

    def _get_drop_tables_sql(self) -> "list[str]":
        """Get DuckDB DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop tables and indexes.

        Notes:
            Order matters: drop events table (child) before sessions (parent).
            DuckDB automatically drops indexes when dropping tables.
        """
        return [f"DROP TABLE IF EXISTS {self._events_table}", f"DROP TABLE IF EXISTS {self._session_table}"]

    def create_tables(self) -> None:
        """Create both sessions and events tables if they don't exist."""
        with self._config.provide_connection() as conn:
            conn.execute(self._get_create_sessions_table_sql())
            conn.execute(self._get_create_events_table_sql())
        logger.debug("Created ADK tables: %s, %s", self._session_table, self._events_table)

    def create_session(self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]") -> SessionRecord:
        """Create a new session.

        Args:
            session_id: Unique session identifier.
            app_name: Application name.
            user_id: User identifier.
            state: Initial session state.

        Returns:
            Created session record.

        Notes:
            Uses CURRENT_TIMESTAMP for create_time and update_time.
            State is JSON-serialized before insertion.
        """
        state_json = json.dumps(state)
        sql = f"""
        INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """

        with self._config.provide_connection() as conn:
            conn.execute(sql, (session_id, app_name, user_id, state_json))
            conn.commit()

        return self.get_session(session_id)  # type: ignore[return-value]

    def get_session(self, session_id: str) -> "SessionRecord | None":
        """Get session by ID.

        Args:
            session_id: Session identifier.

        Returns:
            Session record or None if not found.

        Notes:
            DuckDB returns datetime objects for TIMESTAMP columns.
            JSON is parsed from database storage.
        """
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = ?
        """

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.execute(sql, (session_id,))
                row = cursor.fetchone()

                if row is None:
                    return None

                session_id_val, app_name, user_id, state_data, create_time, update_time = row

                state = json.loads(state_data) if isinstance(state_data, str) else state_data

                return SessionRecord(
                    id=session_id_val,
                    app_name=app_name,
                    user_id=user_id,
                    state=state,
                    create_time=create_time,
                    update_time=update_time,
                )
        except Exception as e:
            if DUCKDB_TABLE_NOT_FOUND_ERROR in str(e):
                return None
            raise

    def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state.

        Args:
            session_id: Session identifier.
            state: New state dictionary (replaces existing state).

        Notes:
            This replaces the entire state dictionary.
            Update time is automatically updated.
        """
        state_json = json.dumps(state)

        sql = f"""
        UPDATE {self._session_table}
        SET state = ?, update_time = CURRENT_TIMESTAMP
        WHERE id = ?
        """

        with self._config.provide_connection() as conn:
            conn.execute(sql, (state_json, session_id))
            conn.commit()

    def delete_session(self, session_id: str) -> None:
        """Delete session and all associated events.

        Args:
            session_id: Session identifier.

        Notes:
            DuckDB doesn't support CASCADE in foreign keys, so we manually delete events first.
        """
        delete_events_sql = f"DELETE FROM {self._events_table} WHERE session_id = ?"
        delete_session_sql = f"DELETE FROM {self._session_table} WHERE id = ?"

        with self._config.provide_connection() as conn:
            conn.execute(delete_events_sql, (session_id,))
            conn.execute(delete_session_sql, (session_id,))
            conn.commit()

    def list_sessions(self, app_name: str, user_id: str) -> "list[SessionRecord]":
        """List all sessions for a user in an app.

        Args:
            app_name: Application name.
            user_id: User identifier.

        Returns:
            List of session records ordered by update_time DESC.

        Notes:
            Uses composite index on (app_name, user_id).
        """
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE app_name = ? AND user_id = ?
        ORDER BY update_time DESC
        """

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.execute(sql, (app_name, user_id))
                rows = cursor.fetchall()

                return [
                    SessionRecord(
                        id=row[0],
                        app_name=row[1],
                        user_id=row[2],
                        state=json.loads(row[3]) if isinstance(row[3], str) else row[3],
                        create_time=row[4],
                        update_time=row[5],
                    )
                    for row in rows
                ]
        except Exception as e:
            if DUCKDB_TABLE_NOT_FOUND_ERROR in str(e):
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
    ) -> EventRecord:
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
        """
        content_json = json.dumps(content) if content else None
        grounding_metadata = kwargs.get("grounding_metadata")
        grounding_metadata_json = json.dumps(grounding_metadata) if grounding_metadata else None
        custom_metadata = kwargs.get("custom_metadata")
        custom_metadata_json = json.dumps(custom_metadata) if custom_metadata else None

        sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, app_name, user_id, invocation_id, author, actions,
            long_running_tool_ids_json, branch, timestamp, content,
            grounding_metadata, custom_metadata, partial, turn_complete,
            interrupted, error_code, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        with self._config.provide_connection() as conn:
            conn.execute(
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
                    kwargs.get("timestamp") if kwargs.get("timestamp") else None,
                    content_json,
                    grounding_metadata_json,
                    custom_metadata_json,
                    kwargs.get("partial"),
                    kwargs.get("turn_complete"),
                    kwargs.get("interrupted"),
                    kwargs.get("error_code"),
                    kwargs.get("error_message"),
                ),
            )
            conn.commit()

        return self.get_event(event_id)  # type: ignore[return-value]

    def get_event(self, event_id: str) -> "EventRecord | None":
        """Get event by ID.

        Args:
            event_id: Event identifier.

        Returns:
            Event record or None if not found.
        """
        sql = f"""
        SELECT id, session_id, app_name, user_id, invocation_id, author, actions,
               long_running_tool_ids_json, branch, timestamp, content,
               grounding_metadata, custom_metadata, partial, turn_complete,
               interrupted, error_code, error_message
        FROM {self._events_table}
        WHERE id = ?
        """

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.execute(sql, (event_id,))
                row = cursor.fetchone()

                if row is None:
                    return None

                return EventRecord(
                    id=row[0],
                    session_id=row[1],
                    app_name=row[2],
                    user_id=row[3],
                    invocation_id=row[4],
                    author=row[5],
                    actions=bytes(row[6]) if row[6] else b"",
                    long_running_tool_ids_json=row[7],
                    branch=row[8],
                    timestamp=row[9],
                    content=json.loads(row[10]) if row[10] and isinstance(row[10], str) else row[10],
                    grounding_metadata=json.loads(row[11]) if row[11] and isinstance(row[11], str) else row[11],
                    custom_metadata=json.loads(row[12]) if row[12] and isinstance(row[12], str) else row[12],
                    partial=row[13],
                    turn_complete=row[14],
                    interrupted=row[15],
                    error_code=row[16],
                    error_message=row[17],
                )
        except Exception as e:
            if DUCKDB_TABLE_NOT_FOUND_ERROR in str(e):
                return None
            raise

    def list_events(self, session_id: str) -> "list[EventRecord]":
        """List events for a session ordered by timestamp.

        Args:
            session_id: Session identifier.

        Returns:
            List of event records ordered by timestamp ASC.
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
                cursor = conn.execute(sql, (session_id,))
                rows = cursor.fetchall()

                return [
                    EventRecord(
                        id=row[0],
                        session_id=row[1],
                        app_name=row[2],
                        user_id=row[3],
                        invocation_id=row[4],
                        author=row[5],
                        actions=bytes(row[6]) if row[6] else b"",
                        long_running_tool_ids_json=row[7],
                        branch=row[8],
                        timestamp=row[9],
                        content=json.loads(row[10]) if row[10] and isinstance(row[10], str) else row[10],
                        grounding_metadata=json.loads(row[11]) if row[11] and isinstance(row[11], str) else row[11],
                        custom_metadata=json.loads(row[12]) if row[12] and isinstance(row[12], str) else row[12],
                        partial=row[13],
                        turn_complete=row[14],
                        interrupted=row[15],
                        error_code=row[16],
                        error_message=row[17],
                    )
                    for row in rows
                ]
        except Exception as e:
            if DUCKDB_TABLE_NOT_FOUND_ERROR in str(e):
                return []
            raise
