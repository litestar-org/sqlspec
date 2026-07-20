"""DuckDB ADK store for Google Agent Development Kit.

DuckDB is an OLAP database optimized for analytical queries. This adapter provides:
- Embedded session storage with zero-configuration setup
- Excellent performance for analytical queries on session data
- Native JSON type support for flexible state storage
- Perfect for development, testing, and analytical workloads
"""

import contextlib
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Final, cast

from typing_extensions import NotRequired, TypedDict

from sqlspec.config import ADKConfig
from sqlspec.extensions.adk import BaseSyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from sqlspec.adapters.duckdb.config import DuckDBConfig
    from sqlspec.extensions.adk import MemoryRecord


__all__ = ("DuckdbADKConfig", "DuckdbADKFTSOptions", "DuckdbADKMemoryStore", "DuckdbADKStore")

logger = get_logger("sqlspec.adapters.duckdb.adk.store")

DUCKDB_TABLE_NOT_FOUND_ERROR: Final = "does not exist"
DUCKDB_EVENT_PROJECTION_STRUCT: Final = "STRUCT(author VARCHAR, node_info STRUCT(path VARCHAR))"
DUCKDB_FTS_ALLOWED_OPTIONS: Final = ("stemmer", "stopwords", "ignore", "strip_accents", "lower", "overwrite")
DUCKDB_FTS_CREATE_OPTION_ORDER: Final = ("stemmer", "stopwords", "ignore", "strip_accents", "lower", "overwrite")
DUCKDB_FTS_REFRESH_OPTION_ORDER: Final = ("overwrite", "stemmer", "stopwords", "ignore", "strip_accents", "lower")
DUCKDB_FTS_DEFAULT_OPTIONS: Final[dict[str, str | int]] = {
    "stemmer": "porter",
    "stopwords": "english",
    "strip_accents": 1,
    "lower": 1,
}


class DuckdbADKFTSOptions(TypedDict):
    """DuckDB FTS ``PRAGMA create_fts_index`` options for ADK memory search."""

    stemmer: NotRequired[str]
    """Stemmer name. Defaults to ``"porter"``."""

    stopwords: NotRequired[str]
    """Stopword table/name. Defaults to ``"english"``."""

    ignore: NotRequired[str]
    """Regular expression ignored by DuckDB tokenization."""

    strip_accents: NotRequired[bool | int]
    """Whether DuckDB strips accents during tokenization. Defaults to ``1``."""

    lower: NotRequired[bool | int]
    """Whether DuckDB lowercases text during tokenization. Defaults to ``1``."""

    overwrite: NotRequired[bool | int]
    """Whether initial FTS index creation overwrites an existing index. Defaults to ``0``."""


class DuckdbADKConfig(ADKConfig):
    """DuckDB-specific ADK extension settings.

    Use these keys inside ``extension_config["adk"]`` with the DuckDB ADK stores.
    Shared ADK table and service settings are inherited from :class:`ADKConfig`.
    """

    enable_event_generated_columns: NotRequired[bool]
    """Create DuckDB virtual generated columns for common ADK event JSON paths."""

    enable_event_generated_column_indexes: NotRequired[bool]
    """Index generated event projection columns when generated columns are enabled."""

    memory_fts_options: NotRequired[DuckdbADKFTSOptions]
    """DuckDB FTS ``PRAGMA create_fts_index`` options for ADK memory search."""


class DuckdbADKStore(BaseSyncADKStore["DuckDBConfig"]):
    """DuckDB ADK store for Google Agent Development Kit.

    Implements session and event storage for Google Agent Development Kit
    using DuckDB's synchronous driver with a synchronous public API.
    Provides:
        - Session state management with native JSON type
        - Event history with single JSON blob (event_data) plus indexed scalars
        - Native TIMESTAMPTZ type support
        - Manual cascade delete (DuckDB has no FK CASCADE)
        - Columnar storage for analytical queries

    Args:
        config: DuckDBConfig with extension_config["adk"] settings.
    """

    __slots__ = ()

    def __init__(self, config: "DuckDBConfig") -> None:
        """Initialize DuckDB ADK store.

        Args:
            config: DuckDBConfig instance.
        """
        super().__init__(config)

    def create_tables(self) -> None:
        """Create both sessions and events tables if they don't exist."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        self._create_tables()

    def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        """Create a new session.

        Args:
            session_id: Unique session identifier.
            app_name: Application name.
            user_id: User identifier.
            state: Initial session state.
            owner_id: Optional owner ID value for owner_id_column (if configured).

        Returns:
            Created session record.
        """
        return self._create_session(session_id, app_name, user_id, state, owner_id)

    def get_session(
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
        return self._get_session(app_name, user_id, session_id, renew_for=renew_for)

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state.

        Args:
            app_name: Application name.
            user_id: User identifier.
            session_id: Session identifier.
            state: New state dictionary (replaces existing state).
        """
        self._update_session_state(app_name, user_id, session_id, state)

    def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        """List sessions for an app, optionally filtered by user.

        Args:
            app_name: Application name.
            user_id: User identifier. If None, lists all sessions for the app.

        Returns:
            List of session records ordered by update_time DESC.
        """
        return self._list_sessions(app_name, user_id)

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete session and all associated events.

        Args:
            app_name: Application name.
            user_id: User identifier.
            session_id: Session identifier.
        """
        self._delete_session(app_name, user_id, session_id)

    def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session.

        Args:
            event_record: Event record to store.
        """
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
        """Atomically append an event and update the session's durable state.

        The event insert and state update succeed together or fail together
        within a single DuckDB transaction; the updated SessionRecord is
        returned via UPDATE...RETURNING.

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
        return self._get_events(app_name, user_id, session_id, after_timestamp, limit)

    def delete_expired_events(self, before: "datetime") -> int:
        """Delete events older than a timestamp."""
        return self._delete_expired_events(before)

    def delete_idle_sessions(self, updated_before: "datetime") -> int:
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

    def _sessions_table_ddl(self) -> str:
        """Get DuckDB CREATE TABLE SQL for sessions.

        Returns:
            SQL statement to create adk_sessions table with indexes.
        """
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id VARCHAR PRIMARY KEY,
            app_name VARCHAR NOT NULL,
            user_id VARCHAR NOT NULL{owner_id_line},
            state JSON NOT NULL,
            create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user ON {self._session_table}(app_name, user_id);
        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time ON {self._session_table}(update_time DESC);
        """

    def _events_table_ddl(self) -> str:
        """Get DuckDB CREATE TABLE SQL for events.

        Returns:
            SQL statement to create adk_event table with indexes.
        """
        generated_columns = self._event_generated_columns_sql()
        generated_indexes = self._event_generated_column_indexes_sql()
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR PRIMARY KEY,
            session_id VARCHAR NOT NULL,
            invocation_id VARCHAR,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_data JSON NOT NULL{generated_columns},
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id)
        );
        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session ON {self._events_table}(session_id, timestamp ASC);
        {generated_indexes}
        """

    def _event_generated_columns_sql(self) -> str:
        """Return DuckDB ADK generated event projection columns."""
        adk_config = _adk_config(self._config)
        if not adk_config.get("enable_event_generated_columns", False):
            return ""

        return f""",
            author_gc VARCHAR GENERATED ALWAYS AS ((event_data::{DUCKDB_EVENT_PROJECTION_STRUCT}).author) VIRTUAL,
            node_path_gc VARCHAR GENERATED ALWAYS AS ((event_data::{DUCKDB_EVENT_PROJECTION_STRUCT}).node_info.path) VIRTUAL"""

    def _event_generated_column_indexes_sql(self) -> str:
        """Return optional indexes for generated event projection columns."""
        adk_config = _adk_config(self._config)
        if not (
            adk_config.get("enable_event_generated_columns", False)
            and adk_config.get("enable_event_generated_column_indexes", False)
        ):
            return ""

        return f"""
        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_author_gc
            ON {self._events_table}(session_id, author_gc, timestamp ASC);

        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_node_path_gc
            ON {self._events_table}(session_id, node_path_gc, timestamp ASC);
        """

    def _app_states_table_ddl(self) -> str:
        """Get DuckDB CREATE TABLE SQL for app-scoped state."""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._app_state_table} (
            app_name VARCHAR PRIMARY KEY,
            state JSON NOT NULL,
            update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """

    def _user_states_table_ddl(self) -> str:
        """Get DuckDB CREATE TABLE SQL for user-scoped state."""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._user_state_table} (
            app_name VARCHAR NOT NULL,
            user_id VARCHAR NOT NULL,
            state JSON NOT NULL,
            update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (app_name, user_id)
        )
        """

    def _metadata_table_ddl(self) -> str:
        """Get DuckDB CREATE TABLE SQL for internal ADK metadata."""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._metadata_table} (
            key VARCHAR PRIMARY KEY,
            value VARCHAR NOT NULL
        )
        """

    def _drop_app_states_table_sql(self) -> str:
        """Get DuckDB DROP TABLE SQL for app-scoped state."""
        return f"DROP TABLE IF EXISTS {self._app_state_table}"

    def _drop_user_states_table_sql(self) -> str:
        """Get DuckDB DROP TABLE SQL for user-scoped state."""
        return f"DROP TABLE IF EXISTS {self._user_state_table}"

    def _drop_metadata_table_sql(self) -> str:
        """Get DuckDB DROP TABLE SQL for internal ADK metadata."""
        return f"DROP TABLE IF EXISTS {self._metadata_table}"

    def _drop_tables_sql(self) -> "list[str]":
        """Get DuckDB DROP TABLE SQL statements.

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
        """Synchronous implementation of create_tables."""
        with self._config.provide_connection() as conn:
            conn.execute(self._sync_sessions_table_ddl())
            conn.execute(self._sync_events_table_ddl())
            conn.execute(self._app_states_table_ddl())
            conn.execute(self._user_states_table_ddl())
            conn.execute(self._metadata_table_ddl())
            conn.commit()

    def _sync_sessions_table_ddl(self) -> str:
        """Synchronous version of DDL generation for use in _create_tables."""
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id VARCHAR PRIMARY KEY,
            app_name VARCHAR NOT NULL,
            user_id VARCHAR NOT NULL{owner_id_line},
            state JSON NOT NULL,
            create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user ON {self._session_table}(app_name, user_id);
        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time ON {self._session_table}(update_time DESC);
        """

    def _sync_events_table_ddl(self) -> str:
        """Synchronous version of DDL generation for use in _create_tables."""
        generated_columns = self._event_generated_columns_sql()
        generated_indexes = self._event_generated_column_indexes_sql()
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR PRIMARY KEY,
            session_id VARCHAR NOT NULL,
            invocation_id VARCHAR,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_data JSON NOT NULL{generated_columns},
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id)
        );
        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session ON {self._events_table}(session_id, timestamp ASC);
        {generated_indexes}
        """

    def _create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        """Synchronous implementation of create_session."""
        now = datetime.now(timezone.utc)
        state_json = to_json(state)

        params: tuple[Any, ...]
        if self._owner_id_column_name:
            sql = f"""
            INSERT INTO {self._session_table}
            (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            params = (session_id, app_name, user_id, owner_id, state_json, now, now)
        else:
            sql = f"""
            INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
            VALUES (?, ?, ?, ?, ?, ?)
            """
            params = (session_id, app_name, user_id, state_json, now, now)

        with self._config.provide_connection() as conn:
            conn.execute(sql, params)
            conn.commit()

        return SessionRecord(
            id=session_id, app_name=app_name, user_id=user_id, state=state, create_time=now, update_time=now
        )

    def _get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        """Synchronous implementation of get_session."""
        if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
            sql = f"""
            UPDATE {self._session_table}
            SET update_time = ?
            WHERE app_name = ? AND user_id = ? AND id = ?
            RETURNING id, app_name, user_id, state, create_time, update_time
            """
            params: list[Any] = [datetime.now(timezone.utc), app_name, user_id, session_id]
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = ? AND user_id = ? AND id = ?
            """
            params = [app_name, user_id, session_id]

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.execute(sql, params)
                row = cursor.fetchone()

                if row is None:
                    return None

                session_id_val, app_name, user_id, state_data, create_time, update_time = row

                state = from_json(state_data) if state_data else {}

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

    def _update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        """Synchronous implementation of update_session_state."""
        now = datetime.now(timezone.utc)
        state_json = to_json(state)

        sql = f"""
        UPDATE {self._session_table}
        SET state = ?, update_time = ?
        WHERE app_name = ? AND user_id = ? AND id = ?
        """

        with self._config.provide_connection() as conn:
            conn.execute(sql, (state_json, now, app_name, user_id, session_id))
            conn.commit()

    def _delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Synchronous implementation of delete_session."""
        delete_events_sql = f"DELETE FROM {self._events_table} WHERE session_id = ?"
        delete_session_sql = f"DELETE FROM {self._session_table} WHERE app_name = ? AND user_id = ? AND id = ?"

        with self._config.provide_connection() as conn:
            conn.execute(delete_events_sql, (session_id,))
            conn.execute(delete_session_sql, (app_name, user_id, session_id))
            conn.commit()

    def _list_sessions(self, app_name: str, user_id: "str | None" = None) -> "list[SessionRecord]":
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
                cursor = conn.execute(sql, params)
                rows = cursor.fetchall()

                return [
                    SessionRecord(
                        id=row[0],
                        app_name=row[1],
                        user_id=row[2],
                        state=from_json(row[3]) if row[3] else {},
                        create_time=row[4],
                        update_time=row[5],
                    )
                    for row in rows
                ]
        except Exception as e:
            if DUCKDB_TABLE_NOT_FOUND_ERROR in str(e):
                return []
            raise

    def _append_event(self, event_record: EventRecord) -> None:
        """Synchronous implementation of append_event."""
        event_data_str = to_json(event_record["event_data"])

        sql = f"""
        INSERT INTO {self._events_table}
        (id, session_id, invocation_id, timestamp, event_data)
        VALUES (?, ?, ?, ?, ?)
        """

        with self._config.provide_connection() as conn:
            conn.execute(
                sql,
                (
                    event_record["id"],
                    event_record["session_id"],
                    event_record["invocation_id"],
                    event_record["timestamp"],
                    event_data_str,
                ),
            )
            conn.commit()

    def _append_event_and_update_state(
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
        """Synchronous implementation of append_event_and_update_state."""
        now = datetime.now(timezone.utc)
        state_json = to_json(state)
        event_data_str = to_json(event_record["event_data"])

        insert_sql = f"""
        INSERT INTO {self._events_table}
        (id, session_id, invocation_id, timestamp, event_data)
        VALUES (?, ?, ?, ?, ?)
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
            try:
                conn.execute("BEGIN TRANSACTION")
                conn.execute(
                    insert_sql,
                    (
                        event_record["id"],
                        event_record["session_id"],
                        event_record["invocation_id"],
                        event_record["timestamp"],
                        event_data_str,
                    ),
                )
                cursor = conn.execute(update_sql, (state_json, now, app_name, user_id, session_id))
                row = cursor.fetchone()
                if row is None:
                    _raise_session_not_found(session_id)
                assert row is not None
                if app_state is not None:
                    conn.execute(app_upsert_sql, (app_name, to_json(app_state), now))
                if user_state is not None:
                    conn.execute(user_upsert_sql, (app_name, user_id, to_json(user_state), now))
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        assert row is not None
        session_id_val, app_name, user_id, state_data, create_time, update_time = row
        return SessionRecord(
            id=session_id_val,
            app_name=app_name,
            user_id=user_id,
            state=from_json(state_data) if isinstance(state_data, str) else state_data,
            create_time=create_time,
            update_time=update_time,
        )

    def _get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "list[EventRecord]":
        """Synchronous implementation of get_events."""
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
                cursor = conn.execute(sql, params)
                rows = cursor.fetchall()

                return [
                    EventRecord(
                        id=row[0],
                        session_id=row[1],
                        invocation_id=row[2],
                        timestamp=row[3],
                        event_data=from_json(row[4]) if isinstance(row[4], str) else row[4],
                        app_name=row[5],
                        user_id=row[6],
                    )
                    for row in rows
                ]
        except Exception as e:
            if DUCKDB_TABLE_NOT_FOUND_ERROR in str(e):
                return []
            raise

    def _delete_expired_events(self, before: "datetime") -> int:
        count_sql = f"SELECT COUNT(*) FROM {self._events_table} WHERE timestamp < ?"
        delete_sql = f"DELETE FROM {self._events_table} WHERE timestamp < ?"

        try:
            with self._config.provide_connection() as conn:
                count_row = conn.execute(count_sql, (before,)).fetchone()
                count = int(count_row[0]) if count_row is not None else 0
                conn.execute(delete_sql, (before,))
                conn.commit()
                return count
        except Exception as e:
            if DUCKDB_TABLE_NOT_FOUND_ERROR in str(e):
                return 0
            raise

    def _delete_idle_sessions(self, updated_before: "datetime") -> int:
        count_sql = f"SELECT COUNT(*) FROM {self._session_table} WHERE update_time < ?"
        delete_events_sql = f"""
        DELETE FROM {self._events_table}
        WHERE session_id IN (SELECT id FROM {self._session_table} WHERE update_time < ?)
        """
        delete_sessions_sql = f"DELETE FROM {self._session_table} WHERE update_time < ?"

        try:
            with self._config.provide_connection() as conn:
                count_row = conn.execute(count_sql, (updated_before,)).fetchone()
                count = int(count_row[0]) if count_row is not None else 0
                conn.execute(delete_events_sql, (updated_before,))
                conn.execute(delete_sessions_sql, (updated_before,))
                conn.commit()
                return count
        except Exception as e:
            if DUCKDB_TABLE_NOT_FOUND_ERROR in str(e):
                return 0
            raise

    def _get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        sql = f"SELECT state FROM {self._app_state_table} WHERE app_name = ?"
        try:
            with self._config.provide_connection() as conn:
                row = conn.execute(sql, (app_name,)).fetchone()
                if row is None:
                    return None
                return cast("dict[str, Any]", from_json(row[0]) if isinstance(row[0], str) else row[0])
        except Exception as e:
            if DUCKDB_TABLE_NOT_FOUND_ERROR in str(e):
                return None
            raise

    def _get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        sql = f"SELECT state FROM {self._user_state_table} WHERE app_name = ? AND user_id = ?"
        try:
            with self._config.provide_connection() as conn:
                row = conn.execute(sql, (app_name, user_id)).fetchone()
                if row is None:
                    return None
                return cast("dict[str, Any]", from_json(row[0]) if isinstance(row[0], str) else row[0])
        except Exception as e:
            if DUCKDB_TABLE_NOT_FOUND_ERROR in str(e):
                return None
            raise

    def _upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        now = datetime.now(timezone.utc)
        sql = f"""
        INSERT INTO {self._app_state_table} (app_name, state, update_time)
        VALUES (?, ?, ?)
        ON CONFLICT(app_name) DO UPDATE SET
            state = excluded.state,
            update_time = excluded.update_time
        """
        with self._config.provide_connection() as conn:
            conn.execute(sql, (app_name, to_json(state), now))
            conn.commit()

    def _upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        now = datetime.now(timezone.utc)
        sql = f"""
        INSERT INTO {self._user_state_table} (app_name, user_id, state, update_time)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(app_name, user_id) DO UPDATE SET
            state = excluded.state,
            update_time = excluded.update_time
        """
        with self._config.provide_connection() as conn:
            conn.execute(sql, (app_name, user_id, to_json(state), now))
            conn.commit()

    def _get_metadata(self, key: str) -> "str | None":
        sql = f"SELECT value FROM {self._metadata_table} WHERE key = ?"
        try:
            with self._config.provide_connection() as conn:
                row = conn.execute(sql, (key,)).fetchone()
                return row[0] if row is not None else None
        except Exception as e:
            if DUCKDB_TABLE_NOT_FOUND_ERROR in str(e):
                return None
            raise

    def _set_metadata(self, key: str, value: str) -> None:
        sql = f"""
        INSERT INTO {self._metadata_table} (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """
        with self._config.provide_connection() as conn:
            conn.execute(sql, (key, value))
            conn.commit()


class DuckdbADKMemoryStore(BaseSyncADKMemoryStore["DuckDBConfig"]):
    """DuckDB ADK memory store using synchronous DuckDB driver with async wrappers.

    Implements memory entry storage for Google Agent Development Kit
    using DuckDB's synchronous driver with a synchronous public API.
    Provides:
        - Session memory storage with native JSON type
        - Simple ILIKE search or BM25 full-text search via FTS extension
        - Native TIMESTAMP type support
        - Deduplication via event_id unique constraint
        - Efficient upserts using INSERT OR IGNORE
        - Columnar storage for analytical queries

    Args:
        config: DuckDBConfig with extension_config["adk"] settings.
    """

    __slots__ = ()

    def __init__(self, config: "DuckDBConfig") -> None:
        """Initialize DuckDB ADK memory store.

        Args:
            config: DuckDBConfig instance.
        """
        super().__init__(config)

    def create_tables(self) -> None:
        """Create the memory table and indexes if they don't exist."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        self._create_tables()

    def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication.

        After successful inserts, refreshes the FTS index if FTS is enabled.
        """
        return self._insert_memory_entries(entries, owner_id)

    def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query.

        When FTS is enabled, uses ``match_bm25()`` for BM25-ranked results.
        Falls back to ILIKE for simple substring matching.
        """
        return self._search_entries(query, app_name, user_id, limit)

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        return self._delete_entries_by_session(session_id)

    def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        return self._delete_entries_older_than(days)

    def _ensure_fts_extension(self, conn: Any) -> bool:
        """Ensure the DuckDB FTS extension is available for this connection."""
        with contextlib.suppress(Exception):
            conn.execute("INSTALL fts")

        try:
            conn.execute("LOAD fts")
        except Exception as exc:
            logger.debug("DuckDB FTS extension unavailable: %s", exc)
            return False

        return True

    def _create_fts_index(self, conn: Any) -> None:
        """Create FTS index for the memory table."""
        if not self._ensure_fts_extension(conn):
            return

        try:
            conn.execute(self._fts_index_ddl(overwrite=False))
            conn.commit()
        except Exception as exc:
            logger.debug("Failed to create DuckDB FTS index: %s", exc)

    def _refresh_fts_index(self, conn: Any) -> None:
        """Rebuild the FTS index to reflect recent inserts.

        DuckDB FTS indexes do not auto-update. This must be called after
        insert/update/delete operations, NOT on every search.
        """
        if not self._ensure_fts_extension(conn):
            return

        try:
            conn.execute(self._fts_index_ddl(overwrite=True))
            conn.commit()
        except Exception as exc:
            logger.debug("Failed to refresh DuckDB FTS index: %s", exc)

    def _fts_index_ddl(self, *, overwrite: bool) -> str:
        """Return DuckDB FTS index PRAGMA SQL for the memory table."""
        return f"PRAGMA create_fts_index('{self._memory_table}', 'id', 'content_text', {self._render_fts_options(overwrite=overwrite)})"

    def _render_fts_options(self, *, overwrite: bool) -> str:
        """Render DuckDB FTS options from adapter-local ADK config."""
        options: dict[str, object] = dict(DUCKDB_FTS_DEFAULT_OPTIONS)
        adk_config = _adk_config(self._config)
        configured_options = adk_config.get("memory_fts_options", {})
        if configured_options:
            _validate_duckdb_fts_options(configured_options)
            options.update(configured_options)
        if overwrite:
            options["overwrite"] = 1

        option_order = DUCKDB_FTS_REFRESH_OPTION_ORDER if overwrite else DUCKDB_FTS_CREATE_OPTION_ORDER
        return ", ".join(
            f"{option_name}={_format_duckdb_fts_option(options[option_name])}"
            for option_name in option_order
            if option_name in options
        )

    def _memory_table_ddl(self) -> str:
        """Get DuckDB CREATE TABLE SQL for memory entries.

        Returns:
            SQL statement to create memory table with indexes.
        """
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_line},
            timestamp TIMESTAMP NOT NULL,
            content_json JSON NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json JSON,
            inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_app_user_time
            ON {self._memory_table}(app_name, user_id, timestamp DESC);

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_session
            ON {self._memory_table}(session_id);
        """

    def _drop_memory_table_sql(self) -> "list[str]":
        """Get DuckDB DROP TABLE SQL statements."""
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    def _create_tables(self) -> None:
        """Synchronous implementation of create_tables."""
        if not self._enabled:
            return

        ddl = self._sync_memory_table_ddl()
        with self._config.provide_connection() as conn:
            conn.execute(ddl)
            if self._use_fts:
                self._create_fts_index(conn)

    def _sync_memory_table_ddl(self) -> str:
        """Synchronous version of DDL generation for use in _create_tables."""
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_line},
            timestamp TIMESTAMP NOT NULL,
            content_json JSON NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json JSON,
            inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_app_user_time
            ON {self._memory_table}(app_name, user_id, timestamp DESC);

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_session
            ON {self._memory_table}(session_id);
        """

    def _insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Synchronous implementation of insert_memory_entries."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        inserted_count = 0
        if self._owner_id_column_name:
            sql = f"""
            INSERT INTO {self._memory_table} (
                id, session_id, app_name, user_id, event_id, author,
                {self._owner_id_column_name}, timestamp, content_json,
                content_text, metadata_json, inserted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO NOTHING RETURNING 1
            """
        else:
            sql = f"""
            INSERT INTO {self._memory_table} (
                id, session_id, app_name, user_id, event_id, author,
                timestamp, content_json, content_text, metadata_json, inserted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO NOTHING RETURNING 1
            """

        with self._config.provide_connection() as conn:
            for entry in entries:
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
                        entry["timestamp"],
                        to_json(entry["content_json"]),
                        entry["content_text"],
                        to_json(entry["metadata_json"]),
                        entry["inserted_at"],
                    )
                else:
                    params = (
                        entry["id"],
                        entry["session_id"],
                        entry["app_name"],
                        entry["user_id"],
                        entry["event_id"],
                        entry["author"],
                        entry["timestamp"],
                        to_json(entry["content_json"]),
                        entry["content_text"],
                        to_json(entry["metadata_json"]),
                        entry["inserted_at"],
                    )
                result = conn.execute(sql, params)
                inserted_count += len(result.fetchall())
            conn.commit()

            # Refresh FTS index after inserts, not on search
            if self._use_fts and inserted_count > 0:
                self._refresh_fts_index(conn)

        return inserted_count

    def _search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Synchronous implementation of search_entries."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not query:
            return []

        limit_value = limit or self._max_results
        use_fts = self._use_fts

        with self._config.provide_connection() as conn:
            if use_fts and not self._ensure_fts_extension(conn):
                use_fts = False

            if use_fts:
                # Use match_bm25() -- the correct DuckDB FTS syntax
                sql = f"""
            SELECT m.*
            FROM {self._memory_table} m
            JOIN (
                SELECT id, fts_main_{self._memory_table}.match_bm25(id, ?, fields := 'content_text') AS score
                FROM {self._memory_table}
            ) fts ON m.id = fts.id
            WHERE m.app_name = ? AND m.user_id = ? AND fts.score IS NOT NULL
            ORDER BY fts.score DESC
            LIMIT ?
            """
                params = (query, app_name, user_id, limit_value)
            else:
                sql = f"""
            SELECT * FROM {self._memory_table}
            WHERE app_name = ? AND user_id = ? AND content_text ILIKE ?
            ORDER BY timestamp DESC
            LIMIT ?
            """
                params = (app_name, user_id, f"%{query}%", limit_value)

            rows = conn.execute(sql, params).fetchall()
            columns = [col[0] for col in conn.description or []]
        records: list[MemoryRecord] = []
        for row in rows:
            record = cast("MemoryRecord", dict(zip(columns, row, strict=False)))
            content_value = record["content_json"]
            if isinstance(content_value, (str, bytes)):
                record["content_json"] = from_json(content_value)
            metadata_value = record.get("metadata_json")
            if isinstance(metadata_value, (str, bytes)):
                record["metadata_json"] = from_json(metadata_value)
            records.append(record)
        return records

    def _delete_entries_by_session(self, session_id: str) -> int:
        """Synchronous implementation of delete_entries_by_session."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"DELETE FROM {self._memory_table} WHERE session_id = ? RETURNING 1"
        with self._config.provide_connection() as conn:
            result = conn.execute(sql, (session_id,))
            deleted_count = len(result.fetchall())
            conn.commit()
            if self._use_fts and deleted_count > 0:
                self._refresh_fts_index(conn)
            return deleted_count

    def _delete_entries_older_than(self, days: int) -> int:
        """Synchronous implementation of delete_entries_older_than."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < (CURRENT_TIMESTAMP - INTERVAL '{days} days')
        RETURNING 1
        """
        with self._config.provide_connection() as conn:
            result = conn.execute(sql)
            deleted_count = len(result.fetchall())
            conn.commit()
            if self._use_fts and deleted_count > 0:
                self._refresh_fts_index(conn)
            return deleted_count


def _raise_session_not_found(session_id: str) -> None:
    msg = f"Session {session_id} not found during append_event_and_update_state."
    raise ValueError(msg)


def _adk_config(config: Any) -> DuckdbADKConfig:
    """Return DuckDB ADK extension settings from ``extension_config["adk"]``."""
    extension_config = getattr(config, "extension_config", {})
    if not isinstance(extension_config, dict):
        return cast("DuckdbADKConfig", {})
    adk_config = extension_config.get("adk", {})
    if not isinstance(adk_config, dict):
        return cast("DuckdbADKConfig", {})
    return cast("DuckdbADKConfig", adk_config)


def _validate_duckdb_fts_options(options: Mapping[str, object]) -> None:
    """Validate DuckDB FTS option names before rendering a PRAGMA."""
    unknown_options = sorted(set(options) - set(DUCKDB_FTS_ALLOWED_OPTIONS))
    if unknown_options:
        msg = f"Unsupported DuckDB ADK memory_fts_options: {', '.join(unknown_options)}"
        raise ValueError(msg)


def _format_duckdb_fts_option(value: object) -> str:
    """Render a DuckDB FTS option value."""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        escaped_value = value.replace("'", "''")
        return f"'{escaped_value}'"
    msg = f"DuckDB ADK memory_fts_options values must be str, int, or bool; got {type(value).__name__}"
    raise TypeError(msg)
