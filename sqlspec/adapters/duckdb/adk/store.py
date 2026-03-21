"""DuckDB ADK store for Google Agent Development Kit.

DuckDB is an OLAP database optimized for analytical queries. This adapter provides:
- Embedded session storage with zero-configuration setup
- Excellent performance for analytical queries on session data
- Native JSON type support for flexible state storage
- Perfect for development, testing, and analytical workloads

Notes:
    DuckDB is optimized for OLAP workloads and analytical queries. For highly
    concurrent DML operations (frequent inserts/updates/deletes), consider
    PostgreSQL or other OLTP-optimized databases.
"""

import contextlib
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Final, cast

from sqlspec.extensions.adk import BaseAsyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.adapters.duckdb.config import DuckDBConfig
    from sqlspec.extensions.adk import MemoryRecord


__all__ = ("DuckdbADKMemoryStore", "DuckdbADKStore")

logger = get_logger("sqlspec.adapters.duckdb.adk.store")

DUCKDB_TABLE_NOT_FOUND_ERROR: Final = "does not exist"


class DuckdbADKStore(BaseAsyncADKStore["DuckDBConfig"]):
    """DuckDB ADK store for Google Agent Development Kit.

    Implements session and event storage for Google Agent Development Kit
    using DuckDB's synchronous driver with async wrappers via ``async_()``.
    Provides:
    - Session state management with native JSON type
    - Event history with single JSON blob (event_json) plus indexed scalars
    - Native TIMESTAMPTZ type support
    - Manual cascade delete (DuckDB has no FK CASCADE)
    - Columnar storage for analytical queries

    Args:
        config: DuckDBConfig with extension_config["adk"] settings.

    Example:
        from sqlspec.adapters.duckdb import DuckDBConfig
        from sqlspec.adapters.duckdb.adk import DuckdbADKStore

        config = DuckDBConfig(
            database="sessions.ddb",
            extension_config={
                "adk": {
                    "session_table": "my_sessions",
                    "events_table": "my_events",
                    "owner_id_column": "tenant_id INTEGER REFERENCES tenants(id)"
                }
            }
        )
        store = DuckdbADKStore(config)
        await store.ensure_tables()

    Notes:
        - Uses DuckDB native JSON type for event_json and state
        - TIMESTAMPTZ for date/time storage with microsecond precision
        - event_json stores the full ADK Event as a single JSON blob
        - Columnar storage provides excellent analytical query performance
        - DuckDB doesn't support CASCADE in foreign keys (manual cascade required)
        - Optimized for OLAP workloads; for high-concurrency writes use PostgreSQL
        - Configuration is read from config.extension_config["adk"]
    """

    __slots__ = ()

    def __init__(self, config: "DuckDBConfig") -> None:
        """Initialize DuckDB ADK store.

        Args:
            config: DuckDBConfig instance.

        Notes:
            Configuration is read from config.extension_config["adk"]:
            - session_table: Sessions table name (default: "adk_sessions")
            - events_table: Events table name (default: "adk_events")
            - owner_id_column: Optional owner FK column DDL (default: None)
        """
        super().__init__(config)

    async def _get_create_sessions_table_sql(self) -> str:
        """Get DuckDB CREATE TABLE SQL for sessions.

        Returns:
            SQL statement to create adk_sessions table with indexes.

        Notes:
            - VARCHAR for IDs and names
            - JSON type for state storage (DuckDB native)
            - TIMESTAMPTZ for create_time and update_time
            - CURRENT_TIMESTAMP for defaults
            - Optional owner ID column for multi-tenant scenarios
            - Composite index on (app_name, user_id) for listing
            - Index on update_time DESC for recent session queries
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

    async def _get_create_events_table_sql(self) -> str:
        """Get DuckDB CREATE TABLE SQL for events.

        Returns:
            SQL statement to create adk_events table with indexes.

        Notes:
            - 5-column schema: session_id, invocation_id, author, timestamp, event_json
            - event_json stores the full ADK Event as a single JSON blob
            - No decomposed columns -- eliminates column drift with upstream ADK
            - Foreign key constraint (DuckDB doesn't support CASCADE)
            - Index on (session_id, timestamp ASC) for ordered event retrieval
            - Manual cascade delete required in delete_session method
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            session_id VARCHAR NOT NULL,
            invocation_id VARCHAR NOT NULL,
            author VARCHAR NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_json JSON NOT NULL,
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

    def _create_tables(self) -> None:
        """Synchronous implementation of create_tables."""
        with self._config.provide_connection() as conn:
            conn.execute(self.__get_create_sessions_table_sql_sync())
            conn.execute(self.__get_create_events_table_sql_sync())

    def __get_create_sessions_table_sql_sync(self) -> str:
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

    def __get_create_events_table_sql_sync(self) -> str:
        """Synchronous version of DDL generation for use in _create_tables."""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            session_id VARCHAR NOT NULL,
            invocation_id VARCHAR NOT NULL,
            author VARCHAR NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_json JSON NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id)
        );
        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session ON {self._events_table}(session_id, timestamp ASC);
        """

    async def create_tables(self) -> None:
        """Create both sessions and events tables if they don't exist."""
        await async_(self._create_tables)()

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

    async def create_session(
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

        Notes:
            Uses current UTC timestamp for create_time and update_time.
            State is JSON-serialized using SQLSpec serializers.
        """
        return await async_(self._create_session)(session_id, app_name, user_id, state, owner_id)

    def _get_session(self, session_id: str) -> "SessionRecord | None":
        """Synchronous implementation of get_session."""
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

    async def get_session(self, session_id: str) -> "SessionRecord | None":
        """Get session by ID.

        Args:
            session_id: Session identifier.

        Returns:
            Session record or None if not found.

        Notes:
            DuckDB returns datetime objects for TIMESTAMPTZ columns.
            JSON is parsed from database storage.
        """
        return await async_(self._get_session)(session_id)

    def _update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Synchronous implementation of update_session_state."""
        now = datetime.now(timezone.utc)
        state_json = to_json(state)

        sql = f"""
        UPDATE {self._session_table}
        SET state = ?, update_time = ?
        WHERE id = ?
        """

        with self._config.provide_connection() as conn:
            conn.execute(sql, (state_json, now, session_id))
            conn.commit()

    async def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state.

        Args:
            session_id: Session identifier.
            state: New state dictionary (replaces existing state).

        Notes:
            This replaces the entire state dictionary.
            Update time is automatically set to current UTC timestamp.
        """
        await async_(self._update_session_state)(session_id, state)

    def _delete_session(self, session_id: str) -> None:
        """Synchronous implementation of delete_session."""
        delete_events_sql = f"DELETE FROM {self._events_table} WHERE session_id = ?"
        delete_session_sql = f"DELETE FROM {self._session_table} WHERE id = ?"

        with self._config.provide_connection() as conn:
            conn.execute(delete_events_sql, (session_id,))
            conn.execute(delete_session_sql, (session_id,))
            conn.commit()

    async def delete_session(self, session_id: str) -> None:
        """Delete session and all associated events.

        Args:
            session_id: Session identifier.

        Notes:
            DuckDB doesn't support CASCADE in foreign keys, so we manually delete events first.
        """
        await async_(self._delete_session)(session_id)

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
        return await async_(self._list_sessions)(app_name, user_id)

    def _append_event(self, event_record: EventRecord) -> None:
        """Synchronous implementation of append_event."""
        event_json_str = to_json(event_record["event_json"])

        sql = f"""
        INSERT INTO {self._events_table}
        (session_id, invocation_id, author, timestamp, event_json)
        VALUES (?, ?, ?, ?, ?)
        """

        with self._config.provide_connection() as conn:
            conn.execute(
                sql,
                (
                    event_record["session_id"],
                    event_record["invocation_id"],
                    event_record["author"],
                    event_record["timestamp"],
                    event_json_str,
                ),
            )
            conn.commit()

    async def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session.

        Args:
            event_record: Event record with 5 keys (session_id, invocation_id,
                author, timestamp, event_json).
        """
        await async_(self._append_event)(event_record)

    def _append_event_and_update_state(
        self, event_record: EventRecord, session_id: str, state: "dict[str, Any]"
    ) -> None:
        """Synchronous implementation of append_event_and_update_state."""
        now = datetime.now(timezone.utc)
        state_json = to_json(state)
        event_json_str = to_json(event_record["event_json"])

        insert_sql = f"""
        INSERT INTO {self._events_table}
        (session_id, invocation_id, author, timestamp, event_json)
        VALUES (?, ?, ?, ?, ?)
        """

        update_sql = f"""
        UPDATE {self._session_table}
        SET state = ?, update_time = ?
        WHERE id = ?
        """

        with self._config.provide_connection() as conn:
            conn.execute(
                insert_sql,
                (
                    event_record["session_id"],
                    event_record["invocation_id"],
                    event_record["author"],
                    event_record["timestamp"],
                    event_json_str,
                ),
            )
            conn.execute(update_sql, (state_json, now, session_id))
            conn.commit()

    async def append_event_and_update_state(
        self, event_record: EventRecord, session_id: str, state: "dict[str, Any]"
    ) -> None:
        """Atomically append an event and update the session's durable state.

        The event insert and state update succeed together or fail together
        within a single DuckDB transaction.

        Args:
            event_record: Event record to store (5-key shape).
            session_id: Session identifier whose state should be updated.
            state: Post-append durable state snapshot (``temp:`` keys already
                stripped by the service layer).
        """
        await async_(self._append_event_and_update_state)(event_record, session_id, state)

    def _get_events(
        self, session_id: str, after_timestamp: "datetime | None" = None, limit: "int | None" = None
    ) -> "list[EventRecord]":
        """Synchronous implementation of get_events."""
        where_clauses = ["session_id = ?"]
        params: list[Any] = [session_id]

        if after_timestamp is not None:
            where_clauses.append("timestamp > ?")
            params.append(after_timestamp)

        where_clause = " AND ".join(where_clauses)
        limit_clause = f" LIMIT {limit}" if limit else ""

        sql = f"""
        SELECT session_id, invocation_id, author, timestamp, event_json
        FROM {self._events_table}
        WHERE {where_clause}
        ORDER BY timestamp ASC{limit_clause}
        """

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.execute(sql, params)
                rows = cursor.fetchall()

                return [
                    EventRecord(
                        session_id=row[0],
                        invocation_id=row[1],
                        author=row[2],
                        timestamp=row[3],
                        event_json=from_json(row[4]) if isinstance(row[4], str) else row[4],
                    )
                    for row in rows
                ]
        except Exception as e:
            if DUCKDB_TABLE_NOT_FOUND_ERROR in str(e):
                return []
            raise

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
        """
        return await async_(self._get_events)(session_id, after_timestamp, limit)


class DuckdbADKMemoryStore(BaseAsyncADKMemoryStore["DuckDBConfig"]):
    """DuckDB ADK memory store using synchronous DuckDB driver with async wrappers.

    Implements memory entry storage for Google Agent Development Kit
    using DuckDB's synchronous driver with async wrappers via ``async_()``.
    Provides:
    - Session memory storage with native JSON type
    - Simple ILIKE search or BM25 full-text search via FTS extension
    - Native TIMESTAMP type support
    - Deduplication via event_id unique constraint
    - Efficient upserts using INSERT OR IGNORE
    - Columnar storage for analytical queries

    Args:
        config: DuckDBConfig with extension_config["adk"] settings.

    Example:
        from sqlspec.adapters.duckdb import DuckDBConfig
        from sqlspec.adapters.duckdb.adk.store import DuckdbADKMemoryStore

        config = DuckDBConfig(
            database="app.ddb",
            extension_config={
                "adk": {
                    "memory_table": "adk_memory_entries",
                    "memory_max_results": 20,
                }
            }
        )
        store = DuckdbADKMemoryStore(config)
        await store.ensure_tables()

    Notes:
        - Uses DuckDB native JSON type (not JSONB)
        - TIMESTAMP for date/time storage with microsecond precision
        - event_id UNIQUE constraint for deduplication
        - Composite index on (app_name, user_id, timestamp DESC)
        - FTS uses match_bm25() for BM25-ranked results (not @@ operator)
        - FTS index is refreshed after inserts, not on every search
        - Columnar storage provides excellent analytical query performance
        - Optimized for OLAP workloads; for high-concurrency writes use PostgreSQL
        - Configuration is read from config.extension_config["adk"]
    """

    __slots__ = ()

    def __init__(self, config: "DuckDBConfig") -> None:
        """Initialize DuckDB ADK memory store.

        Args:
            config: DuckDBConfig instance.

        Notes:
            Configuration is read from config.extension_config["adk"]:
            - memory_table: Memory table name (default: "adk_memory_entries")
            - memory_use_fts: Enable full-text search when supported (default: False)
            - memory_max_results: Max search results (default: 20)
            - owner_id_column: Optional owner FK column DDL (default: None)
            - enable_memory: Whether memory is enabled (default: True)
        """
        super().__init__(config)

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
            conn.execute(
                f"PRAGMA create_fts_index('{self._memory_table}', 'id', 'content_text', "
                f"stemmer='porter', stopwords='english', strip_accents=1, lower=1)"
            )
        except Exception as exc:
            logger.debug("Failed to create DuckDB FTS index: %s", exc)

    def _refresh_fts_index(self, conn: Any) -> None:
        """Rebuild the FTS index to reflect recent inserts.

        DuckDB FTS indexes do not auto-update. This must be called after
        insert/update/delete operations, NOT on every search.
        """
        if not self._ensure_fts_extension(conn):
            return

        with contextlib.suppress(Exception):
            conn.execute(f"PRAGMA drop_fts_index('{self._memory_table}')")

        try:
            conn.execute(
                f"PRAGMA create_fts_index('{self._memory_table}', 'id', 'content_text', "
                f"overwrite=1, stemmer='porter', stopwords='english', strip_accents=1, lower=1)"
            )
        except Exception as exc:
            logger.debug("Failed to refresh DuckDB FTS index: %s", exc)

    async def _get_create_memory_table_sql(self) -> str:
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

    def _get_drop_memory_table_sql(self) -> "list[str]":
        """Get DuckDB DROP TABLE SQL statements."""
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    def _create_tables(self) -> None:
        """Synchronous implementation of create_tables."""
        if not self._enabled:
            return

        ddl = self.__get_create_memory_table_sql_sync()
        with self._config.provide_connection() as conn:
            conn.execute(ddl)
            if self._use_fts:
                self._create_fts_index(conn)

    def __get_create_memory_table_sql_sync(self) -> str:
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

    async def create_tables(self) -> None:
        """Create the memory table and indexes if they don't exist."""
        await async_(self._create_tables)()

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

    async def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication.

        After successful inserts, refreshes the FTS index if FTS is enabled.
        """
        return await async_(self._insert_memory_entries)(entries, owner_id)

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
        if self._use_fts:
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

        with self._config.provide_connection() as conn:
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

    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query.

        When FTS is enabled, uses ``match_bm25()`` for BM25-ranked results.
        Falls back to ILIKE for simple substring matching.
        """
        return await async_(self._search_entries)(query, app_name, user_id, limit)

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

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        return await async_(self._delete_entries_by_session)(session_id)

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

    async def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        return await async_(self._delete_entries_older_than)(days)
