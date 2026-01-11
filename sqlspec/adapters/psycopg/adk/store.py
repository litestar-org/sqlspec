"""Psycopg ADK store for Google Agent Development Kit session/event storage."""

from typing import TYPE_CHECKING, Any

from psycopg import errors
from psycopg import sql as pg_sql
from psycopg.types.json import Jsonb

from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from datetime import datetime

    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
    from sqlspec.extensions.adk import MemoryRecord


__all__ = ("PsycopgAsyncADKMemoryStore", "PsycopgAsyncADKStore", "PsycopgSyncADKMemoryStore", "PsycopgSyncADKStore")

logger = get_logger("sqlspec.adapters.psycopg.adk.store")


def _build_insert_params(entry: "MemoryRecord") -> "tuple[object, ...]":
    return (
        entry["id"],
        entry["session_id"],
        entry["app_name"],
        entry["user_id"],
        entry["event_id"],
        entry["author"],
        entry["timestamp"],
        Jsonb(entry["content_json"]),
        entry["content_text"],
        Jsonb(entry["metadata_json"]) if entry["metadata_json"] is not None else None,
        entry["inserted_at"],
    )


def _build_insert_params_with_owner(entry: "MemoryRecord", owner_id: "object | None") -> "tuple[object, ...]":
    return (
        entry["id"],
        entry["session_id"],
        entry["app_name"],
        entry["user_id"],
        entry["event_id"],
        entry["author"],
        owner_id,
        entry["timestamp"],
        Jsonb(entry["content_json"]),
        entry["content_text"],
        Jsonb(entry["metadata_json"]) if entry["metadata_json"] is not None else None,
        entry["inserted_at"],
    )


class PsycopgAsyncADKStore(BaseAsyncADKStore["PsycopgAsyncConfig"]):
    """PostgreSQL ADK store using Psycopg3 driver.

    Implements session and event storage for Google Agent Development Kit
    using PostgreSQL via psycopg3 with native async/await support.

    Provides:
    - Session state management with JSONB storage and merge operations
    - Event history tracking with BYTEA-serialized actions
    - Microsecond-precision timestamps with TIMESTAMPTZ
    - Foreign key constraints with cascade delete
    - Efficient upserts using ON CONFLICT
    - GIN indexes for JSONB queries
    - HOT updates with FILLFACTOR 80

    Args:
        config: PsycopgAsyncConfig with extension_config["adk"] settings.

    Example:
        from sqlspec.adapters.psycopg import PsycopgAsyncConfig
        from sqlspec.adapters.psycopg.adk import PsycopgAsyncADKStore

        config = PsycopgAsyncConfig(
            connection_config={"conninfo": "postgresql://..."},
            extension_config={
                "adk": {
                    "session_table": "my_sessions",
                    "events_table": "my_events",
                    "owner_id_column": "tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"
                }
            }
        )
        store = PsycopgAsyncADKStore(config)
        await store.ensure_tables()

    Notes:
        - PostgreSQL JSONB type used for state (more efficient than JSON)
        - Psycopg requires wrapping dicts with Jsonb() for type safety
        - TIMESTAMPTZ provides timezone-aware microsecond precision
        - State merging uses `state || $1::jsonb` operator for efficiency
        - BYTEA for pre-serialized actions from Google ADK
        - GIN index on state for JSONB queries (partial index)
        - FILLFACTOR 80 leaves space for HOT updates
        - Parameter style: $1, $2, $3 (PostgreSQL numeric placeholders)
        - Configuration is read from config.extension_config["adk"]
    """

    __slots__ = ()

    def __init__(self, config: "PsycopgAsyncConfig") -> None:
        """Initialize Psycopg ADK store.

        Args:
            config: PsycopgAsyncConfig instance.

        Notes:
            Configuration is read from config.extension_config["adk"]:
            - session_table: Sessions table name (default: "adk_sessions")
            - events_table: Events table name (default: "adk_events")
            - owner_id_column: Optional owner FK column DDL (default: None)
        """
        super().__init__(config)

    async def _get_create_sessions_table_sql(self) -> str:
        """Get PostgreSQL CREATE TABLE SQL for sessions.

        Returns:
            SQL statement to create adk_sessions table with indexes.

        Notes:
            - VARCHAR(128) for IDs and names (sufficient for UUIDs and app names)
            - JSONB type for state storage with default empty object
            - TIMESTAMPTZ with microsecond precision
            - FILLFACTOR 80 for HOT updates (reduces table bloat)
            - Composite index on (app_name, user_id) for listing
            - Index on update_time DESC for recent session queries
            - Partial GIN index on state for JSONB queries (only non-empty)
            - Optional owner ID column for multi-tenancy or user references
        """
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id VARCHAR(128) PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL{owner_id_line},
            state JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) WITH (fillfactor = 80);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user
            ON {self._session_table}(app_name, user_id);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time
            ON {self._session_table}(update_time DESC);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_state
            ON {self._session_table} USING GIN (state)
            WHERE state != '{{}}'::jsonb;
        """

    async def _get_create_events_table_sql(self) -> str:
        """Get PostgreSQL CREATE TABLE SQL for events.

        Returns:
            SQL statement to create adk_events table with indexes.

        Notes:
            - VARCHAR sizes: id(128), session_id(128), invocation_id(256), author(256),
              branch(256), error_code(256), error_message(1024)
            - BYTEA for pickled actions (no size limit)
            - JSONB for content, grounding_metadata, custom_metadata, long_running_tool_ids_json
            - BOOLEAN for partial, turn_complete, interrupted
            - Foreign key to sessions with CASCADE delete
            - Index on (session_id, timestamp ASC) for ordered event retrieval
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
            long_running_tool_ids_json JSONB,
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
        );

        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session
            ON {self._events_table}(session_id, timestamp ASC);
        """

    def _get_drop_tables_sql(self) -> "list[str]":
        """Get PostgreSQL DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop tables and indexes.

        Notes:
            Order matters: drop events table (child) before sessions (parent).
            PostgreSQL automatically drops indexes when dropping tables.
        """
        return [f"DROP TABLE IF EXISTS {self._events_table}", f"DROP TABLE IF EXISTS {self._session_table}"]

    async def create_tables(self) -> None:
        """Create both sessions and events tables if they don't exist."""
        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._get_create_sessions_table_sql())
            await driver.execute_script(await self._get_create_events_table_sql())

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
            Uses CURRENT_TIMESTAMP for create_time and update_time.
            State is wrapped with Jsonb() for PostgreSQL type safety.
            If owner_id_column is configured, owner_id value must be provided.
        """
        params: tuple[Any, ...]
        if self._owner_id_column_name:
            query = pg_sql.SQL("""
            INSERT INTO {table} (id, app_name, user_id, {owner_id_col}, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """).format(
                table=pg_sql.Identifier(self._session_table), owner_id_col=pg_sql.Identifier(self._owner_id_column_name)
            )
            params = (session_id, app_name, user_id, owner_id, Jsonb(state))
        else:
            query = pg_sql.SQL("""
            INSERT INTO {table} (id, app_name, user_id, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """).format(table=pg_sql.Identifier(self._session_table))
            params = (session_id, app_name, user_id, Jsonb(state))

        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(query, params)

        return await self.get_session(session_id)  # type: ignore[return-value]

    async def get_session(self, session_id: str) -> "SessionRecord | None":
        """Get session by ID.

        Args:
            session_id: Session identifier.

        Returns:
            Session record or None if not found.

        Notes:
            PostgreSQL returns datetime objects for TIMESTAMPTZ columns.
            JSONB is automatically deserialized by psycopg to Python dict.
        """
        query = pg_sql.SQL("""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {table}
        WHERE id = %s
        """).format(table=pg_sql.Identifier(self._session_table))

        try:
            async with self._config.provide_connection() as conn, conn.cursor() as cur:
                await cur.execute(query, (session_id,))
                row = await cur.fetchone()

                if row is None:
                    return None

                return SessionRecord(
                    id=row["id"],
                    app_name=row["app_name"],
                    user_id=row["user_id"],
                    state=row["state"],
                    create_time=row["create_time"],
                    update_time=row["update_time"],
                )
        except errors.UndefinedTable:
            return None

    async def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state.

        Args:
            session_id: Session identifier.
            state: New state dictionary (replaces existing state).

        Notes:
            This replaces the entire state dictionary.
            Uses CURRENT_TIMESTAMP for update_time.
            State is wrapped with Jsonb() for PostgreSQL type safety.
        """
        query = pg_sql.SQL("""
        UPDATE {table}
        SET state = %s, update_time = CURRENT_TIMESTAMP
        WHERE id = %s
        """).format(table=pg_sql.Identifier(self._session_table))

        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(query, (Jsonb(state), session_id))

    async def delete_session(self, session_id: str) -> None:
        """Delete session and all associated events (cascade).

        Args:
            session_id: Session identifier.

        Notes:
            Foreign key constraint ensures events are cascade-deleted.
        """
        query = pg_sql.SQL("DELETE FROM {table} WHERE id = %s").format(table=pg_sql.Identifier(self._session_table))

        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(query, (session_id,))

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
            query = pg_sql.SQL("""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {table}
            WHERE app_name = %s
            ORDER BY update_time DESC
            """).format(table=pg_sql.Identifier(self._session_table))
            params: tuple[str, ...] = (app_name,)
        else:
            query = pg_sql.SQL("""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {table}
            WHERE app_name = %s AND user_id = %s
            ORDER BY update_time DESC
            """).format(table=pg_sql.Identifier(self._session_table))
            params = (app_name, user_id)

        try:
            async with self._config.provide_connection() as conn, conn.cursor() as cur:
                await cur.execute(query, params)
                rows = await cur.fetchall()

                return [
                    SessionRecord(
                        id=row["id"],
                        app_name=row["app_name"],
                        user_id=row["user_id"],
                        state=row["state"],
                        create_time=row["create_time"],
                        update_time=row["update_time"],
                    )
                    for row in rows
                ]
        except errors.UndefinedTable:
            return []

    async def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session.

        Args:
            event_record: Event record to store.

        Notes:
            Uses CURRENT_TIMESTAMP for timestamp if not provided.
            JSONB fields are wrapped with Jsonb() for PostgreSQL type safety.
        """
        content_json = event_record.get("content")
        grounding_metadata_json = event_record.get("grounding_metadata")
        custom_metadata_json = event_record.get("custom_metadata")

        query = pg_sql.SQL("""
        INSERT INTO {table} (
            id, session_id, app_name, user_id, invocation_id, author, actions,
            long_running_tool_ids_json, branch, timestamp, content,
            grounding_metadata, custom_metadata, partial, turn_complete,
            interrupted, error_code, error_message
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """).format(table=pg_sql.Identifier(self._events_table))

        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(
                query,
                (
                    event_record["id"],
                    event_record["session_id"],
                    event_record["app_name"],
                    event_record["user_id"],
                    event_record.get("invocation_id"),
                    event_record.get("author"),
                    event_record.get("actions"),
                    event_record.get("long_running_tool_ids_json"),
                    event_record.get("branch"),
                    event_record["timestamp"],
                    Jsonb(content_json) if content_json is not None else None,
                    Jsonb(grounding_metadata_json) if grounding_metadata_json is not None else None,
                    Jsonb(custom_metadata_json) if custom_metadata_json is not None else None,
                    event_record.get("partial"),
                    event_record.get("turn_complete"),
                    event_record.get("interrupted"),
                    event_record.get("error_code"),
                    event_record.get("error_message"),
                ),
            )

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
            JSONB fields are automatically deserialized by psycopg.
            BYTEA actions are converted to bytes.
        """
        where_clauses = ["session_id = %s"]
        params: list[Any] = [session_id]

        if after_timestamp is not None:
            where_clauses.append("timestamp > %s")
            params.append(after_timestamp)

        where_clause = " AND ".join(where_clauses)
        if limit:
            params.append(limit)

        query = pg_sql.SQL(
            """
        SELECT id, session_id, app_name, user_id, invocation_id, author, actions,
               long_running_tool_ids_json, branch, timestamp, content,
               grounding_metadata, custom_metadata, partial, turn_complete,
               interrupted, error_code, error_message
        FROM {table}
        WHERE {where_clause}
        ORDER BY timestamp ASC{limit_clause}
        """
        ).format(
            table=pg_sql.Identifier(self._events_table),
            where_clause=pg_sql.SQL(where_clause),  # pyright: ignore[reportArgumentType]
            limit_clause=pg_sql.SQL(" LIMIT %s" if limit else ""),  # pyright: ignore[reportArgumentType]
        )

        try:
            async with self._config.provide_connection() as conn, conn.cursor() as cur:
                await cur.execute(query, tuple(params))
                rows = await cur.fetchall()

                return [
                    EventRecord(
                        id=row["id"],
                        session_id=row["session_id"],
                        app_name=row["app_name"],
                        user_id=row["user_id"],
                        invocation_id=row["invocation_id"],
                        author=row["author"],
                        actions=bytes(row["actions"]) if row["actions"] else b"",
                        long_running_tool_ids_json=row["long_running_tool_ids_json"],
                        branch=row["branch"],
                        timestamp=row["timestamp"],
                        content=row["content"],
                        grounding_metadata=row["grounding_metadata"],
                        custom_metadata=row["custom_metadata"],
                        partial=row["partial"],
                        turn_complete=row["turn_complete"],
                        interrupted=row["interrupted"],
                        error_code=row["error_code"],
                        error_message=row["error_message"],
                    )
                    for row in rows
                ]
        except errors.UndefinedTable:
            return []


class PsycopgSyncADKStore(BaseSyncADKStore["PsycopgSyncConfig"]):
    """PostgreSQL synchronous ADK store using Psycopg3 driver.

    Implements session and event storage for Google Agent Development Kit
    using PostgreSQL via psycopg3 with synchronous execution.

    Provides:
    - Session state management with JSONB storage and merge operations
    - Event history tracking with BYTEA-serialized actions
    - Microsecond-precision timestamps with TIMESTAMPTZ
    - Foreign key constraints with cascade delete
    - Efficient upserts using ON CONFLICT
    - GIN indexes for JSONB queries
    - HOT updates with FILLFACTOR 80

    Args:
        config: PsycopgSyncConfig with extension_config["adk"] settings.

    Example:
        from sqlspec.adapters.psycopg import PsycopgSyncConfig
        from sqlspec.adapters.psycopg.adk import PsycopgSyncADKStore

        config = PsycopgSyncConfig(
            connection_config={"conninfo": "postgresql://..."},
            extension_config={
                "adk": {
                    "session_table": "my_sessions",
                    "events_table": "my_events",
                    "owner_id_column": "tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"
                }
            }
        )
        store = PsycopgSyncADKStore(config)
        store.ensure_tables()

    Notes:
        - PostgreSQL JSONB type used for state (more efficient than JSON)
        - Psycopg requires wrapping dicts with Jsonb() for type safety
        - TIMESTAMPTZ provides timezone-aware microsecond precision
        - State merging uses `state || $1::jsonb` operator for efficiency
        - BYTEA for pre-serialized actions from Google ADK
        - GIN index on state for JSONB queries (partial index)
        - FILLFACTOR 80 leaves space for HOT updates
        - Parameter style: $1, $2, $3 (PostgreSQL numeric placeholders)
        - Configuration is read from config.extension_config["adk"]
    """

    __slots__ = ()

    def __init__(self, config: "PsycopgSyncConfig") -> None:
        """Initialize Psycopg synchronous ADK store.

        Args:
            config: PsycopgSyncConfig instance.

        Notes:
            Configuration is read from config.extension_config["adk"]:
            - session_table: Sessions table name (default: "adk_sessions")
            - events_table: Events table name (default: "adk_events")
            - owner_id_column: Optional owner FK column DDL (default: None)
        """
        super().__init__(config)

    def _get_create_sessions_table_sql(self) -> str:
        """Get PostgreSQL CREATE TABLE SQL for sessions.

        Returns:
            SQL statement to create adk_sessions table with indexes.

        Notes:
            - VARCHAR(128) for IDs and names (sufficient for UUIDs and app names)
            - JSONB type for state storage with default empty object
            - TIMESTAMPTZ with microsecond precision
            - FILLFACTOR 80 for HOT updates (reduces table bloat)
            - Composite index on (app_name, user_id) for listing
            - Index on update_time DESC for recent session queries
            - Partial GIN index on state for JSONB queries (only non-empty)
            - Optional owner ID column for multi-tenancy or user references
        """
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id VARCHAR(128) PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL{owner_id_line},
            state JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) WITH (fillfactor = 80);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user
            ON {self._session_table}(app_name, user_id);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time
            ON {self._session_table}(update_time DESC);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_state
            ON {self._session_table} USING GIN (state)
            WHERE state != '{{}}'::jsonb;
        """

    def _get_create_events_table_sql(self) -> str:
        """Get PostgreSQL CREATE TABLE SQL for events.

        Returns:
            SQL statement to create adk_events table with indexes.

        Notes:
            - VARCHAR sizes: id(128), session_id(128), invocation_id(256), author(256),
              branch(256), error_code(256), error_message(1024)
            - BYTEA for pickled actions (no size limit)
            - JSONB for content, grounding_metadata, custom_metadata, long_running_tool_ids_json
            - BOOLEAN for partial, turn_complete, interrupted
            - Foreign key to sessions with CASCADE delete
            - Index on (session_id, timestamp ASC) for ordered event retrieval
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
            long_running_tool_ids_json JSONB,
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
        );

        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session
            ON {self._events_table}(session_id, timestamp ASC);
        """

    def _get_drop_tables_sql(self) -> "list[str]":
        """Get PostgreSQL DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop tables and indexes.

        Notes:
            Order matters: drop events table (child) before sessions (parent).
            PostgreSQL automatically drops indexes when dropping tables.
        """
        return [f"DROP TABLE IF EXISTS {self._events_table}", f"DROP TABLE IF EXISTS {self._session_table}"]

    def create_tables(self) -> None:
        """Create both sessions and events tables if they don't exist."""
        with self._config.provide_session() as driver:
            driver.execute_script(self._get_create_sessions_table_sql())
            driver.execute_script(self._get_create_events_table_sql())

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

        Notes:
            Uses CURRENT_TIMESTAMP for create_time and update_time.
            State is wrapped with Jsonb() for PostgreSQL type safety.
            If owner_id_column is configured, owner_id value must be provided.
        """
        params: tuple[Any, ...]
        if self._owner_id_column_name:
            query = pg_sql.SQL("""
            INSERT INTO {table} (id, app_name, user_id, {owner_id_col}, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """).format(
                table=pg_sql.Identifier(self._session_table), owner_id_col=pg_sql.Identifier(self._owner_id_column_name)
            )
            params = (session_id, app_name, user_id, owner_id, Jsonb(state))
        else:
            query = pg_sql.SQL("""
            INSERT INTO {table} (id, app_name, user_id, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """).format(table=pg_sql.Identifier(self._session_table))
            params = (session_id, app_name, user_id, Jsonb(state))

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(query, params)

        return self.get_session(session_id)  # type: ignore[return-value]

    def get_session(self, session_id: str) -> "SessionRecord | None":
        """Get session by ID.

        Args:
            session_id: Session identifier.

        Returns:
            Session record or None if not found.

        Notes:
            PostgreSQL returns datetime objects for TIMESTAMPTZ columns.
            JSONB is automatically deserialized by psycopg to Python dict.
        """
        query = pg_sql.SQL("""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {table}
        WHERE id = %s
        """).format(table=pg_sql.Identifier(self._session_table))

        try:
            with self._config.provide_connection() as conn, conn.cursor() as cur:
                cur.execute(query, (session_id,))
                row = cur.fetchone()

                if row is None:
                    return None

                return SessionRecord(
                    id=row["id"],
                    app_name=row["app_name"],
                    user_id=row["user_id"],
                    state=row["state"],
                    create_time=row["create_time"],
                    update_time=row["update_time"],
                )
        except errors.UndefinedTable:
            return None

    def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state.

        Args:
            session_id: Session identifier.
            state: New state dictionary (replaces existing state).

        Notes:
            This replaces the entire state dictionary.
            Uses CURRENT_TIMESTAMP for update_time.
            State is wrapped with Jsonb() for PostgreSQL type safety.
        """
        query = pg_sql.SQL("""
        UPDATE {table}
        SET state = %s, update_time = CURRENT_TIMESTAMP
        WHERE id = %s
        """).format(table=pg_sql.Identifier(self._session_table))

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(query, (Jsonb(state), session_id))

    def delete_session(self, session_id: str) -> None:
        """Delete session and all associated events (cascade).

        Args:
            session_id: Session identifier.

        Notes:
            Foreign key constraint ensures events are cascade-deleted.
        """
        query = pg_sql.SQL("DELETE FROM {table} WHERE id = %s").format(table=pg_sql.Identifier(self._session_table))

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(query, (session_id,))

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
            query = pg_sql.SQL("""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {table}
            WHERE app_name = %s
            ORDER BY update_time DESC
            """).format(table=pg_sql.Identifier(self._session_table))
            params: tuple[str, ...] = (app_name,)
        else:
            query = pg_sql.SQL("""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {table}
            WHERE app_name = %s AND user_id = %s
            ORDER BY update_time DESC
            """).format(table=pg_sql.Identifier(self._session_table))
            params = (app_name, user_id)

        try:
            with self._config.provide_connection() as conn, conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

                return [
                    SessionRecord(
                        id=row["id"],
                        app_name=row["app_name"],
                        user_id=row["user_id"],
                        state=row["state"],
                        create_time=row["create_time"],
                        update_time=row["update_time"],
                    )
                    for row in rows
                ]
        except errors.UndefinedTable:
            return []

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
            content: Event content (JSONB).
            **kwargs: Additional optional fields (invocation_id, branch, timestamp,
                     grounding_metadata, custom_metadata, partial, turn_complete,
                     interrupted, error_code, error_message, long_running_tool_ids_json).

        Returns:
            Created event record.

        Notes:
            Uses CURRENT_TIMESTAMP for timestamp if not provided in kwargs.
            JSONB fields are wrapped with Jsonb() for PostgreSQL type safety.
        """
        content_json = Jsonb(content) if content is not None else None
        grounding_metadata = kwargs.get("grounding_metadata")
        grounding_metadata_json = Jsonb(grounding_metadata) if grounding_metadata is not None else None
        custom_metadata = kwargs.get("custom_metadata")
        custom_metadata_json = Jsonb(custom_metadata) if custom_metadata is not None else None

        query = pg_sql.SQL("""
        INSERT INTO {table} (
            id, session_id, app_name, user_id, invocation_id, author, actions,
            long_running_tool_ids_json, branch, timestamp, content,
            grounding_metadata, custom_metadata, partial, turn_complete,
            interrupted, error_code, error_message
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, CURRENT_TIMESTAMP), %s, %s, %s, %s, %s, %s, %s, %s
        )
        RETURNING id, session_id, app_name, user_id, invocation_id, author, actions,
                  long_running_tool_ids_json, branch, timestamp, content,
                  grounding_metadata, custom_metadata, partial, turn_complete,
                  interrupted, error_code, error_message
        """).format(table=pg_sql.Identifier(self._events_table))

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(
                query,
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
                    kwargs.get("timestamp"),
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
            row = cur.fetchone()

            if row is None:
                msg = f"Failed to create event {event_id}"
                raise RuntimeError(msg)

            return EventRecord(
                id=row["id"],
                session_id=row["session_id"],
                app_name=row["app_name"],
                user_id=row["user_id"],
                invocation_id=row["invocation_id"],
                author=row["author"],
                actions=bytes(row["actions"]) if row["actions"] else b"",
                long_running_tool_ids_json=row["long_running_tool_ids_json"],
                branch=row["branch"],
                timestamp=row["timestamp"],
                content=row["content"],
                grounding_metadata=row["grounding_metadata"],
                custom_metadata=row["custom_metadata"],
                partial=row["partial"],
                turn_complete=row["turn_complete"],
                interrupted=row["interrupted"],
                error_code=row["error_code"],
                error_message=row["error_message"],
            )

    def list_events(self, session_id: str) -> "list[EventRecord]":
        """List events for a session ordered by timestamp.

        Args:
            session_id: Session identifier.

        Returns:
            List of event records ordered by timestamp ASC.

        Notes:
            Uses index on (session_id, timestamp ASC).
            JSONB fields are automatically deserialized by psycopg.
            BYTEA actions are converted to bytes.
        """
        query = pg_sql.SQL("""
        SELECT id, session_id, app_name, user_id, invocation_id, author, actions,
               long_running_tool_ids_json, branch, timestamp, content,
               grounding_metadata, custom_metadata, partial, turn_complete,
               interrupted, error_code, error_message
        FROM {table}
        WHERE session_id = %s
        ORDER BY timestamp ASC
        """).format(table=pg_sql.Identifier(self._events_table))

        try:
            with self._config.provide_connection() as conn, conn.cursor() as cur:
                cur.execute(query, (session_id,))
                rows = cur.fetchall()

                return [
                    EventRecord(
                        id=row["id"],
                        session_id=row["session_id"],
                        app_name=row["app_name"],
                        user_id=row["user_id"],
                        invocation_id=row["invocation_id"],
                        author=row["author"],
                        actions=bytes(row["actions"]) if row["actions"] else b"",
                        long_running_tool_ids_json=row["long_running_tool_ids_json"],
                        branch=row["branch"],
                        timestamp=row["timestamp"],
                        content=row["content"],
                        grounding_metadata=row["grounding_metadata"],
                        custom_metadata=row["custom_metadata"],
                        partial=row["partial"],
                        turn_complete=row["turn_complete"],
                        interrupted=row["interrupted"],
                        error_code=row["error_code"],
                        error_message=row["error_message"],
                    )
                    for row in rows
                ]
        except errors.UndefinedTable:
            return []


class PsycopgAsyncADKMemoryStore(BaseAsyncADKMemoryStore["PsycopgAsyncConfig"]):
    """PostgreSQL ADK memory store using Psycopg3 async driver."""

    __slots__ = ()

    def __init__(self, config: "PsycopgAsyncConfig") -> None:
        """Initialize Psycopg async memory store."""
        super().__init__(config)

    async def _get_create_memory_table_sql(self) -> str:
        """Get PostgreSQL CREATE TABLE SQL for memory entries."""
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        fts_index = ""
        if self._use_fts:
            fts_index = f"""
        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_fts
            ON {self._memory_table} USING GIN (to_tsvector('english', content_text));
            """

        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_line},
            timestamp TIMESTAMPTZ NOT NULL,
            content_json JSONB NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json JSONB,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_app_user_time
            ON {self._memory_table}(app_name, user_id, timestamp DESC);

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_session
            ON {self._memory_table}(session_id);
        {fts_index}
        """

    def _get_drop_memory_table_sql(self) -> "list[str]":
        """Get PostgreSQL DROP TABLE SQL statements."""
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    async def create_tables(self) -> None:
        """Create the memory table and indexes if they don't exist."""
        if not self._enabled:
            return

        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._get_create_memory_table_sql())

    async def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        inserted_count = 0
        if self._owner_id_column_name:
            query = pg_sql.SQL("""
            INSERT INTO {table} (
                id, session_id, app_name, user_id, event_id, author,
                {owner_id_col}, timestamp, content_json, content_text,
                metadata_json, inserted_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (event_id) DO NOTHING
            """).format(
                table=pg_sql.Identifier(self._memory_table), owner_id_col=pg_sql.Identifier(self._owner_id_column_name)
            )
        else:
            query = pg_sql.SQL("""
            INSERT INTO {table} (
                id, session_id, app_name, user_id, event_id, author,
                timestamp, content_json, content_text, metadata_json, inserted_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (event_id) DO NOTHING
            """).format(table=pg_sql.Identifier(self._memory_table))

        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            for entry in entries:
                if self._owner_id_column_name:
                    await cur.execute(query, _build_insert_params_with_owner(entry, owner_id))
                else:
                    await cur.execute(query, _build_insert_params(entry))
                if cur.rowcount and cur.rowcount > 0:
                    inserted_count += cur.rowcount

        return inserted_count

    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        effective_limit = limit if limit is not None else self._max_results

        try:
            if self._use_fts:
                try:
                    return await self._search_entries_fts(query, app_name, user_id, effective_limit)
                except Exception as exc:  # pragma: no cover - defensive fallback
                    logger.warning("FTS search failed; falling back to simple search: %s", exc)
            return await self._search_entries_simple(query, app_name, user_id, effective_limit)
        except errors.UndefinedTable:
            return []

    async def _search_entries_fts(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = pg_sql.SQL(
            """
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at,
               ts_rank(to_tsvector('english', content_text), plainto_tsquery('english', %s)) as rank
        FROM {table}
        WHERE app_name = %s
          AND user_id = %s
          AND to_tsvector('english', content_text) @@ plainto_tsquery('english', %s)
        ORDER BY rank DESC, timestamp DESC
        LIMIT %s
        """
        ).format(table=pg_sql.Identifier(self._memory_table))
        params: tuple[str, str, str, str, int] = (query, app_name, user_id, query, limit)
        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()
        return _rows_to_records(rows)

    async def _search_entries_simple(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = pg_sql.SQL(
            """
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at
        FROM {table}
        WHERE app_name = %s
          AND user_id = %s
          AND content_text ILIKE %s
        ORDER BY timestamp DESC
        LIMIT %s
        """
        ).format(table=pg_sql.Identifier(self._memory_table))
        pattern = f"%{query}%"
        params: tuple[str, str, str, int] = (app_name, user_id, pattern, limit)
        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()
        return _rows_to_records(rows)

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        sql = pg_sql.SQL("DELETE FROM {table} WHERE session_id = %s").format(
            table=pg_sql.Identifier(self._memory_table)
        )

        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(sql, (session_id,))
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    async def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        sql = pg_sql.SQL(
            """
        DELETE FROM {table}
        WHERE inserted_at < CURRENT_TIMESTAMP - {interval}::interval
        """
        ).format(table=pg_sql.Identifier(self._memory_table), interval=pg_sql.Literal(f"{days} days"))

        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(sql)
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0


class PsycopgSyncADKMemoryStore(BaseSyncADKMemoryStore["PsycopgSyncConfig"]):
    """PostgreSQL ADK memory store using Psycopg3 sync driver."""

    __slots__ = ()

    def __init__(self, config: "PsycopgSyncConfig") -> None:
        """Initialize Psycopg sync memory store."""
        super().__init__(config)

    def _get_create_memory_table_sql(self) -> str:
        """Get PostgreSQL CREATE TABLE SQL for memory entries."""
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        fts_index = ""
        if self._use_fts:
            fts_index = f"""
        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_fts
            ON {self._memory_table} USING GIN (to_tsvector('english', content_text));
            """

        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_line},
            timestamp TIMESTAMPTZ NOT NULL,
            content_json JSONB NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json JSONB,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_app_user_time
            ON {self._memory_table}(app_name, user_id, timestamp DESC);

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_session
            ON {self._memory_table}(session_id);
        {fts_index}
        """

    def _get_drop_memory_table_sql(self) -> "list[str]":
        """Get PostgreSQL DROP TABLE SQL statements."""
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    def create_tables(self) -> None:
        """Create the memory table and indexes if they don't exist."""
        if not self._enabled:
            return

        with self._config.provide_session() as driver:
            driver.execute_script(self._get_create_memory_table_sql())

    def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        inserted_count = 0
        if self._owner_id_column_name:
            query = pg_sql.SQL("""
            INSERT INTO {table} (
                id, session_id, app_name, user_id, event_id, author,
                {owner_id_col}, timestamp, content_json, content_text,
                metadata_json, inserted_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (event_id) DO NOTHING
            """).format(
                table=pg_sql.Identifier(self._memory_table), owner_id_col=pg_sql.Identifier(self._owner_id_column_name)
            )
        else:
            query = pg_sql.SQL("""
            INSERT INTO {table} (
                id, session_id, app_name, user_id, event_id, author,
                timestamp, content_json, content_text, metadata_json, inserted_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (event_id) DO NOTHING
            """).format(table=pg_sql.Identifier(self._memory_table))

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            for entry in entries:
                if self._owner_id_column_name:
                    cur.execute(query, _build_insert_params_with_owner(entry, owner_id))
                else:
                    cur.execute(query, _build_insert_params(entry))
                if cur.rowcount and cur.rowcount > 0:
                    inserted_count += cur.rowcount

        return inserted_count

    def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        effective_limit = limit if limit is not None else self._max_results

        try:
            if self._use_fts:
                try:
                    return self._search_entries_fts(query, app_name, user_id, effective_limit)
                except Exception as exc:  # pragma: no cover - defensive fallback
                    logger.warning("FTS search failed; falling back to simple search: %s", exc)
            return self._search_entries_simple(query, app_name, user_id, effective_limit)
        except errors.UndefinedTable:
            return []

    def _search_entries_fts(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = pg_sql.SQL(
            """
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at,
               ts_rank(to_tsvector('english', content_text), plainto_tsquery('english', %s)) as rank
        FROM {table}
        WHERE app_name = %s
          AND user_id = %s
          AND to_tsvector('english', content_text) @@ plainto_tsquery('english', %s)
        ORDER BY rank DESC, timestamp DESC
        LIMIT %s
        """
        ).format(table=pg_sql.Identifier(self._memory_table))
        params: tuple[str, str, str, str, int] = (query, app_name, user_id, query, limit)
        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return _rows_to_records(rows)

    def _search_entries_simple(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = pg_sql.SQL(
            """
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at
        FROM {table}
        WHERE app_name = %s
          AND user_id = %s
          AND content_text ILIKE %s
        ORDER BY timestamp DESC
        LIMIT %s
        """
        ).format(table=pg_sql.Identifier(self._memory_table))
        pattern = f"%{query}%"
        params: tuple[str, str, str, int] = (app_name, user_id, pattern, limit)
        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return _rows_to_records(rows)

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        sql = pg_sql.SQL("DELETE FROM {table} WHERE session_id = %s").format(
            table=pg_sql.Identifier(self._memory_table)
        )

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql, (session_id,))
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        sql = pg_sql.SQL(
            """
        DELETE FROM {table}
        WHERE inserted_at < CURRENT_TIMESTAMP - {interval}::interval
        """
        ).format(table=pg_sql.Identifier(self._memory_table), interval=pg_sql.Literal(f"{days} days"))

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql)
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0


def _rows_to_records(rows: "list[Any]") -> "list[MemoryRecord]":
    return [
        {
            "id": row["id"],
            "session_id": row["session_id"],
            "app_name": row["app_name"],
            "user_id": row["user_id"],
            "event_id": row["event_id"],
            "author": row["author"],
            "timestamp": row["timestamp"],
            "content_json": row["content_json"],
            "content_text": row["content_text"],
            "metadata_json": row["metadata_json"],
            "inserted_at": row["inserted_at"],
        }
        for row in rows
    ]
