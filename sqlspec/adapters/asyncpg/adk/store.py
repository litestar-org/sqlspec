"""AsyncPG ADK store for Google Agent Development Kit session/event storage."""

from typing import TYPE_CHECKING, Any, Final, cast

import asyncpg

from sqlspec.config import AsyncConfigT
from sqlspec.extensions.adk import BaseAsyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore

if TYPE_CHECKING:
    from datetime import datetime

    from sqlspec.adapters.asyncpg.config import AsyncpgConfig
    from sqlspec.extensions.adk import MemoryRecord


__all__ = ("AsyncpgADKMemoryStore", "AsyncpgADKStore")

POSTGRES_TABLE_NOT_FOUND_ERROR: Final = "42P01"


class AsyncpgADKStore(BaseAsyncADKStore[AsyncConfigT]):
    """PostgreSQL ADK store base class for all PostgreSQL drivers.

    Implements session and event storage for Google Agent Development Kit
    using PostgreSQL via any PostgreSQL driver (AsyncPG, Psycopg, Psqlpy).
    All drivers share the same SQL dialect and parameter style ($1, $2, etc).

    Provides:
    - Session state management with JSONB storage and merge operations
    - Event history tracking with BYTEA-serialized actions
    - Microsecond-precision timestamps with TIMESTAMPTZ
    - Foreign key constraints with cascade delete
    - Efficient upserts using ON CONFLICT
    - GIN indexes for JSONB queries
    - HOT updates with FILLFACTOR 80
    - Optional user FK column for multi-tenancy

    Args:
        config: PostgreSQL database config with extension_config["adk"] settings.

    Example:
        from sqlspec.adapters.asyncpg import AsyncpgConfig
        from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore

        config = AsyncpgConfig(
            connection_config={"dsn": "postgresql://..."},
            extension_config={
                "adk": {
                    "session_table": "my_sessions",
                    "events_table": "my_events",
                    "owner_id_column": "tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"
                }
            }
        )
        store = AsyncpgADKStore(config)
        await store.ensure_tables()

    Notes:
        - PostgreSQL JSONB type used for state (more efficient than JSON)
        - AsyncPG automatically converts Python dicts to/from JSONB (no manual serialization)
        - TIMESTAMPTZ provides timezone-aware microsecond precision
        - State merging uses `state || $1::jsonb` operator for efficiency
        - BYTEA for pre-serialized actions from Google ADK (not pickled here)
        - GIN index on state for JSONB queries (partial index)
        - FILLFACTOR 80 leaves space for HOT updates
        - Generic over PostgresConfigT to support all PostgreSQL drivers
        - Owner ID column enables multi-tenant isolation with referential integrity
        - Configuration is read from config.extension_config["adk"]
    """

    __slots__ = ()

    def __init__(self, config: AsyncConfigT) -> None:
        """Initialize AsyncPG ADK store.

        Args:
            config: PostgreSQL database config.

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
            - Optional owner ID column for multi-tenancy or owner references
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
        async with self.config.provide_session() as driver:
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
            State is passed as dict and asyncpg converts to JSONB automatically.
            If owner_id_column is configured, owner_id value must be provided.
        """
        async with self.config.provide_connection() as conn:
            if self._owner_id_column_name:
                sql = f"""
                INSERT INTO {self._session_table}
                (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
                VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
                await conn.execute(sql, session_id, app_name, user_id, owner_id, state)
            else:
                sql = f"""
                INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
                VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
                await conn.execute(sql, session_id, app_name, user_id, state)

        return await self.get_session(session_id)  # type: ignore[return-value]

    async def get_session(self, session_id: str) -> "SessionRecord | None":
        """Get session by ID.

        Args:
            session_id: Session identifier.

        Returns:
            Session record or None if not found.

        Notes:
            PostgreSQL returns datetime objects for TIMESTAMPTZ columns.
            JSONB is automatically parsed by asyncpg.
        """
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = $1
        """

        try:
            async with self.config.provide_connection() as conn:
                row = await conn.fetchrow(sql, session_id)

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
        except asyncpg.exceptions.UndefinedTableError:
            return None

    async def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state.

        Args:
            session_id: Session identifier.
            state: New state dictionary (replaces existing state).

        Notes:
            This replaces the entire state dictionary.
            Uses CURRENT_TIMESTAMP for update_time.
        """
        sql = f"""
        UPDATE {self._session_table}
        SET state = $1, update_time = CURRENT_TIMESTAMP
        WHERE id = $2
        """

        async with self.config.provide_connection() as conn:
            await conn.execute(sql, state, session_id)

    async def delete_session(self, session_id: str) -> None:
        """Delete session and all associated events (cascade).

        Args:
            session_id: Session identifier.

        Notes:
            Foreign key constraint ensures events are cascade-deleted.
        """
        sql = f"DELETE FROM {self._session_table} WHERE id = $1"

        async with self.config.provide_connection() as conn:
            await conn.execute(sql, session_id)

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
            WHERE app_name = $1
            ORDER BY update_time DESC
            """
            params = [app_name]
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = $1 AND user_id = $2
            ORDER BY update_time DESC
            """
            params = [app_name, user_id]

        try:
            async with self.config.provide_connection() as conn:
                rows = await conn.fetch(sql, *params)

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
        except asyncpg.exceptions.UndefinedTableError:
            return []

    async def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session.

        Args:
            event_record: Event record to store.

        Notes:
            Uses CURRENT_TIMESTAMP for timestamp if not provided.
            JSONB fields are passed as dicts and asyncpg converts automatically.
        """
        content_json = event_record.get("content")
        grounding_metadata_json = event_record.get("grounding_metadata")
        custom_metadata_json = event_record.get("custom_metadata")

        sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, app_name, user_id, invocation_id, author, actions,
            long_running_tool_ids_json, branch, timestamp, content,
            grounding_metadata, custom_metadata, partial, turn_complete,
            interrupted, error_code, error_message
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
        )
        """

        async with self.config.provide_connection() as conn:
            await conn.execute(
                sql,
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
                content_json,
                grounding_metadata_json,
                custom_metadata_json,
                event_record.get("partial"),
                event_record.get("turn_complete"),
                event_record.get("interrupted"),
                event_record.get("error_code"),
                event_record.get("error_message"),
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
            Parses JSONB fields and converts BYTEA actions to bytes.
        """
        where_clauses = ["session_id = $1"]
        params: list[Any] = [session_id]

        if after_timestamp is not None:
            where_clauses.append(f"timestamp > ${len(params) + 1}")
            params.append(after_timestamp)

        where_clause = " AND ".join(where_clauses)
        limit_clause = f" LIMIT ${len(params) + 1}" if limit else ""
        if limit:
            params.append(limit)

        sql = f"""
        SELECT id, session_id, app_name, user_id, invocation_id, author, actions,
               long_running_tool_ids_json, branch, timestamp, content,
               grounding_metadata, custom_metadata, partial, turn_complete,
               interrupted, error_code, error_message
        FROM {self._events_table}
        WHERE {where_clause}
        ORDER BY timestamp ASC{limit_clause}
        """

        try:
            async with self.config.provide_connection() as conn:
                rows = await conn.fetch(sql, *params)

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
        except asyncpg.exceptions.UndefinedTableError:
            return []


class AsyncpgADKMemoryStore(BaseAsyncADKMemoryStore["AsyncpgConfig"]):
    """PostgreSQL ADK memory store using asyncpg driver.

    Implements memory entry storage for Google Agent Development Kit
    using PostgreSQL via the asyncpg driver. Provides:
    - Session memory storage with JSONB for content and metadata
    - Full-text search using to_tsvector/to_tsquery (postgres_fts strategy)
    - Simple ILIKE search fallback (simple strategy)
    - TIMESTAMPTZ for precise timestamp storage
    - Deduplication via event_id unique constraint
    - Efficient upserts using ON CONFLICT DO NOTHING

    Args:
        config: AsyncpgConfig with extension_config["adk"] settings.

    Example:
        from sqlspec.adapters.asyncpg import AsyncpgConfig
        from sqlspec.adapters.asyncpg.adk.store import AsyncpgADKMemoryStore

        config = AsyncpgConfig(
            connection_config={"dsn": "postgresql://..."},
            extension_config={
                "adk": {
                    "memory_table": "adk_memory_entries",
                    "memory_use_fts": True,
                    "memory_max_results": 20,
                }
            }
        )
        store = AsyncpgADKMemoryStore(config)
        await store.ensure_tables()

    Notes:
        - JSONB type for content_json and metadata_json
        - TIMESTAMPTZ with microsecond precision
        - GIN index on content_text tsvector for FTS queries
        - Composite index on (app_name, user_id) for filtering
        - event_id UNIQUE constraint for deduplication
        - Configuration is read from config.extension_config["adk"]
    """

    __slots__ = ()

    def __init__(self, config: "AsyncpgConfig") -> None:
        """Initialize AsyncPG ADK memory store.

        Args:
            config: AsyncpgConfig instance.

        Notes:
            Configuration is read from config.extension_config["adk"]:
            - memory_table: Memory table name (default: "adk_memory_entries")
            - memory_use_fts: Enable full-text search when supported (default: False)
            - memory_max_results: Max search results (default: 20)
            - owner_id_column: Optional owner FK column DDL (default: None)
            - enable_memory: Whether memory is enabled (default: True)
        """
        super().__init__(config)

    async def _get_create_memory_table_sql(self) -> str:
        """Get PostgreSQL CREATE TABLE SQL for memory entries.

        Returns:
            SQL statement to create memory table with indexes.

        Notes:
            - VARCHAR(128) for IDs and names
            - JSONB for content and metadata storage
            - TIMESTAMPTZ with microsecond precision
            - UNIQUE constraint on event_id for deduplication
            - Composite index on (app_name, user_id, timestamp DESC)
            - GIN index on content_text tsvector for FTS
            - Optional owner ID column for multi-tenancy
        """
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
        """Get PostgreSQL DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop the memory table.

        Notes:
            PostgreSQL automatically drops indexes when dropping tables.
        """
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

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

        Uses UPSERT pattern (ON CONFLICT DO NOTHING) to skip duplicates
        based on event_id unique constraint.

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
        async with self._config.provide_connection() as conn:
            for entry in entries:
                if self._owner_id_column_name:
                    sql = f"""
                    INSERT INTO {self._memory_table}
                    (id, session_id, app_name, user_id, event_id, author,
                     {self._owner_id_column_name}, timestamp, content_json,
                     content_text, metadata_json, inserted_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (event_id) DO NOTHING
                    """
                    result = await conn.execute(
                        sql,
                        entry["id"],
                        entry["session_id"],
                        entry["app_name"],
                        entry["user_id"],
                        entry["event_id"],
                        entry["author"],
                        owner_id,
                        entry["timestamp"],
                        entry["content_json"],
                        entry["content_text"],
                        entry["metadata_json"],
                        entry["inserted_at"],
                    )
                else:
                    sql = f"""
                    INSERT INTO {self._memory_table}
                    (id, session_id, app_name, user_id, event_id, author,
                     timestamp, content_json, content_text, metadata_json, inserted_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT (event_id) DO NOTHING
                    """
                    result = await conn.execute(
                        sql,
                        entry["id"],
                        entry["session_id"],
                        entry["app_name"],
                        entry["user_id"],
                        entry["event_id"],
                        entry["author"],
                        entry["timestamp"],
                        entry["content_json"],
                        entry["content_text"],
                        entry["metadata_json"],
                        entry["inserted_at"],
                    )
                try:
                    inserted_count += int(result.split(" ")[1])
                except (IndexError, ValueError):
                    continue

        return inserted_count

    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query.

        Uses the configured search strategy (simple ILIKE or FTS).

        Args:
            query: Text query to search for.
            app_name: Application name to filter by.
            user_id: User ID to filter by.
            limit: Maximum number of results (defaults to max_results config).

        Returns:
            List of memory records.
        """
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not query:
            return []

        limit_value = limit or self._max_results
        if self._use_fts:
            sql = f"""
            SELECT * FROM {self._memory_table}
            WHERE app_name = $1 AND user_id = $2
              AND to_tsvector('english', content_text) @@ plainto_tsquery('english', $3)
            ORDER BY timestamp DESC
            LIMIT $4
            """
            params = (app_name, user_id, query, limit_value)
        else:
            sql = f"""
            SELECT * FROM {self._memory_table}
            WHERE app_name = $1 AND user_id = $2 AND content_text ILIKE $3
            ORDER BY timestamp DESC
            LIMIT $4
            """
            params = (app_name, user_id, f"%{query}%", limit_value)

        async with self._config.provide_connection() as conn:
            rows = await conn.fetch(sql, *params)
        return [cast("MemoryRecord", dict(row)) for row in rows]

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"DELETE FROM {self._memory_table} WHERE session_id = $1"
        async with self._config.provide_connection() as conn:
            result = await conn.execute(sql, session_id)
        try:
            return int(result.split(" ")[1])
        except (IndexError, ValueError):
            return 0

    async def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < (CURRENT_TIMESTAMP - ($1::int * INTERVAL '1 day'))
        """
        async with self._config.provide_connection() as conn:
            result = await conn.execute(sql, days)
        try:
            return int(result.split(" ")[1])
        except (IndexError, ValueError):
            return 0
