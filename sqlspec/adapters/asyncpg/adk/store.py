"""AsyncPG ADK store for Google Agent Development Kit session/event storage."""

from typing import TYPE_CHECKING, Any, Final, cast

import asyncpg
from typing_extensions import NotRequired

from sqlspec.config import ADKConfig, AsyncConfigT
from sqlspec.extensions.adk import BaseAsyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore

if TYPE_CHECKING:
    from datetime import datetime, timedelta

    from sqlspec.adapters.asyncpg.config import AsyncpgConfig
    from sqlspec.extensions.adk import MemoryRecord


__all__ = ("AsyncpgADKConfig", "AsyncpgADKMemoryStore", "AsyncpgADKStore")

POSTGRES_TABLE_NOT_FOUND_ERROR: Final = "42P01"


class AsyncpgADKConfig(ADKConfig):
    """Asyncpg-specific ADK extension settings.

    Use these keys inside ``extension_config["adk"]`` with the asyncpg ADK store.
    """

    enable_event_generated_columns: NotRequired[bool]
    """Create PostgreSQL generated columns and indexes for common ADK event JSON paths."""

    enable_covering_indexes: NotRequired[bool]
    """Add PostgreSQL INCLUDE columns to ADK event replay indexes."""


class AsyncpgADKStore(BaseAsyncADKStore[AsyncConfigT]):
    """PostgreSQL ADK store using asyncpg driver.

    Implements session and event storage for Google Agent Development Kit
    using PostgreSQL via asyncpg. Events are stored as a single JSONB blob
    (``event_data``) alongside indexed scalar columns for efficient querying.

    Provides:
        - Session state management with JSONB storage
        - Full-fidelity event storage via ``event_data`` JSONB column
        - Atomic ``append_event_and_update_state`` for durable session mutations
        - Microsecond-precision timestamps with TIMESTAMPTZ
        - Foreign key constraints with cascade delete
        - GIN indexes for JSONB queries
        - HOT updates with FILLFACTOR 80
        - Optional owner ID column for multi-tenancy

    Args:
        config: PostgreSQL database config with extension_config["adk"] settings.
    """

    __slots__ = ()

    def __init__(self, config: AsyncConfigT) -> None:
        super().__init__(config)

    async def create_tables(self) -> None:
        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._get_create_sessions_table_sql())
            await driver.execute_script(await self._get_create_events_table_sql())
            await driver.execute_script(await self._get_create_app_states_table_sql())
            await driver.execute_script(await self._get_create_user_states_table_sql())
            await driver.execute_script(await self._get_create_metadata_table_sql())
            await driver.execute_script(await self._get_seed_metadata_sql())

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        async with self._config.provide_connection() as conn:
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

        result = await self.get_session(app_name, user_id, session_id)
        if result is None:
            msg = "Failed to fetch created session"
            raise RuntimeError(msg)
        return result

    async def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
            sql = f"""
            UPDATE {self._session_table}
            SET update_time = CURRENT_TIMESTAMP
            WHERE app_name = $1 AND user_id = $2 AND id = $3
            RETURNING id, app_name, user_id, state, create_time, update_time
            """
            params = [app_name, user_id, session_id]
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = $1 AND user_id = $2 AND id = $3
            """
            params = [app_name, user_id, session_id]

        try:
            async with self._config.provide_connection() as conn:
                row = await conn.fetchrow(sql, *params)

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

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        sql = f"""
        UPDATE {self._session_table}
        SET state = $1, update_time = CURRENT_TIMESTAMP
        WHERE app_name = $2 AND user_id = $3 AND id = $4
        """

        async with self._config.provide_connection() as conn:
            await conn.execute(sql, state, app_name, user_id, session_id)

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        sql = f"DELETE FROM {self._session_table} WHERE app_name = $1 AND user_id = $2 AND id = $3"

        async with self._config.provide_connection() as conn:
            await conn.execute(sql, app_name, user_id, session_id)

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
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
            async with self._config.provide_connection() as conn:
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
        sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, invocation_id, timestamp, event_data
        ) VALUES ($1, $2, $3, $4, $5)
        """

        async with self._config.provide_connection() as conn:
            await conn.execute(
                sql,
                event_record["id"],
                event_record["session_id"],
                event_record["invocation_id"],
                event_record["timestamp"],
                event_record["event_data"],
            )

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
        insert_sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, invocation_id, timestamp, event_data
        ) VALUES ($1, $2, $3, $4, $5)
        """
        update_sql = f"""
        UPDATE {self._session_table}
        SET state = $1, update_time = CURRENT_TIMESTAMP
        WHERE app_name = $2 AND user_id = $3 AND id = $4
        RETURNING id, app_name, user_id, state, create_time, update_time
        """
        app_upsert_sql = f"""
        INSERT INTO {self._app_state_table} (app_name, state, update_time)
        VALUES ($1, $2, CURRENT_TIMESTAMP)
        ON CONFLICT (app_name) DO UPDATE SET
            state = EXCLUDED.state,
            update_time = CURRENT_TIMESTAMP
        """
        user_upsert_sql = f"""
        INSERT INTO {self._user_state_table} (app_name, user_id, state, update_time)
        VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
        ON CONFLICT (app_name, user_id) DO UPDATE SET
            state = EXCLUDED.state,
            update_time = CURRENT_TIMESTAMP
        """

        async with self._config.provide_connection() as conn, conn.transaction():
            await conn.execute(
                insert_sql,
                event_record["id"],
                event_record["session_id"],
                event_record["invocation_id"],
                event_record["timestamp"],
                event_record["event_data"],
            )
            row = await conn.fetchrow(update_sql, state, app_name, user_id, session_id)
            if row is None:
                msg = f"Session {session_id} not found during append_event_and_update_state."
                raise ValueError(msg)
            if app_state is not None:
                await conn.execute(app_upsert_sql, app_name, app_state)
            if user_state is not None:
                await conn.execute(user_upsert_sql, app_name, user_id, user_state)

        return SessionRecord(
            id=row["id"],
            app_name=row["app_name"],
            user_id=row["user_id"],
            state=row["state"],
            create_time=row["create_time"],
            update_time=row["update_time"],
        )

    async def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "list[EventRecord]":
        if limit == 0:
            return []

        where_clauses = ["s.app_name = $1", "s.user_id = $2", "e.session_id = $3"]
        params: list[Any] = [app_name, user_id, session_id]

        if after_timestamp is not None:
            where_clauses.append(f"e.timestamp > ${len(params) + 1}")
            params.append(after_timestamp)

        where_clause = " AND ".join(where_clauses)
        limit_clause = f" LIMIT ${len(params) + 1}" if limit is not None else ""
        if limit is not None:
            params.append(limit)

        sql = f"""
        SELECT e.id, e.session_id, e.invocation_id, e.timestamp, e.event_data, s.app_name, s.user_id
        FROM {self._events_table} e
        JOIN {self._session_table} s ON e.session_id = s.id
        WHERE {where_clause}
        ORDER BY e.timestamp ASC{limit_clause}
        """

        try:
            async with self._config.provide_connection() as conn:
                rows = await conn.fetch(sql, *params)

                return [
                    EventRecord(
                        id=row["id"],
                        session_id=row["session_id"],
                        invocation_id=row["invocation_id"],
                        timestamp=row["timestamp"],
                        event_data=row["event_data"],
                        app_name=row["app_name"],
                        user_id=row["user_id"],
                    )
                    for row in rows
                ]
        except asyncpg.exceptions.UndefinedTableError:
            return []

    async def delete_expired_events(self, before: "datetime") -> int:
        sql = f"DELETE FROM {self._events_table} WHERE timestamp < $1"

        try:
            async with self._config.provide_connection() as conn:
                result = await conn.execute(sql, before)
                return int(result.split()[-1]) if result else 0
        except asyncpg.exceptions.UndefinedTableError:
            return 0

    async def delete_idle_sessions(self, updated_before: "datetime") -> int:
        sql = f"DELETE FROM {self._session_table} WHERE update_time < $1"

        try:
            async with self._config.provide_connection() as conn:
                result = await conn.execute(sql, updated_before)
                return int(result.split()[-1]) if result else 0
        except asyncpg.exceptions.UndefinedTableError:
            return 0

    async def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        sql = f"SELECT state FROM {self._app_state_table} WHERE app_name = $1"

        try:
            async with self._config.provide_connection() as conn:
                row = await conn.fetchrow(sql, app_name)
                return row["state"] if row is not None else None
        except asyncpg.exceptions.UndefinedTableError:
            return None

    async def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        sql = f"SELECT state FROM {self._user_state_table} WHERE app_name = $1 AND user_id = $2"

        try:
            async with self._config.provide_connection() as conn:
                row = await conn.fetchrow(sql, app_name, user_id)
                return row["state"] if row is not None else None
        except asyncpg.exceptions.UndefinedTableError:
            return None

    async def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        sql = f"""
        INSERT INTO {self._app_state_table} (app_name, state, update_time)
        VALUES ($1, $2, CURRENT_TIMESTAMP)
        ON CONFLICT (app_name) DO UPDATE SET
            state = EXCLUDED.state,
            update_time = CURRENT_TIMESTAMP
        """

        async with self._config.provide_connection() as conn:
            await conn.execute(sql, app_name, state)

    async def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        sql = f"""
        INSERT INTO {self._user_state_table} (app_name, user_id, state, update_time)
        VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
        ON CONFLICT (app_name, user_id) DO UPDATE SET
            state = EXCLUDED.state,
            update_time = CURRENT_TIMESTAMP
        """

        async with self._config.provide_connection() as conn:
            await conn.execute(sql, app_name, user_id, state)

    async def get_metadata(self, key: str) -> "str | None":
        sql = f"SELECT value FROM {self._metadata_table} WHERE key = $1"

        try:
            async with self._config.provide_connection() as conn:
                row = await conn.fetchrow(sql, key)
                return row["value"] if row is not None else None
        except asyncpg.exceptions.UndefinedTableError:
            return None

    async def set_metadata(self, key: str, value: str) -> None:
        sql = f"""
        INSERT INTO {self._metadata_table} (key, value)
        VALUES ($1, $2)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """

        async with self._config.provide_connection() as conn:
            await conn.execute(sql, key, value)

    async def _get_create_sessions_table_sql(self) -> str:
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
        adk_config = _get_asyncpg_adk_config(self._config)
        generated_columns = ""
        generated_indexes = ""
        if adk_config.get("enable_event_generated_columns", False):
            generated_columns = """,
            author_gc VARCHAR(256) GENERATED ALWAYS AS (event_data->>'author') STORED,
            node_path_gc TEXT GENERATED ALWAYS AS (event_data->'node_info'->>'path') STORED"""
            generated_indexes = f"""

        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_author_gc
            ON {self._events_table}(session_id, author_gc, timestamp ASC);

        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_node_path_gc
            ON {self._events_table}(session_id, node_path_gc, timestamp ASC);
        """

        covering_columns = ""
        if adk_config.get("enable_covering_indexes", False):
            covering_columns = " INCLUDE (invocation_id)"

        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256),
            timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_data JSONB NOT NULL{generated_columns},
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        ) WITH (fillfactor = 80);

        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session
            ON {self._events_table}(session_id, timestamp ASC){covering_columns};
        {generated_indexes}
        """

    async def _get_create_app_states_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._app_state_table} (
            app_name VARCHAR(128) PRIMARY KEY,
            state JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) WITH (fillfactor = 80);
        """

    async def _get_create_user_states_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._user_state_table} (
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            state JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (app_name, user_id)
        ) WITH (fillfactor = 80);
        """

    async def _get_create_metadata_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._metadata_table} (
            key VARCHAR(128) PRIMARY KEY,
            value VARCHAR(512) NOT NULL
        );
        """

    async def _get_seed_metadata_sql(self) -> str:
        return f"""
        INSERT INTO {self._metadata_table} (key, value)
        VALUES ('schema_version', '1')
        ON CONFLICT (key) DO NOTHING
        """

    def _get_drop_app_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._app_state_table}"

    def _get_drop_user_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._user_state_table}"

    def _get_drop_metadata_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._metadata_table}"

    def _get_drop_tables_sql(self) -> "list[str]":
        return [
            self._get_drop_metadata_table_sql(),
            self._get_drop_user_states_table_sql(),
            self._get_drop_app_states_table_sql(),
            f"DROP TABLE IF EXISTS {self._events_table}",
            f"DROP TABLE IF EXISTS {self._session_table}",
        ]


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
    """

    __slots__ = ()

    def __init__(self, config: "AsyncpgConfig") -> None:
        super().__init__(config)

    async def _get_create_memory_table_sql(self) -> str:
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
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    async def create_tables(self) -> None:
        if not self._enabled:
            return

        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._get_create_memory_table_sql())

    async def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
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
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not query:
            return []

        from typing import cast

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


def _get_asyncpg_adk_config(config: Any) -> AsyncpgADKConfig:
    """Return asyncpg ADK extension settings from ``extension_config["adk"]``."""

    extension_config = getattr(config, "extension_config", {})
    if not isinstance(extension_config, dict):
        return {}
    adk_config = extension_config.get("adk", {})
    if not isinstance(adk_config, dict):
        return {}
    return cast("AsyncpgADKConfig", adk_config)
