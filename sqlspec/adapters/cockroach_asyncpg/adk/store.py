"""CockroachDB ADK store for Google Agent Development Kit session/event storage (asyncpg)."""

from typing import TYPE_CHECKING, Any, cast

from sqlspec.extensions.adk import BaseAsyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from datetime import datetime

    from sqlspec.adapters.cockroach_asyncpg.config import CockroachAsyncpgConfig
    from sqlspec.extensions.adk import MemoryRecord


__all__ = ("CockroachAsyncpgADKMemoryStore", "CockroachAsyncpgADKStore")

logger = get_logger("sqlspec.adapters.cockroach_asyncpg.adk.store")


class CockroachAsyncpgADKStore(BaseAsyncADKStore["CockroachAsyncpgConfig"]):
    """CockroachDB ADK store using asyncpg driver.

    Implements session and event storage for Google Agent Development Kit
    using CockroachDB via asyncpg in PostgreSQL compatibility mode.
    Events are stored as a single JSONB blob (``event_json``) alongside
    indexed scalar columns for efficient querying.

    CockroachDB-specific differences from native PostgreSQL:
    - No FILLFACTOR (CockroachDB uses different storage engine)
    - No BRIN indexes (different physical storage layout)
    - GIN/Inverted indexes on JSONB are fully supported (v23.1+)
    - Native tsvector/tsquery FTS with GIN is supported (v23.1+)
    """

    __slots__ = ()

    def __init__(self, config: "CockroachAsyncpgConfig") -> None:
        super().__init__(config)

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
        );

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user
            ON {self._session_table}(app_name, user_id);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time
            ON {self._session_table}(update_time DESC);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_state
            ON {self._session_table} USING GIN (state)
            WHERE state != '{{}}'::jsonb;
        """

    async def _get_create_events_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            session_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256) NOT NULL,
            author VARCHAR(256) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_json JSONB NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session
            ON {self._events_table}(session_id, timestamp ASC);

        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_event_json
            ON {self._events_table} USING GIN (event_json);
        """

    def _get_drop_tables_sql(self) -> "list[str]":
        return [f"DROP TABLE IF EXISTS {self._events_table}", f"DROP TABLE IF EXISTS {self._session_table}"]

    async def create_tables(self) -> None:
        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._get_create_sessions_table_sql())
            await driver.execute_script(await self._get_create_events_table_sql())

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        params: tuple[Any, ...]
        if self._owner_id_column_name:
            sql = f"""
            INSERT INTO {self._session_table} (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
            VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
            params = (session_id, app_name, user_id, owner_id, state)
        else:
            sql = f"""
            INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
            VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
            params = (session_id, app_name, user_id, state)

        async with self._config.provide_connection() as conn:
            await conn.execute(sql, *params)

        result = await self.get_session(session_id)
        if result is None:
            msg = "Session creation failed"
            raise RuntimeError(msg)
        return result

    async def get_session(self, session_id: str) -> "SessionRecord | None":
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = $1
        """

        async with self._config.provide_connection() as conn:
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

    async def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        sql = f"""
        UPDATE {self._session_table}
        SET state = $1, update_time = CURRENT_TIMESTAMP
        WHERE id = $2
        """

        async with self._config.provide_connection() as conn:
            await conn.execute(sql, state, session_id)

    async def delete_session(self, session_id: str) -> None:
        sql = f"DELETE FROM {self._session_table} WHERE id = $1"

        async with self._config.provide_connection() as conn:
            await conn.execute(sql, session_id)

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        if user_id is None:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = $1
            ORDER BY update_time DESC
            """
            params: tuple[Any, ...] = (app_name,)
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = $1 AND user_id = $2
            ORDER BY update_time DESC
            """
            params = (app_name, user_id)

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

    async def append_event(self, event_record: EventRecord) -> None:
        sql = f"""
        INSERT INTO {self._events_table} (
            session_id, invocation_id, author, timestamp, event_json
        ) VALUES ($1, $2, $3, $4, $5)
        """

        async with self._config.provide_connection() as conn:
            await conn.execute(
                sql,
                event_record["session_id"],
                event_record["invocation_id"],
                event_record["author"],
                event_record["timestamp"],
                event_record["event_json"],
            )

    async def append_event_and_update_state(
        self, event_record: EventRecord, session_id: str, state: "dict[str, Any]"
    ) -> None:
        insert_sql = f"""
        INSERT INTO {self._events_table} (
            session_id, invocation_id, author, timestamp, event_json
        ) VALUES ($1, $2, $3, $4, $5)
        """
        update_sql = f"""
        UPDATE {self._session_table}
        SET state = $1, update_time = CURRENT_TIMESTAMP
        WHERE id = $2
        """

        async with self._config.provide_connection() as conn, conn.transaction():
                await conn.execute(
                    insert_sql,
                    event_record["session_id"],
                    event_record["invocation_id"],
                    event_record["author"],
                    event_record["timestamp"],
                    event_record["event_json"],
                )
                await conn.execute(update_sql, state, session_id)

    async def get_events(
        self, session_id: str, after_timestamp: "datetime | None" = None, limit: "int | None" = None
    ) -> "list[EventRecord]":
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
        SELECT session_id, invocation_id, author, timestamp, event_json
        FROM {self._events_table}
        WHERE {where_clause}
        ORDER BY timestamp ASC{limit_clause}
        """

        async with self._config.provide_connection() as conn:
            rows = await conn.fetch(sql, *params)

        return [
            EventRecord(
                session_id=row["session_id"],
                invocation_id=row["invocation_id"],
                author=row["author"],
                timestamp=row["timestamp"],
                event_json=row["event_json"],
            )
            for row in rows
        ]


class CockroachAsyncpgADKMemoryStore(BaseAsyncADKMemoryStore["CockroachAsyncpgConfig"]):
    """CockroachDB ADK memory store using asyncpg driver."""

    __slots__ = ()

    def __init__(self, config: "CockroachAsyncpgConfig") -> None:
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

                if result and result.split()[-1].isdigit() and int(result.split()[-1]) > 0:
                    inserted_count += 1

        return inserted_count

    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not query:
            return []

        effective_limit = limit if limit is not None else self._max_results

        if self._use_fts:
            sql = f"""
            SELECT * FROM {self._memory_table}
            WHERE app_name = $1 AND user_id = $2
              AND to_tsvector('english', content_text) @@ plainto_tsquery('english', $3)
            ORDER BY timestamp DESC
            LIMIT $4
            """
            params: tuple[Any, ...] = (app_name, user_id, query, effective_limit)
        else:
            sql = f"""
            SELECT * FROM {self._memory_table}
            WHERE app_name = $1 AND user_id = $2 AND content_text ILIKE $3
            ORDER BY timestamp DESC
            LIMIT $4
            """
            params = (app_name, user_id, f"%{query}%", effective_limit)

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
            return int(result.split()[-1]) if result else 0

    async def delete_entries_older_than(self, days: int) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < (CURRENT_TIMESTAMP - INTERVAL '{days} days')
        """
        async with self._config.provide_connection() as conn:
            result = await conn.execute(sql)
            return int(result.split()[-1]) if result else 0
