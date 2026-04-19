"""Psqlpy ADK store for Google Agent Development Kit session/event storage."""

import re
from typing import TYPE_CHECKING, Any, Final

from sqlspec.adapters.psqlpy._typing import PsqlpyDatabaseError
from sqlspec.extensions.adk import BaseAsyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import has_query_result_metadata

if TYPE_CHECKING:
    from datetime import datetime

    from sqlspec.adapters.psqlpy.config import PsqlpyConfig
    from sqlspec.extensions.adk import MemoryRecord


__all__ = ("PsqlpyADKMemoryStore", "PsqlpyADKStore")

logger = get_logger("sqlspec.adapters.psqlpy.adk.store")

POSTGRES_TABLE_NOT_FOUND_SQLSTATE: Final = "42P01"


class PsqlpyADKStore(BaseAsyncADKStore["PsqlpyConfig"]):
    """PostgreSQL ADK store using Psqlpy driver.

    Implements session and event storage for Google Agent Development Kit
    using PostgreSQL via the high-performance Rust-based psqlpy driver.
    Events are stored as a single JSONB blob (``event_json``) alongside
    indexed scalar columns for efficient querying.

    Provides:
    - Session state management with JSONB storage
    - Full-fidelity event storage via ``event_json`` JSONB column
    - Atomic ``append_event_and_update_state`` for durable session mutations
    - Microsecond-precision timestamps with TIMESTAMPTZ
    - Foreign key constraints with cascade delete
    - GIN indexes for JSONB queries
    - HOT updates with FILLFACTOR 80

    Args:
        config: PsqlpyConfig with extension_config["adk"] settings.
    """

    __slots__ = ()

    def __init__(self, config: "PsqlpyConfig") -> None:
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
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            session_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256) NOT NULL,
            author VARCHAR(256) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_json JSONB NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        ) WITH (fillfactor = 80);

        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session
            ON {self._events_table}(session_id, timestamp ASC);
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
        async with self._config.provide_connection() as conn:  # pyright: ignore[reportAttributeAccessIssue]
            if self._owner_id_column_name:
                sql = f"""
                INSERT INTO {self._session_table}
                (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
                VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
                await conn.execute(sql, [session_id, app_name, user_id, owner_id, state])
            else:
                sql = f"""
                INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
                VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
                await conn.execute(sql, [session_id, app_name, user_id, state])

        return await self.get_session(session_id)  # type: ignore[return-value]

    async def get_session(self, session_id: str) -> "SessionRecord | None":
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = $1
        """

        try:
            async with self._config.provide_connection() as conn:  # pyright: ignore[reportAttributeAccessIssue]
                result = await conn.fetch(sql, [session_id])
                rows: list[dict[str, Any]] = result.result() if result else []

                if not rows:
                    return None

                row = rows[0]
                return SessionRecord(
                    id=row["id"],
                    app_name=row["app_name"],
                    user_id=row["user_id"],
                    state=row["state"],
                    create_time=row["create_time"],
                    update_time=row["update_time"],
                )
        except PsqlpyDatabaseError as e:
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "relation" in error_msg:
                return None
            raise

    async def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        sql = f"""
        UPDATE {self._session_table}
        SET state = $1, update_time = CURRENT_TIMESTAMP
        WHERE id = $2
        """

        async with self._config.provide_connection() as conn:  # pyright: ignore[reportAttributeAccessIssue]
            await conn.execute(sql, [state, session_id])

    async def delete_session(self, session_id: str) -> None:
        sql = f"DELETE FROM {self._session_table} WHERE id = $1"

        async with self._config.provide_connection() as conn:  # pyright: ignore[reportAttributeAccessIssue]
            await conn.execute(sql, [session_id])

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
            async with self._config.provide_connection() as conn:  # pyright: ignore[reportAttributeAccessIssue]
                result = await conn.fetch(sql, params)
                rows: list[dict[str, Any]] = result.result() if result else []

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
        except PsqlpyDatabaseError as e:
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "relation" in error_msg:
                return []
            raise

    async def append_event(self, event_record: EventRecord) -> None:
        sql = f"""
        INSERT INTO {self._events_table} (
            session_id, invocation_id, author, timestamp, event_json
        ) VALUES ($1, $2, $3, $4, $5)
        """

        async with self._config.provide_connection() as conn:  # pyright: ignore[reportAttributeAccessIssue]
            await conn.execute(
                sql,
                [
                    event_record["session_id"],
                    event_record["invocation_id"],
                    event_record["author"],
                    event_record["timestamp"],
                    event_record["event_json"],
                ],
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

        async with self._config.provide_connection() as conn:  # pyright: ignore[reportAttributeAccessIssue]
            await conn.execute(
                insert_sql,
                [
                    event_record["session_id"],
                    event_record["invocation_id"],
                    event_record["author"],
                    event_record["timestamp"],
                    event_record["event_json"],
                ],
            )
            await conn.execute(update_sql, [state, session_id])

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

        try:
            async with self._config.provide_connection() as conn:  # pyright: ignore[reportAttributeAccessIssue]
                result = await conn.fetch(sql, params)
                rows: list[dict[str, Any]] = result.result() if result else []

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
        except PsqlpyDatabaseError as e:
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "relation" in error_msg:
                return []
            raise


PSQLPY_STATUS_REGEX: Final[re.Pattern[str]] = re.compile(r"^([A-Z]+)(?:\s+(\d+))?\s+(\d+)$", re.IGNORECASE)


class PsqlpyADKMemoryStore(BaseAsyncADKMemoryStore["PsqlpyConfig"]):
    """PostgreSQL ADK memory store using Psqlpy driver."""

    __slots__ = ()

    def __init__(self, config: "PsqlpyConfig") -> None:
        """Initialize Psqlpy memory store."""
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
            sql = f"""
            INSERT INTO {self._memory_table} (
                id, session_id, app_name, user_id, event_id, author,
                {self._owner_id_column_name}, timestamp, content_json,
                content_text, metadata_json, inserted_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
            )
            ON CONFLICT (event_id) DO NOTHING
            """
        else:
            sql = f"""
            INSERT INTO {self._memory_table} (
                id, session_id, app_name, user_id, event_id, author,
                timestamp, content_json, content_text, metadata_json, inserted_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
            )
            ON CONFLICT (event_id) DO NOTHING
            """

        async with self._config.provide_connection() as conn:  # pyright: ignore[reportAttributeAccessIssue]
            for entry in entries:
                if self._owner_id_column_name:
                    params = [
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
                    ]
                else:
                    params = [
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
                    ]

                result = await conn.execute(sql, params)
                rows_affected = self._extract_rows_affected(result)
                if rows_affected > 0:
                    inserted_count += rows_affected

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
        except PsqlpyDatabaseError as e:
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "relation" in error_msg:
                return []
            raise

    async def _search_entries_fts(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = f"""
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at,
               ts_rank(to_tsvector('english', content_text), plainto_tsquery('english', $1)) as rank
        FROM {self._memory_table}
        WHERE app_name = $2
          AND user_id = $3
          AND to_tsvector('english', content_text) @@ plainto_tsquery('english', $1)
        ORDER BY rank DESC, timestamp DESC
        LIMIT $4
        """
        params = [query, app_name, user_id, limit]
        async with self._config.provide_connection() as conn:  # pyright: ignore[reportAttributeAccessIssue]
            result = await conn.fetch(sql, params)
            rows: list[dict[str, Any]] = result.result() if result else []
        return _rows_to_records(rows)

    async def _search_entries_simple(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = f"""
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at
        FROM {self._memory_table}
        WHERE app_name = $1
          AND user_id = $2
          AND content_text ILIKE $3
        ORDER BY timestamp DESC
        LIMIT $4
        """
        pattern = f"%{query}%"
        params = [app_name, user_id, pattern, limit]
        async with self._config.provide_connection() as conn:  # pyright: ignore[reportAttributeAccessIssue]
            result = await conn.fetch(sql, params)
            rows: list[dict[str, Any]] = result.result() if result else []
        return _rows_to_records(rows)

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        count_sql = f"SELECT COUNT(*) AS count FROM {self._memory_table} WHERE session_id = $1"
        delete_sql = f"DELETE FROM {self._memory_table} WHERE session_id = $1"

        try:
            async with self._config.provide_connection() as conn:  # pyright: ignore[reportAttributeAccessIssue]
                count_result = await conn.fetch(count_sql, [session_id])
                count_rows: list[dict[str, Any]] = count_result.result() if count_result else []
                count = int(count_rows[0]["count"]) if count_rows else 0
                await conn.execute(delete_sql, [session_id])
                return count
        except PsqlpyDatabaseError as e:
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "relation" in error_msg:
                return 0
            raise

    async def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        count_sql = f"""
        SELECT COUNT(*) AS count FROM {self._memory_table}
        WHERE inserted_at < CURRENT_TIMESTAMP - INTERVAL '{days} days'
        """
        delete_sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < CURRENT_TIMESTAMP - INTERVAL '{days} days'
        """

        try:
            async with self._config.provide_connection() as conn:  # pyright: ignore[reportAttributeAccessIssue]
                count_result = await conn.fetch(count_sql, [])
                count_rows: list[dict[str, Any]] = count_result.result() if count_result else []
                count = int(count_rows[0]["count"]) if count_rows else 0
                await conn.execute(delete_sql, [])
                return count
        except PsqlpyDatabaseError as e:
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "relation" in error_msg:
                return 0
            raise

    def _extract_rows_affected(self, result: Any) -> int:
        """Extract rows affected from psqlpy result."""
        try:
            if has_query_result_metadata(result):
                if result.tag:
                    return self._parse_command_tag(result.tag)
                if result.status:
                    return self._parse_command_tag(result.status)
            if isinstance(result, str):
                return self._parse_command_tag(result)
        except Exception as e:
            logger.debug("Failed to parse psqlpy command tag: %s", e)
        return -1

    def _parse_command_tag(self, tag: str) -> int:
        """Parse PostgreSQL command tag to extract rows affected."""
        if not tag:
            return -1

        match = PSQLPY_STATUS_REGEX.match(tag.strip())
        if match:
            command = match.group(1).upper()
            if command == "INSERT" and match.group(3):
                return int(match.group(3))
            if command in {"UPDATE", "DELETE"} and match.group(3):
                return int(match.group(3))
        return -1


def _rows_to_records(rows: "list[dict[str, Any]]") -> "list[MemoryRecord]":
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
