"""Psycopg ADK memory store for Google Agent Development Kit memory storage."""

from datetime import datetime
from typing import TYPE_CHECKING, Any

from psycopg import errors
from psycopg import sql as pg_sql
from psycopg.types.json import Jsonb

from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
    from sqlspec.extensions.adk.memory._types import MemoryRecord

logger = get_logger("adapters.psycopg.adk.memory_store")

__all__ = ("PsycopgAsyncADKMemoryStore", "PsycopgSyncADKMemoryStore")

_MemoryInsertParams = tuple[str, str, str, str, str, str | None, datetime, Jsonb, str, Jsonb | None, datetime]
_MemoryInsertParamsWithOwner = tuple[
    str, str, str, str, str, str | None, object | None, datetime, Jsonb, str, Jsonb | None, datetime
]


def _build_insert_params(entry: "MemoryRecord") -> _MemoryInsertParams:
    metadata_json = Jsonb(entry["metadata_json"]) if entry["metadata_json"] is not None else None
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
        metadata_json,
        entry["inserted_at"],
    )


def _build_insert_params_with_owner(entry: "MemoryRecord", owner_id: object | None) -> _MemoryInsertParamsWithOwner:
    metadata_json = Jsonb(entry["metadata_json"]) if entry["metadata_json"] is not None else None
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
        metadata_json,
        entry["inserted_at"],
    )


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
