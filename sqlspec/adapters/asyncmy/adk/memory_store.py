"""AsyncMy ADK memory store for Google Agent Development Kit memory storage."""

import json
import re
from typing import TYPE_CHECKING, Any, Final

import asyncmy

from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.asyncmy.config import AsyncmyConfig
    from sqlspec.extensions.adk.memory._types import MemoryRecord

logger = get_logger("adapters.asyncmy.adk.memory_store")

__all__ = ("AsyncmyADKMemoryStore",)

MYSQL_TABLE_NOT_FOUND_ERROR: Final = 1146


def _parse_owner_id_column_for_mysql(column_ddl: str) -> "tuple[str, str]":
    """Parse owner ID column DDL for MySQL FOREIGN KEY syntax.

    Args:
        column_ddl: Column DDL like "tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE".

    Returns:
        Tuple of (column_definition, foreign_key_constraint).
    """
    references_match = re.search(r"\s+REFERENCES\s+(.+)", column_ddl, re.IGNORECASE)
    if not references_match:
        return (column_ddl.strip(), "")

    col_def = column_ddl[: references_match.start()].strip()
    fk_clause = references_match.group(1).strip()
    col_name = col_def.split()[0]
    fk_constraint = f"FOREIGN KEY ({col_name}) REFERENCES {fk_clause}"
    return (col_def, fk_constraint)


class AsyncmyADKMemoryStore(BaseAsyncADKMemoryStore["AsyncmyConfig"]):
    """MySQL/MariaDB ADK memory store using AsyncMy driver."""

    __slots__ = ()

    def __init__(self, config: "AsyncmyConfig") -> None:
        """Initialize AsyncMy memory store."""
        super().__init__(config)

    async def _get_create_memory_table_sql(self) -> str:
        """Get MySQL CREATE TABLE SQL for memory entries."""
        owner_id_line = ""
        fk_constraint = ""
        if self._owner_id_column_ddl:
            col_def, fk_def = _parse_owner_id_column_for_mysql(self._owner_id_column_ddl)
            owner_id_line = f",\n            {col_def}"
            if fk_def:
                fk_constraint = f",\n            {fk_def}"

        fts_index = ""
        if self._use_fts:
            fts_index = f",\n            FULLTEXT INDEX idx_{self._memory_table}_fts (content_text)"

        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_line},
            timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            content_json JSON NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json JSON,
            inserted_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            INDEX idx_{self._memory_table}_app_user_time (app_name, user_id, timestamp),
            INDEX idx_{self._memory_table}_session (session_id){fts_index}{fk_constraint}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

    def _get_drop_memory_table_sql(self) -> "list[str]":
        """Get MySQL DROP TABLE SQL statements."""
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    async def create_tables(self) -> None:
        """Create the memory table and indexes if they don't exist."""
        if not self._enabled:
            logger.debug("Memory store disabled, skipping table creation")
            return

        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._get_create_memory_table_sql())
        logger.debug("Created ADK memory table: %s", self._memory_table)

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
            INSERT IGNORE INTO {self._memory_table} (
                id, session_id, app_name, user_id, event_id, author,
                {self._owner_id_column_name}, timestamp, content_json,
                content_text, metadata_json, inserted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        else:
            sql = f"""
            INSERT IGNORE INTO {self._memory_table} (
                id, session_id, app_name, user_id, event_id, author,
                timestamp, content_json, content_text, metadata_json, inserted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

        async with self._config.provide_connection() as conn, conn.cursor() as cursor:
            for entry in entries:
                content_json = json.dumps(entry["content_json"])
                metadata_json = json.dumps(entry["metadata_json"]) if entry["metadata_json"] is not None else None

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
                        content_json,
                        entry["content_text"],
                        metadata_json,
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
                        content_json,
                        entry["content_text"],
                        metadata_json,
                        entry["inserted_at"],
                    )

                await cursor.execute(sql, params)
                if cursor.rowcount and cursor.rowcount > 0:
                    inserted_count += cursor.rowcount

            await conn.commit()

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
        except asyncmy.errors.ProgrammingError as exc:  # pyright: ignore[reportAttributeAccessIssue]
            if "doesn't exist" in str(exc) or exc.args[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return []
            raise

    async def _search_entries_fts(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = f"""
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at
        FROM {self._memory_table}
        WHERE app_name = %s
          AND user_id = %s
          AND MATCH(content_text) AGAINST (%s IN NATURAL LANGUAGE MODE)
        ORDER BY timestamp DESC
        LIMIT %s
        """
        params = (app_name, user_id, query, limit)
        async with self._config.provide_connection() as conn, conn.cursor() as cursor:
            await cursor.execute(sql, params)
            rows = await cursor.fetchall()
        return _rows_to_records(rows)

    async def _search_entries_simple(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = f"""
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at
        FROM {self._memory_table}
        WHERE app_name = %s
          AND user_id = %s
          AND content_text LIKE %s
        ORDER BY timestamp DESC
        LIMIT %s
        """
        pattern = f"%{query}%"
        params = (app_name, user_id, pattern, limit)
        async with self._config.provide_connection() as conn, conn.cursor() as cursor:
            await cursor.execute(sql, params)
            rows = await cursor.fetchall()
        return _rows_to_records(rows)

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        sql = f"DELETE FROM {self._memory_table} WHERE session_id = %s"
        async with self._config.provide_connection() as conn, conn.cursor() as cursor:
            await cursor.execute(sql, (session_id,))
            await conn.commit()
            return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

    async def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < CURRENT_TIMESTAMP - INTERVAL {days} DAY
        """
        async with self._config.provide_connection() as conn, conn.cursor() as cursor:
            await cursor.execute(sql)
            await conn.commit()
            return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0


def _decode_json_field(value: Any, *, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, bytes):
        return json.loads(value.decode("utf-8"))
    if isinstance(value, str):
        return json.loads(value)
    return value


def _rows_to_records(rows: "list[tuple[Any, ...]]") -> "list[MemoryRecord]":
    return [
        {
            "id": row[0],
            "session_id": row[1],
            "app_name": row[2],
            "user_id": row[3],
            "event_id": row[4],
            "author": row[5],
            "timestamp": row[6],
            "content_json": _decode_json_field(row[7], default={}),
            "content_text": row[8],
            "metadata_json": _decode_json_field(row[9], default=None),
            "inserted_at": row[10],
        }
        for row in rows
    ]
