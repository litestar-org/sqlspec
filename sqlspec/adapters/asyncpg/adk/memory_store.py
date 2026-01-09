"""AsyncPG ADK memory store for Google Agent Development Kit memory storage."""

from typing import TYPE_CHECKING

import asyncpg

from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig
    from sqlspec.extensions.adk.memory._types import MemoryRecord

logger = get_logger("adapters.asyncpg.adk.memory_store")

__all__ = ("AsyncpgADKMemoryStore",)


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
        from sqlspec.adapters.asyncpg.adk.memory_store import AsyncpgADKMemoryStore

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

                if result and "INSERT" in result:
                    count_str = result.split()[-1]
                    if count_str.isdigit() and int(count_str) > 0:
                        inserted_count += 1

        return inserted_count

    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query.

        Args:
            query: Text query to search for.
            app_name: Application name to filter by.
            user_id: User ID to filter by.
            limit: Maximum number of results (defaults to max_results config).

        Returns:
            List of matching memory records ordered by relevance/timestamp.

        Raises:
            RuntimeError: If memory store is disabled.
        """
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
        except asyncpg.exceptions.UndefinedTableError:
            return []

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
        async with self._config.provide_connection() as conn:
            rows = await conn.fetch(sql, query, app_name, user_id, limit)
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
        async with self._config.provide_connection() as conn:
            rows = await conn.fetch(sql, app_name, user_id, pattern, limit)
        return _rows_to_records(rows)

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session.

        Args:
            session_id: Session ID to delete entries for.

        Returns:
            Number of entries deleted.
        """
        sql = f"DELETE FROM {self._memory_table} WHERE session_id = $1"

        async with self._config.provide_connection() as conn:
            result = await conn.execute(sql, session_id)

            if result and "DELETE" in result:
                count_str = result.split()[-1]
                if count_str.isdigit():
                    return int(count_str)

        return 0

    async def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days.

        Used for TTL cleanup operations.

        Args:
            days: Number of days to retain entries.

        Returns:
            Number of entries deleted.
        """
        sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < CURRENT_TIMESTAMP - INTERVAL '{days} days'
        """

        async with self._config.provide_connection() as conn:
            result = await conn.execute(sql)

            if result and "DELETE" in result:
                count_str = result.split()[-1]
                if count_str.isdigit():
                    return int(count_str)

        return 0


def _rows_to_records(rows: "list[asyncpg.Record]") -> "list[MemoryRecord]":
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
