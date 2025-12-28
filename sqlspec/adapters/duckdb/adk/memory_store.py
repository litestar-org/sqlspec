"""DuckDB sync ADK memory store for Google Agent Development Kit memory storage."""

import contextlib
from typing import TYPE_CHECKING, Any, Final

from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from sqlspec.adapters.duckdb.config import DuckDBConfig
    from sqlspec.extensions.adk.memory._types import MemoryRecord

logger = get_logger("adapters.duckdb.adk.memory_store")

__all__ = ("DuckdbADKMemoryStore",)

DUCKDB_TABLE_NOT_FOUND_ERROR: Final = "does not exist"


class DuckdbADKMemoryStore(BaseSyncADKMemoryStore["DuckDBConfig"]):
    """DuckDB ADK memory store using synchronous DuckDB driver.

    Implements memory entry storage for Google Agent Development Kit
    using DuckDB's synchronous driver. Provides:
    - Session memory storage with native JSON type
    - Simple ILIKE search
    - Native TIMESTAMP type support
    - Deduplication via event_id unique constraint
    - Efficient upserts using INSERT OR IGNORE
    - Columnar storage for analytical queries

    Args:
        config: DuckDBConfig with extension_config["adk"] settings.

    Example:
        from sqlspec.adapters.duckdb import DuckDBConfig
        from sqlspec.adapters.duckdb.adk.memory_store import DuckdbADKMemoryStore

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
        store.create_tables()

    Notes:
        - Uses DuckDB native JSON type (not JSONB)
        - TIMESTAMP for date/time storage with microsecond precision
        - event_id UNIQUE constraint for deduplication
        - Composite index on (app_name, user_id, timestamp DESC)
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
            conn.execute(f"PRAGMA create_fts_index('{self._memory_table}', 'id', 'content_text')")
        except Exception as exc:
            logger.debug("Failed to create DuckDB FTS index: %s", exc)

    def _refresh_fts_index(self, conn: Any) -> None:
        """Rebuild the FTS index to reflect recent changes."""
        if not self._ensure_fts_extension(conn):
            return

        with contextlib.suppress(Exception):
            conn.execute(f"PRAGMA drop_fts_index('{self._memory_table}')")

        try:
            conn.execute(f"PRAGMA create_fts_index('{self._memory_table}', 'id', 'content_text')")
        except Exception as exc:
            logger.debug("Failed to refresh DuckDB FTS index: %s", exc)

    def _get_create_memory_table_sql(self) -> str:
        """Get DuckDB CREATE TABLE SQL for memory entries.

        Returns:
            SQL statement to create memory table with indexes.

        Notes:
            - VARCHAR for IDs and names
            - JSON type for content and metadata storage (DuckDB native)
            - TIMESTAMP for timestamps
            - UNIQUE constraint on event_id for deduplication
            - Composite index on (app_name, user_id, timestamp DESC)
            - Optional owner ID column for multi-tenancy
        """
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR PRIMARY KEY,
            session_id VARCHAR NOT NULL,
            app_name VARCHAR NOT NULL,
            user_id VARCHAR NOT NULL,
            event_id VARCHAR NOT NULL UNIQUE,
            author VARCHAR{owner_id_line},
            timestamp TIMESTAMP NOT NULL,
            content_json JSON NOT NULL,
            content_text VARCHAR NOT NULL,
            metadata_json JSON,
            inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_app_user_time
            ON {self._memory_table}(app_name, user_id, timestamp DESC);

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_session
            ON {self._memory_table}(session_id);
        """

    def _get_drop_memory_table_sql(self) -> "list[str]":
        """Get DuckDB DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop the memory table.

        Notes:
            DuckDB automatically drops indexes when dropping tables.
        """
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    def create_tables(self) -> None:
        """Create the memory table and indexes if they don't exist.

        Skips table creation if memory store is disabled.
        """
        if not self._enabled:
            logger.debug("Memory store disabled, skipping table creation")
            return

        with self._config.provide_connection() as conn:
            conn.execute(self._get_create_memory_table_sql())
            if self._use_fts:
                self._create_fts_index(conn)
        logger.debug("Created ADK memory table: %s", self._memory_table)

    def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication.

        Uses INSERT OR IGNORE to skip duplicates based on event_id
        unique constraint.

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
        with self._config.provide_connection() as conn:
            for entry in entries:
                content_json_str = to_json(entry["content_json"])
                metadata_json_str = to_json(entry["metadata_json"]) if entry["metadata_json"] else None

                if self._owner_id_column_name:
                    sql = f"""
                    INSERT OR IGNORE INTO {self._memory_table}
                    (id, session_id, app_name, user_id, event_id, author,
                     {self._owner_id_column_name}, timestamp, content_json,
                     content_text, metadata_json, inserted_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    params: tuple[Any, ...] = (
                        entry["id"],
                        entry["session_id"],
                        entry["app_name"],
                        entry["user_id"],
                        entry["event_id"],
                        entry["author"],
                        owner_id,
                        entry["timestamp"],
                        content_json_str,
                        entry["content_text"],
                        metadata_json_str,
                        entry["inserted_at"],
                    )
                else:
                    sql = f"""
                    INSERT OR IGNORE INTO {self._memory_table}
                    (id, session_id, app_name, user_id, event_id, author,
                     timestamp, content_json, content_text, metadata_json, inserted_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    params = (
                        entry["id"],
                        entry["session_id"],
                        entry["app_name"],
                        entry["user_id"],
                        entry["event_id"],
                        entry["author"],
                        entry["timestamp"],
                        content_json_str,
                        entry["content_text"],
                        metadata_json_str,
                        entry["inserted_at"],
                    )

                try:
                    conn.execute(sql, params)
                    inserted_count += 1
                except Exception as e:
                    if "duplicate" in str(e).lower() or "unique" in str(e).lower():
                        continue
                    raise

            if self._use_fts and inserted_count > 0:
                self._refresh_fts_index(conn)
            conn.commit()

        return inserted_count

    def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query.

        Args:
            query: Text query to search for.
            app_name: Application name to filter by.
            user_id: User ID to filter by.
            limit: Maximum number of results (defaults to max_results config).

        Returns:
            List of matching memory records ordered by timestamp.

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
                    return self._search_entries_fts(query, app_name, user_id, effective_limit)
                except Exception as exc:  # pragma: no cover - defensive fallback
                    logger.warning("FTS search failed; falling back to simple search: %s", exc)
            return self._search_entries_simple(query, app_name, user_id, effective_limit)
        except Exception as exc:
            if DUCKDB_TABLE_NOT_FOUND_ERROR in str(exc):
                return []
            raise

    def _search_entries_fts(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        fts_schema = f"fts_main_{self._memory_table}"
        sql = f"""
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at
        FROM (
            SELECT id, session_id, app_name, user_id, event_id, author,
                   timestamp, content_json, content_text, metadata_json, inserted_at,
                   {fts_schema}.match_bm25(id, ?) AS score
            FROM {self._memory_table}
            WHERE app_name = ?
              AND user_id = ?
        ) AS ranked
        WHERE score IS NOT NULL
        ORDER BY score DESC, timestamp DESC
        LIMIT ?
        """
        params: tuple[Any, ...] = (query, app_name, user_id, limit)
        with self._config.provide_connection() as conn:
            if not self._ensure_fts_extension(conn):
                msg = "DuckDB FTS extension not available"
                raise RuntimeError(msg)
            cursor = conn.execute(sql, params)
            rows = cursor.fetchall()
        return _rows_to_records(rows)

    def _search_entries_simple(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = f"""
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at
        FROM {self._memory_table}
        WHERE app_name = ?
          AND user_id = ?
          AND content_text ILIKE ?
        ORDER BY timestamp DESC
        LIMIT ?
        """
        pattern = f"%{query}%"
        params: tuple[Any, ...] = (app_name, user_id, pattern, limit)
        with self._config.provide_connection() as conn:
            cursor = conn.execute(sql, params)
            rows = cursor.fetchall()
        return _rows_to_records(rows)

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session.

        Args:
            session_id: Session ID to delete entries for.

        Returns:
            Number of entries deleted.
        """
        count_sql = f"SELECT COUNT(*) FROM {self._memory_table} WHERE session_id = ?"
        delete_sql = f"DELETE FROM {self._memory_table} WHERE session_id = ?"

        with self._config.provide_connection() as conn:
            cursor = conn.execute(count_sql, (session_id,))
            count_row = cursor.fetchone()
            count = count_row[0] if count_row else 0

            conn.execute(delete_sql, (session_id,))
            if self._use_fts and count > 0:
                self._refresh_fts_index(conn)
            conn.commit()

        return count

    def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days.

        Used for TTL cleanup operations.

        Args:
            days: Number of days to retain entries.

        Returns:
            Number of entries deleted.
        """
        count_sql = f"""
        SELECT COUNT(*) FROM {self._memory_table}
        WHERE inserted_at < CURRENT_TIMESTAMP - INTERVAL '{days} days'
        """
        delete_sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < CURRENT_TIMESTAMP - INTERVAL '{days} days'
        """

        with self._config.provide_connection() as conn:
            cursor = conn.execute(count_sql)
            count_row = cursor.fetchone()
            count = count_row[0] if count_row else 0

            conn.execute(delete_sql)
            if self._use_fts and count > 0:
                self._refresh_fts_index(conn)
            conn.commit()

        return count


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
            "content_json": from_json(row[7]) if row[7] else {},
            "content_text": row[8],
            "metadata_json": from_json(row[9]) if row[9] else None,
            "inserted_at": row[10],
        }
        for row in rows
    ]
