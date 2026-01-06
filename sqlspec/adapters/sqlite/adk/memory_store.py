"""SQLite sync ADK memory store for Google Agent Development Kit memory storage."""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from sqlspec.adapters.sqlite.config import SqliteConfig
    from sqlspec.extensions.adk.memory._types import MemoryRecord

logger = get_logger("adapters.sqlite.adk.memory_store")

SECONDS_PER_DAY = 86400.0
JULIAN_EPOCH = 2440587.5

__all__ = ("SqliteADKMemoryStore",)


def _datetime_to_julian(dt: datetime) -> float:
    """Convert datetime to Julian Day number for SQLite storage.

    Args:
        dt: Datetime to convert (must be UTC-aware).

    Returns:
        Julian Day number as REAL.

    Notes:
        Julian Day number is days since November 24, 4714 BCE (proleptic Gregorian).
        This enables direct comparison with julianday('now') in SQL queries.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta_days = (dt - epoch).total_seconds() / SECONDS_PER_DAY
    return JULIAN_EPOCH + delta_days


def _julian_to_datetime(julian: float) -> datetime:
    """Convert Julian Day number back to datetime.

    Args:
        julian: Julian Day number.

    Returns:
        UTC-aware datetime.
    """
    days_since_epoch = julian - JULIAN_EPOCH
    timestamp = days_since_epoch * SECONDS_PER_DAY
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


class SqliteADKMemoryStore(BaseSyncADKMemoryStore["SqliteConfig"]):
    """SQLite ADK memory store using synchronous SQLite driver.

    Implements memory entry storage for Google Agent Development Kit
    using SQLite via the synchronous sqlite3 driver. Provides:
    - Session memory storage with JSON as TEXT
    - Simple LIKE search (simple strategy)
    - Optional FTS5 full-text search (sqlite_fts5 strategy)
    - Julian Day timestamps (REAL) for efficient date operations
    - Deduplication via event_id unique constraint
    - Efficient upserts using INSERT OR IGNORE

    Args:
        config: SqliteConfig with extension_config["adk"] settings.

    Example:
        from sqlspec.adapters.sqlite import SqliteConfig
        from sqlspec.adapters.sqlite.adk.memory_store import SqliteADKMemoryStore

        config = SqliteConfig(
            database="app.db",
            extension_config={
                "adk": {
                    "memory_table": "adk_memory_entries",
                    "memory_use_fts": False,
                    "memory_max_results": 20,
                }
            }
        )
        store = SqliteADKMemoryStore(config)
        store.create_tables()

    Notes:
        - JSON stored as TEXT with SQLSpec serializers
        - REAL for Julian Day timestamps
        - event_id UNIQUE constraint for deduplication
        - Composite index on (app_name, user_id, timestamp DESC)
        - Optional FTS5 virtual table for full-text search
        - Configuration is read from config.extension_config["adk"]
    """

    __slots__ = ()

    def __init__(self, config: "SqliteConfig") -> None:
        """Initialize SQLite ADK memory store.

        Args:
            config: SqliteConfig instance.

        Notes:
            Configuration is read from config.extension_config["adk"]:
            - memory_table: Memory table name (default: "adk_memory_entries")
            - memory_use_fts: Enable full-text search when supported (default: False)
            - memory_max_results: Max search results (default: 20)
            - owner_id_column: Optional owner FK column DDL (default: None)
            - enable_memory: Whether memory is enabled (default: True)
        """
        super().__init__(config)

    def _get_create_memory_table_sql(self) -> str:
        """Get SQLite CREATE TABLE SQL for memory entries.

        Returns:
            SQL statement to create memory table with indexes.

        Notes:
            - TEXT for IDs, names, and JSON content
            - REAL for Julian Day timestamps
            - UNIQUE constraint on event_id for deduplication
            - Composite index on (app_name, user_id, timestamp DESC)
            - Optional owner ID column for multi-tenancy
            - Optional FTS5 virtual table for full-text search
        """
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        fts_table = ""
        if self._use_fts:
            fts_table = f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS {self._memory_table}_fts USING fts5(
            content_text,
            content={self._memory_table},
            content_rowid=rowid
        );

        CREATE TRIGGER IF NOT EXISTS {self._memory_table}_ai AFTER INSERT ON {self._memory_table} BEGIN
            INSERT INTO {self._memory_table}_fts(rowid, content_text) VALUES (new.rowid, new.content_text);
        END;

        CREATE TRIGGER IF NOT EXISTS {self._memory_table}_ad AFTER DELETE ON {self._memory_table} BEGIN
            INSERT INTO {self._memory_table}_fts({self._memory_table}_fts, rowid, content_text)
            VALUES('delete', old.rowid, old.content_text);
        END;

        CREATE TRIGGER IF NOT EXISTS {self._memory_table}_au AFTER UPDATE ON {self._memory_table} BEGIN
            INSERT INTO {self._memory_table}_fts({self._memory_table}_fts, rowid, content_text)
            VALUES('delete', old.rowid, old.content_text);
            INSERT INTO {self._memory_table}_fts(rowid, content_text) VALUES (new.rowid, new.content_text);
        END;
            """

        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            app_name TEXT NOT NULL,
            user_id TEXT NOT NULL,
            event_id TEXT NOT NULL UNIQUE,
            author TEXT{owner_id_line},
            timestamp REAL NOT NULL,
            content_json TEXT NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json TEXT,
            inserted_at REAL NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_app_user_time
            ON {self._memory_table}(app_name, user_id, timestamp DESC);

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_session
            ON {self._memory_table}(session_id);
        {fts_table}
        """

    def _get_drop_memory_table_sql(self) -> "list[str]":
        """Get SQLite DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop the memory table and FTS table.

        Notes:
            SQLite automatically drops indexes when dropping tables.
            FTS5 virtual table must be dropped separately if it exists.
        """
        statements = [f"DROP TABLE IF EXISTS {self._memory_table}"]
        if self._use_fts:
            statements.insert(0, f"DROP TABLE IF EXISTS {self._memory_table}_fts")
        return statements

    def _enable_foreign_keys(self, connection: Any) -> None:
        """Enable foreign key constraints for this connection.

        Args:
            connection: SQLite connection.

        Notes:
            SQLite requires PRAGMA foreign_keys = ON per connection.
        """
        connection.execute("PRAGMA foreign_keys = ON")

    def create_tables(self) -> None:
        """Create the memory table and indexes if they don't exist.

        Skips table creation if memory store is disabled.
        """
        if not self._enabled:
            logger.debug("Memory store disabled, skipping table creation")
            return

        with self._config.provide_session() as driver:
            self._enable_foreign_keys(driver.connection)
            driver.execute_script(self._get_create_memory_table_sql())
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
            self._enable_foreign_keys(conn)

            for entry in entries:
                timestamp_julian = _datetime_to_julian(entry["timestamp"])
                inserted_at_julian = _datetime_to_julian(entry["inserted_at"])
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
                        timestamp_julian,
                        content_json_str,
                        entry["content_text"],
                        metadata_json_str,
                        inserted_at_julian,
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
                        timestamp_julian,
                        content_json_str,
                        entry["content_text"],
                        metadata_json_str,
                        inserted_at_julian,
                    )

                cursor = conn.execute(sql, params)
                if cursor.rowcount > 0:
                    inserted_count += 1

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
            List of matching memory records ordered by relevance/timestamp.

        Raises:
            RuntimeError: If memory store is disabled.
        """
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        effective_limit = limit if limit is not None else self._max_results

        if self._use_fts:
            try:
                return self._search_entries_fts(query, app_name, user_id, effective_limit)
            except Exception as exc:  # pragma: no cover - defensive fallback
                logger.warning("FTS search failed; falling back to simple search: %s", exc)
        return self._search_entries_simple(query, app_name, user_id, effective_limit)

    def _search_entries_fts(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = f"""
        SELECT m.id, m.session_id, m.app_name, m.user_id, m.event_id, m.author,
               m.timestamp, m.content_json, m.content_text, m.metadata_json, m.inserted_at
        FROM {self._memory_table} m
        JOIN {self._memory_table}_fts fts ON m.rowid = fts.rowid
        WHERE m.app_name = ?
          AND m.user_id = ?
          AND fts.content_text MATCH ?
        ORDER BY m.timestamp DESC
        LIMIT ?
        """
        params: tuple[Any, ...] = (app_name, user_id, query, limit)
        return self._fetch_records(sql, params)

    def _search_entries_simple(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = f"""
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at
        FROM {self._memory_table}
        WHERE app_name = ?
          AND user_id = ?
          AND content_text LIKE ?
        ORDER BY timestamp DESC
        LIMIT ?
        """
        pattern = f"%{query}%"
        params = (app_name, user_id, pattern, limit)
        return self._fetch_records(sql, params)

    def _fetch_records(self, sql: str, params: "tuple[Any, ...]") -> "list[MemoryRecord]":
        with self._config.provide_connection() as conn:
            self._enable_foreign_keys(conn)
            cursor = conn.execute(sql, params)
            rows = cursor.fetchall()
        return [
            {
                "id": row[0],
                "session_id": row[1],
                "app_name": row[2],
                "user_id": row[3],
                "event_id": row[4],
                "author": row[5],
                "timestamp": _julian_to_datetime(row[6]),
                "content_json": from_json(row[7]) if row[7] else {},
                "content_text": row[8],
                "metadata_json": from_json(row[9]) if row[9] else None,
                "inserted_at": _julian_to_datetime(row[10]),
            }
            for row in rows
        ]

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session.

        Args:
            session_id: Session ID to delete entries for.

        Returns:
            Number of entries deleted.
        """
        sql = f"DELETE FROM {self._memory_table} WHERE session_id = ?"

        with self._config.provide_connection() as conn:
            self._enable_foreign_keys(conn)
            cursor = conn.execute(sql, (session_id,))
            deleted_count = cursor.rowcount
            conn.commit()

        return deleted_count

    def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days.

        Used for TTL cleanup operations.

        Args:
            days: Number of days to retain entries.

        Returns:
            Number of entries deleted.
        """
        cutoff_julian = _datetime_to_julian(datetime.now(timezone.utc)) - days

        sql = f"DELETE FROM {self._memory_table} WHERE inserted_at < ?"

        with self._config.provide_connection() as conn:
            self._enable_foreign_keys(conn)
            cursor = conn.execute(sql, (cutoff_julian,))
            deleted_count = cursor.rowcount
            conn.commit()

        return deleted_count
