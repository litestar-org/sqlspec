"""ADBC ADK memory store for Google Agent Development Kit memory storage."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from sqlspec.adapters.adbc.adk.store import (
    ADBC_TABLE_NOT_FOUND_PATTERNS,
    DIALECT_DUCKDB,
    DIALECT_GENERIC,
    DIALECT_POSTGRESQL,
    DIALECT_SNOWFLAKE,
    DIALECT_SQLITE,
)
from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from sqlspec.adapters.adbc.config import AdbcConfig
    from sqlspec.extensions.adk.memory._types import MemoryRecord

logger = get_logger("adapters.adbc.adk.memory_store")

__all__ = ("AdbcADKMemoryStore",)


class AdbcADKMemoryStore(BaseSyncADKMemoryStore["AdbcConfig"]):
    """ADBC synchronous ADK memory store for Arrow Database Connectivity."""

    __slots__ = ("_dialect",)

    def __init__(self, config: "AdbcConfig") -> None:
        super().__init__(config)
        self._dialect = self._detect_dialect()

    @property
    def dialect(self) -> str:
        return self._dialect

    def _detect_dialect(self) -> str:
        driver_name = self._config.connection_config.get("driver_name", "").lower()
        if "postgres" in driver_name:
            return DIALECT_POSTGRESQL
        if "sqlite" in driver_name:
            return DIALECT_SQLITE
        if "duckdb" in driver_name:
            return DIALECT_DUCKDB
        if "snowflake" in driver_name:
            return DIALECT_SNOWFLAKE
        logger.warning("Unknown ADBC driver: %s. Using generic SQL dialect.", driver_name)
        return DIALECT_GENERIC

    def _serialize_json_field(self, value: Any) -> "str | None":
        if value is None:
            return None
        return to_json(value)

    def _encode_timestamp(self, value: datetime) -> Any:
        if self._dialect == DIALECT_SQLITE:
            return value.timestamp()
        return value

    def _decode_timestamp(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        return datetime.fromisoformat(str(value))

    def _get_create_memory_table_sql(self) -> str:
        if self._dialect == DIALECT_POSTGRESQL:
            return self._get_memory_ddl_postgresql()
        if self._dialect == DIALECT_SQLITE:
            return self._get_memory_ddl_sqlite()
        if self._dialect == DIALECT_DUCKDB:
            return self._get_memory_ddl_duckdb()
        if self._dialect == DIALECT_SNOWFLAKE:
            return self._get_memory_ddl_snowflake()
        return self._get_memory_ddl_generic()

    def _get_memory_ddl_postgresql(self) -> str:
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_ddl},
            timestamp TIMESTAMPTZ NOT NULL,
            content_json JSONB NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json JSONB,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """

    def _get_memory_ddl_sqlite(self) -> str:
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            app_name TEXT NOT NULL,
            user_id TEXT NOT NULL,
            event_id TEXT NOT NULL UNIQUE,
            author TEXT{owner_id_ddl},
            timestamp REAL NOT NULL,
            content_json TEXT NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json TEXT,
            inserted_at REAL NOT NULL
        )
        """

    def _get_memory_ddl_duckdb(self) -> str:
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_ddl},
            timestamp TIMESTAMP NOT NULL,
            content_json JSON NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json JSON,
            inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """

    def _get_memory_ddl_snowflake(self) -> str:
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR PRIMARY KEY,
            session_id VARCHAR NOT NULL,
            app_name VARCHAR NOT NULL,
            user_id VARCHAR NOT NULL,
            event_id VARCHAR NOT NULL UNIQUE,
            author VARCHAR{owner_id_ddl},
            timestamp TIMESTAMP_TZ NOT NULL,
            content_json VARIANT NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json VARIANT,
            inserted_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
        )
        """

    def _get_memory_ddl_generic(self) -> str:
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_ddl},
            timestamp TIMESTAMP NOT NULL,
            content_json TEXT NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json TEXT,
            inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """

    def _get_drop_memory_table_sql(self) -> "list[str]":
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    def create_tables(self) -> None:
        if not self._enabled:
            return

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(self._get_create_memory_table_sql())
                conn.commit()

                idx_app_user = (
                    f"CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_app_user_time "
                    f"ON {self._memory_table}(app_name, user_id, timestamp DESC)"
                )
                cursor.execute(idx_app_user)
                conn.commit()

                idx_session = (
                    f"CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_session ON {self._memory_table}(session_id)"
                )
                cursor.execute(idx_session)
                conn.commit()
            finally:
                cursor.close()  # type: ignore[no-untyped-call]

    def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        inserted_count = 0
        use_returning = self._dialect in {DIALECT_SQLITE, DIALECT_POSTGRESQL, DIALECT_DUCKDB}

        if self._owner_id_column_name:
            if use_returning:
                sql = f"""
                INSERT INTO {self._memory_table} (
                    id, session_id, app_name, user_id, event_id, author,
                    {self._owner_id_column_name}, timestamp, content_json, content_text,
                    metadata_json, inserted_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                ) ON CONFLICT(event_id) DO NOTHING RETURNING 1
                """
            else:
                sql = f"""
                INSERT INTO {self._memory_table} (
                    id, session_id, app_name, user_id, event_id, author,
                    {self._owner_id_column_name}, timestamp, content_json, content_text,
                    metadata_json, inserted_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """
        elif use_returning:
            sql = f"""
                INSERT INTO {self._memory_table} (
                    id, session_id, app_name, user_id, event_id, author,
                    timestamp, content_json, content_text, metadata_json, inserted_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                ) ON CONFLICT(event_id) DO NOTHING RETURNING 1
                """
        else:
            sql = f"""
                INSERT INTO {self._memory_table} (
                    id, session_id, app_name, user_id, event_id, author,
                    timestamp, content_json, content_text, metadata_json, inserted_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                for entry in entries:
                    content_json = self._serialize_json_field(entry["content_json"])
                    metadata_json = self._serialize_json_field(entry["metadata_json"])
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
                            self._encode_timestamp(entry["timestamp"]),
                            content_json,
                            entry["content_text"],
                            metadata_json,
                            self._encode_timestamp(entry["inserted_at"]),
                        )
                    else:
                        params = (
                            entry["id"],
                            entry["session_id"],
                            entry["app_name"],
                            entry["user_id"],
                            entry["event_id"],
                            entry["author"],
                            self._encode_timestamp(entry["timestamp"]),
                            content_json,
                            entry["content_text"],
                            metadata_json,
                            self._encode_timestamp(entry["inserted_at"]),
                        )
                    if use_returning:
                        cursor.execute(sql, params)
                        if cursor.fetchone():
                            inserted_count += 1
                    else:
                        try:
                            cursor.execute(sql, params)
                            inserted_count += 1
                        except Exception as exc:
                            exc_str = str(exc).lower()
                            if "unique" in exc_str or "constraint" in exc_str or "duplicate" in exc_str:
                                continue
                            raise
                conn.commit()
            finally:
                cursor.close()  # type: ignore[no-untyped-call]

        return inserted_count

    def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if self._use_fts:
            logger.warning("ADBC memory store does not support FTS, falling back to simple search")

        effective_limit = limit if limit is not None else self._max_results
        pattern = f"%{query}%"

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

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, (app_name, user_id, pattern, effective_limit))
                    rows = cursor.fetchall()
                finally:
                    cursor.close()  # type: ignore[no-untyped-call]
        except Exception as exc:
            error_msg = str(exc).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return []
            raise

        return self._rows_to_records(rows)

    def delete_entries_by_session(self, session_id: str) -> int:
        use_returning = self._dialect in {DIALECT_SQLITE, DIALECT_POSTGRESQL, DIALECT_DUCKDB}
        if use_returning:
            sql = f"DELETE FROM {self._memory_table} WHERE session_id = ? RETURNING 1"
        else:
            sql = f"DELETE FROM {self._memory_table} WHERE session_id = ?"
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (session_id,))
                if use_returning:
                    deleted_rows = cursor.fetchall()
                    conn.commit()
                    return len(deleted_rows)
                conn.commit()
                return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            finally:
                cursor.close()  # type: ignore[no-untyped-call]

    def delete_entries_older_than(self, days: int) -> int:
        cutoff = self._encode_timestamp(datetime.now(timezone.utc) - timedelta(days=days))
        use_returning = self._dialect in {DIALECT_SQLITE, DIALECT_POSTGRESQL, DIALECT_DUCKDB}
        if use_returning:
            sql = f"DELETE FROM {self._memory_table} WHERE inserted_at < ? RETURNING 1"
        else:
            sql = f"DELETE FROM {self._memory_table} WHERE inserted_at < ?"
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (cutoff,))
                if use_returning:
                    deleted_rows = cursor.fetchall()
                    conn.commit()
                    return len(deleted_rows)
                conn.commit()
                return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            finally:
                cursor.close()  # type: ignore[no-untyped-call]

    def _rows_to_records(self, rows: "list[Any]") -> "list[MemoryRecord]":
        records: list[MemoryRecord] = []
        for row in rows:
            content_json = row[7]
            if isinstance(content_json, dict):
                content_value = content_json
            else:
                content_value = from_json(content_json if isinstance(content_json, (str, bytes)) else str(content_json))

            metadata_json = row[9]
            if metadata_json is None:
                metadata_value = None
            elif isinstance(metadata_json, dict):
                metadata_value = metadata_json
            else:
                metadata_value = from_json(
                    metadata_json if isinstance(metadata_json, (str, bytes)) else str(metadata_json)
                )

            records.append({
                "id": row[0],
                "session_id": row[1],
                "app_name": row[2],
                "user_id": row[3],
                "event_id": row[4],
                "author": row[5],
                "timestamp": self._decode_timestamp(row[6]),
                "content_json": content_value,
                "content_text": row[8],
                "metadata_json": metadata_value,
                "inserted_at": self._decode_timestamp(row[10]),
            })
        return records
