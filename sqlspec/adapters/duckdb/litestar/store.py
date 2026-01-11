"""DuckDB sync session store for Litestar integration."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.adapters.duckdb.config import DuckDBConfig


__all__ = ("DuckdbStore",)


class DuckdbStore(BaseSQLSpecStore["DuckDBConfig"]):
    """DuckDB session store using synchronous DuckDB driver.

    Implements server-side session storage for Litestar using DuckDB
    via the synchronous duckdb driver. Uses Litestar's sync_to_thread
    utility to provide an async interface compatible with the Store protocol.

    Provides efficient session management with:
    - Sync operations wrapped for async compatibility
    - INSERT OR REPLACE for UPSERT functionality
    - Native TIMESTAMP type support
    - Automatic expiration handling
    - Efficient cleanup of expired sessions
    - Columnar storage optimized for analytical queries

    Note:
        DuckDB is primarily designed for analytical (OLAP) workloads.
        For high-concurrency OLTP session stores, consider PostgreSQL adapters.

    Args:
        config: DuckDBConfig instance.

    Example:
        from sqlspec.adapters.duckdb import DuckDBConfig
        from sqlspec.adapters.duckdb.litestar.store import DuckdbStore

        config = DuckDBConfig()
        store = DuckdbStore(config)
        await store.create_table()
    """

    __slots__ = ()

    def __init__(self, config: "DuckDBConfig") -> None:
        """Initialize DuckDB session store.

        Args:
            config: DuckDBConfig instance.

        Notes:
            Table name is read from config.extension_config["litestar"]["session_table"].
        """
        super().__init__(config)

    def _get_create_table_sql(self) -> str:
        """Get DuckDB CREATE TABLE SQL.

        Returns:
            SQL statement to create the sessions table with proper indexes.

        Notes:
            - Uses TIMESTAMP type for expires_at (DuckDB native datetime type)
            - TIMESTAMP supports ISO 8601 format and direct comparisons
            - Columnar storage makes this efficient for analytical queries
            - DuckDB does not support partial indexes, so full index is created
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            session_id VARCHAR PRIMARY KEY,
            data BLOB NOT NULL,
            expires_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_{self._table_name}_expires_at
        ON {self._table_name}(expires_at);
        """

    def _get_drop_table_sql(self) -> "list[str]":
        """Get DuckDB DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop indexes and table.
        """
        return [f"DROP INDEX IF EXISTS idx_{self._table_name}_expires_at", f"DROP TABLE IF EXISTS {self._table_name}"]

    def _datetime_to_timestamp(self, dt: "datetime | None") -> "str | None":
        """Convert datetime to ISO 8601 string for DuckDB TIMESTAMP storage.

        Args:
            dt: Datetime to convert (must be UTC-aware).

        Returns:
            ISO 8601 formatted string, or None if dt is None.

        Notes:
            DuckDB's TIMESTAMP type accepts ISO 8601 format strings.
            This enables efficient storage and comparison operations.
        """
        if dt is None:
            return None
        return dt.isoformat()

    def _timestamp_to_datetime(self, ts: "str | datetime | None") -> "datetime | None":
        """Convert TIMESTAMP string back to datetime.

        Args:
            ts: ISO 8601 timestamp string or datetime object.

        Returns:
            UTC-aware datetime, or None if ts is None.
        """
        if ts is None:
            return None
        if isinstance(ts, datetime):
            if ts.tzinfo is None:
                return ts.replace(tzinfo=timezone.utc)
            return ts
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def _create_table(self) -> None:
        """Synchronous implementation of create_table."""
        sql = self._get_create_table_sql()
        with self._config.provide_session() as driver:
            driver.execute_script(sql)
        self._log_table_created()

    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        await async_(self._create_table)()

    def _get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Synchronous implementation of get."""
        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = ?
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        with self._config.provide_connection() as conn:
            cursor = conn.execute(sql, (key,))
            row = cursor.fetchone()

            if row is None:
                return None

            data, expires_at_str = row

            if renew_for is not None and expires_at_str is not None:
                new_expires_at = self._calculate_expires_at(renew_for)
                new_expires_at_str = self._datetime_to_timestamp(new_expires_at)
                if new_expires_at_str is not None:
                    update_sql = f"""
                    UPDATE {self._table_name}
                    SET expires_at = ?, updated_at = NOW()
                    WHERE session_id = ?
                    """
                    conn.execute(update_sql, (new_expires_at_str, key))
                    conn.commit()

            return bytes(data)

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Get a session value by key.

        Args:
            key: Session ID to retrieve.
            renew_for: If given, renew the expiry time for this duration.

        Returns:
            Session data as bytes if found and not expired, None otherwise.
        """
        return await async_(self._get)(key, renew_for)

    def _set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Synchronous implementation of set.

        Notes:
            Stores expires_at as TIMESTAMP (ISO 8601 string) for DuckDB native support.
            Uses INSERT ON CONFLICT instead of INSERT OR REPLACE to ensure all columns
            are properly updated. created_at uses DEFAULT on insert, updated_at gets
            current timestamp on both insert and update.
        """
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        expires_at_str = self._datetime_to_timestamp(expires_at)

        sql = f"""
        INSERT INTO {self._table_name} (session_id, data, expires_at)
        VALUES (?, ?, ?)
        ON CONFLICT (session_id)
        DO UPDATE SET
            data = EXCLUDED.data,
            expires_at = EXCLUDED.expires_at,
            updated_at = NOW()
        """

        with self._config.provide_connection() as conn:
            conn.execute(sql, (key, data, expires_at_str))
            conn.commit()

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Store a session value.

        Args:
            key: Session ID.
            value: Session data.
            expires_in: Time until expiration.
        """
        await async_(self._set)(key, value, expires_in)

    def _delete(self, key: str) -> None:
        """Synchronous implementation of delete."""
        sql = f"DELETE FROM {self._table_name} WHERE session_id = ?"

        with self._config.provide_connection() as conn:
            conn.execute(sql, (key,))
            conn.commit()

    async def delete(self, key: str) -> None:
        """Delete a session by key.

        Args:
            key: Session ID to delete.
        """
        await async_(self._delete)(key)

    def _delete_all(self) -> None:
        """Synchronous implementation of delete_all."""
        sql = f"DELETE FROM {self._table_name}"

        with self._config.provide_connection() as conn:
            conn.execute(sql)
            conn.commit()
        self._log_delete_all()

    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        await async_(self._delete_all)()

    def _exists(self, key: str) -> bool:
        """Synchronous implementation of exists."""
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = ?
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        with self._config.provide_connection() as conn:
            cursor = conn.execute(sql, (key,))
            result = cursor.fetchone()
            return result is not None

    async def exists(self, key: str) -> bool:
        """Check if a session key exists and is not expired.

        Args:
            key: Session ID to check.

        Returns:
            True if the session exists and is not expired.
        """
        return await async_(self._exists)(key)

    def _expires_in(self, key: str) -> "int | None":
        """Synchronous implementation of expires_in."""
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = ?
        """

        with self._config.provide_connection() as conn:
            cursor = conn.execute(sql, (key,))
            row = cursor.fetchone()

            if row is None or row[0] is None:
                return None

            expires_at_str = row[0]
            expires_at = self._timestamp_to_datetime(expires_at_str)

            if expires_at is None:
                return None

            now = datetime.now(timezone.utc)

            if expires_at <= now:
                return 0

            delta = expires_at - now
            return int(delta.total_seconds())

    async def expires_in(self, key: str) -> "int | None":
        """Get the time in seconds until the session expires.

        Args:
            key: Session ID to check.

        Returns:
            Seconds until expiration, or None if no expiry or key doesn't exist.
        """
        return await async_(self._expires_in)(key)

    def _delete_expired(self) -> int:
        """Synchronous implementation of delete_expired."""
        sql = f"DELETE FROM {self._table_name} WHERE expires_at <= CURRENT_TIMESTAMP"

        with self._config.provide_connection() as conn:
            cursor = conn.execute(sql)
            count = cursor.fetchone()
            row_count = count[0] if count else 0
            if row_count > 0:
                self._log_delete_expired(row_count)
            return row_count

    async def delete_expired(self) -> int:
        """Delete all expired sessions.

        Returns:
            Number of sessions deleted.
        """
        return await async_(self._delete_expired)()
