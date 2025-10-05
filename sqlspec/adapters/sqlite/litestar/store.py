"""SQLite sync session store for Litestar integration."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from litestar.utils.sync import AsyncCallable

from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.sqlite.config import SqliteConfig

logger = get_logger("adapters.sqlite.litestar.store")

__all__ = ("SQLiteStore",)


class SQLiteStore(BaseSQLSpecStore):
    """SQLite session store using synchronous SQLite driver.

    Implements server-side session storage for Litestar using SQLite
    via the synchronous sqlite3 driver. Uses Litestar's sync_to_thread
    utility to provide an async interface compatible with the Store protocol.

    Provides efficient session management with:
    - Sync operations wrapped for async compatibility
    - INSERT OR REPLACE for UPSERT functionality
    - Automatic expiration handling
    - Efficient cleanup of expired sessions

    Note:
        For high-concurrency applications, consider using AioSQLiteStore instead,
        as it provides native async operations without threading overhead.

    Args:
        config: SqliteConfig instance.
        table_name: Name of the session table. Defaults to "sessions".
        cleanup_probability: Probability of running cleanup on set (0.0-1.0).

    Example:
        from sqlspec.adapters.sqlite import SqliteConfig
        from sqlspec.adapters.sqlite.litestar.store import SQLiteStore

        config = SqliteConfig(database=":memory:")
        store = SQLiteStore(config)
        await store.create_table()
    """

    __slots__ = ()

    def __init__(
        self,
        config: "SqliteConfig",
        table_name: str = "sessions",
        cleanup_probability: float = 0.01,
    ) -> None:
        """Initialize SQLite session store.

        Args:
            config: SqliteConfig instance.
            table_name: Name of the session table.
            cleanup_probability: Probability of cleanup on set (0.0-1.0).
        """
        super().__init__(config, table_name, cleanup_probability)

    def _get_create_table_sql(self) -> str:
        """Get SQLite CREATE TABLE SQL.

        Returns:
            SQL statement to create the sessions table with proper indexes.

        Notes:
            - Uses REAL type for expires_at (stores Julian Day number)
            - Julian Day enables direct comparison with julianday('now')
            - Partial index WHERE expires_at IS NOT NULL reduces index size
            - This approach ensures the index is actually used by query optimizer
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            session_id TEXT PRIMARY KEY,
            data BLOB NOT NULL,
            expires_at REAL
        );
        CREATE INDEX IF NOT EXISTS idx_{self._table_name}_expires_at
        ON {self._table_name}(expires_at) WHERE expires_at IS NOT NULL;
        """

    def _datetime_to_julian(self, dt: "datetime | None") -> "float | None":
        """Convert datetime to Julian Day number for SQLite storage.

        Args:
            dt: Datetime to convert (must be UTC-aware).

        Returns:
            Julian Day number as REAL, or None if dt is None.

        Notes:
            Julian Day number is days since November 24, 4714 BCE (proleptic Gregorian).
            This enables direct comparison with julianday('now') in SQL queries.
        """
        if dt is None:
            return None

        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        julian_epoch = 2440587.5
        delta_days = (dt - epoch).total_seconds() / 86400.0
        return julian_epoch + delta_days

    def _julian_to_datetime(self, julian: "float | None") -> "datetime | None":
        """Convert Julian Day number back to datetime.

        Args:
            julian: Julian Day number.

        Returns:
            UTC-aware datetime, or None if julian is None.
        """
        if julian is None:
            return None

        julian_epoch = 2440587.5
        days_since_epoch = julian - julian_epoch
        timestamp = days_since_epoch * 86400.0
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)

    def _create_table_sync(self) -> None:
        """Synchronous implementation of create_table."""
        sql = self._get_create_table_sql()
        with self._config.provide_connection() as conn:
            conn.executescript(sql)
        logger.debug("Created session table: %s", self._table_name)

    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        await AsyncCallable(self._create_table_sync)()

    def _get_sync(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Synchronous implementation of get."""
        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = ?
        AND (expires_at IS NULL OR julianday(expires_at) > julianday('now'))
        """

        with self._config.provide_connection() as conn:
            cursor = conn.execute(sql, (key,))
            row = cursor.fetchone()

            if row is None:
                return None

            data, expires_at_julian = row

            if renew_for is not None and expires_at_julian is not None:
                new_expires_at = self._calculate_expires_at(renew_for)
                new_expires_at_julian = self._datetime_to_julian(new_expires_at)
                if new_expires_at_julian is not None:
                    update_sql = f"""
                    UPDATE {self._table_name}
                    SET expires_at = ?
                    WHERE session_id = ?
                    """
                    conn.execute(update_sql, (new_expires_at_julian, key))
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
        return await AsyncCallable(self._get_sync)(key, renew_for)

    def _set_sync(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Synchronous implementation of set.

        Notes:
            Stores expires_at as Julian Day number (REAL) for optimal index usage.
        """
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        expires_at_julian = self._datetime_to_julian(expires_at)

        sql = f"""
        INSERT OR REPLACE INTO {self._table_name} (session_id, data, expires_at)
        VALUES (?, ?, ?)
        """

        with self._config.provide_connection() as conn:
            conn.execute(sql, (key, data, expires_at_julian))
            conn.commit()

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Store a session value.

        Args:
            key: Session ID.
            value: Session data.
            expires_in: Time until expiration.
        """
        await AsyncCallable(self._set_sync)(key, value, expires_in)

        if self._should_cleanup():
            await self.delete_expired()

    def _delete_sync(self, key: str) -> None:
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
        await AsyncCallable(self._delete_sync)(key)

    def _delete_all_sync(self) -> None:
        """Synchronous implementation of delete_all."""
        sql = f"DELETE FROM {self._table_name}"

        with self._config.provide_connection() as conn:
            conn.execute(sql)
            conn.commit()
        logger.debug("Deleted all sessions from table: %s", self._table_name)

    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        await AsyncCallable(self._delete_all_sync)()

    def _exists_sync(self, key: str) -> bool:
        """Synchronous implementation of exists."""
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = ?
        AND (expires_at IS NULL OR julianday(expires_at) > julianday('now'))
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
        return await AsyncCallable(self._exists_sync)(key)

    def _expires_in_sync(self, key: str) -> "int | None":
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

            expires_at_julian = row[0]
            expires_at = self._julian_to_datetime(expires_at_julian)

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
        return await AsyncCallable(self._expires_in_sync)(key)

    def _delete_expired_sync(self) -> int:
        """Synchronous implementation of delete_expired."""
        sql = f"DELETE FROM {self._table_name} WHERE julianday(expires_at) <= julianday('now')"

        with self._config.provide_connection() as conn:
            cursor = conn.execute(sql)
            conn.commit()
            count = cursor.rowcount
            if count > 0:
                logger.debug("Cleaned up %d expired sessions", count)
            return count

    async def delete_expired(self) -> int:
        """Delete all expired sessions.

        Returns:
            Number of sessions deleted.
        """
        return await AsyncCallable(self._delete_expired_sync)()
