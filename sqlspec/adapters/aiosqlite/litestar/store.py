"""AioSQLite session store for Litestar integration."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, cast

from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager

    from sqlspec.adapters.aiosqlite._types import AiosqliteConnection
    from sqlspec.adapters.aiosqlite.config import AiosqliteConfig

logger = get_logger("adapters.aiosqlite.litestar.store")

__all__ = ("AioSQLiteStore",)


class AioSQLiteStore(BaseSQLSpecStore):
    """SQLite session store using AioSQLite driver.

    Implements server-side session storage for Litestar using SQLite
    via the AioSQLite driver. Provides efficient session management with:
    - Native async SQLite operations
    - INSERT OR REPLACE for UPSERT functionality
    - Automatic expiration handling
    - Efficient cleanup of expired sessions

    Args:
        config: AiosqliteConfig instance.
        table_name: Name of the session table. Defaults to "sessions".
        cleanup_probability: Probability of running cleanup on set (0.0-1.0).

    Example:
        from sqlspec.adapters.aiosqlite import AiosqliteConfig
        from sqlspec.adapters.aiosqlite.litestar.store import AioSQLiteStore

        config = AiosqliteConfig(database=":memory:")
        store = AioSQLiteStore(config)
        await store.create_table()
    """

    __slots__ = ()

    def __init__(
        self,
        config: "AiosqliteConfig",
        table_name: str = "sessions",
        cleanup_probability: float = 0.01,
    ) -> None:
        """Initialize AioSQLite session store.

        Args:
            config: AiosqliteConfig instance.
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

    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        sql = self._get_create_table_sql()
        async with cast("AbstractAsyncContextManager[AiosqliteConnection]", self._config.provide_connection()) as conn:
            await conn.executescript(sql)
        logger.debug("Created session table: %s", self._table_name)

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Get a session value by key.

        Args:
            key: Session ID to retrieve.
            renew_for: If given, renew the expiry time for this duration.

        Returns:
            Session data as bytes if found and not expired, None otherwise.
        """
        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = ?
        AND (expires_at IS NULL OR julianday(expires_at) > julianday('now'))
        """

        async with cast("AbstractAsyncContextManager[AiosqliteConnection]", self._config.provide_connection()) as conn:
            async with conn.execute(sql, (key,)) as cursor:
                row = await cursor.fetchone()

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
                    await conn.execute(update_sql, (new_expires_at_julian, key))
                    await conn.commit()

            return bytes(data)

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Store a session value.

        Args:
            key: Session ID.
            value: Session data.
            expires_in: Time until expiration.

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

        async with cast("AbstractAsyncContextManager[AiosqliteConnection]", self._config.provide_connection()) as conn:
            await conn.execute(sql, (key, data, expires_at_julian))
            await conn.commit()

        if self._should_cleanup():
            await self.delete_expired()

    async def delete(self, key: str) -> None:
        """Delete a session by key.

        Args:
            key: Session ID to delete.
        """
        sql = f"DELETE FROM {self._table_name} WHERE session_id = ?"

        async with cast("AbstractAsyncContextManager[AiosqliteConnection]", self._config.provide_connection()) as conn:
            await conn.execute(sql, (key,))
            await conn.commit()

    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        sql = f"DELETE FROM {self._table_name}"

        async with cast("AbstractAsyncContextManager[AiosqliteConnection]", self._config.provide_connection()) as conn:
            await conn.execute(sql)
            await conn.commit()
        logger.debug("Deleted all sessions from table: %s", self._table_name)

    async def exists(self, key: str) -> bool:
        """Check if a session key exists and is not expired.

        Args:
            key: Session ID to check.

        Returns:
            True if the session exists and is not expired.
        """
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = ?
        AND (expires_at IS NULL OR julianday(expires_at) > julianday('now'))
        """

        async with cast("AbstractAsyncContextManager[AiosqliteConnection]", self._config.provide_connection()) as conn:
            async with conn.execute(sql, (key,)) as cursor:
                result = await cursor.fetchone()
                return result is not None

    async def expires_in(self, key: str) -> "int | None":
        """Get the time in seconds until the session expires.

        Args:
            key: Session ID to check.

        Returns:
            Seconds until expiration, or None if no expiry or key doesn't exist.
        """
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = ?
        """

        async with cast("AbstractAsyncContextManager[AiosqliteConnection]", self._config.provide_connection()) as conn:
            async with conn.execute(sql, (key,)) as cursor:
                row = await cursor.fetchone()

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

    async def delete_expired(self) -> int:
        """Delete all expired sessions.

        Returns:
            Number of sessions deleted.
        """
        sql = f"DELETE FROM {self._table_name} WHERE julianday(expires_at) <= julianday('now')"

        async with cast("AbstractAsyncContextManager[AiosqliteConnection]", self._config.provide_connection()) as conn:
            cursor = await conn.execute(sql)
            await conn.commit()
            count = cursor.rowcount
            if count > 0:
                logger.debug("Cleaned up %d expired sessions", count)
            return count
