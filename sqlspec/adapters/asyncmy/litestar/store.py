"""AsyncMy session store for Litestar integration."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.asyncmy.config import AsyncmyConfig

logger = get_logger("adapters.asyncmy.litestar.store")

__all__ = ("AsyncmyStore",)


class AsyncmyStore(BaseSQLSpecStore["AsyncmyConfig"]):
    """MySQL/MariaDB session store using AsyncMy driver.

    Implements server-side session storage for Litestar using MySQL/MariaDB
    via the AsyncMy driver. Provides efficient session management with:
    - Native async MySQL operations
    - UPSERT support using ON DUPLICATE KEY UPDATE
    - Automatic expiration handling
    - Efficient cleanup of expired sessions
    - Timezone-aware expiration (stored as UTC in DATETIME)

    Args:
        config: AsyncmyConfig instance.
        table_name: Name of the session table. Defaults to "sessions".
        cleanup_probability: Probability of running cleanup on set (0.0-1.0).

    Example:
        from sqlspec.adapters.asyncmy import AsyncmyConfig
        from sqlspec.adapters.asyncmy.litestar.store import AsyncmyStore

        config = AsyncmyConfig(pool_config={"host": "localhost", ...})
        store = AsyncmyStore(config)
        await store.create_table()

    Notes:
        MySQL DATETIME is timezone-naive, so UTC datetimes are stored without
        timezone info and timezone conversion is handled in Python layer.
    """

    __slots__ = ()

    def __init__(
        self, config: "AsyncmyConfig", table_name: str = "litestar_session", cleanup_probability: float = 0.01
    ) -> None:
        """Initialize AsyncMy session store.

        Args:
            config: AsyncmyConfig instance.
            table_name: Name of the session table.
            cleanup_probability: Probability of cleanup on set (0.0-1.0).
        """
        super().__init__(config, table_name, cleanup_probability)

    def _get_create_table_sql(self) -> str:
        """Get MySQL CREATE TABLE SQL with optimized schema.

        Returns:
            SQL statement to create the sessions table with proper indexes.

        Notes:
            - Uses DATETIME for timestamps (MySQL doesn't have TIMESTAMPTZ)
            - LONGBLOB for large session data support (up to 4GB)
            - InnoDB engine for ACID compliance and proper transaction support
            - UTF8MB4 for full Unicode support (including emoji)
            - Index on expires_at for efficient cleanup queries
            - Auto-update of updated_at timestamp on row modification
            - Table name is internally controlled, not user input (S608 suppressed)
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            session_id VARCHAR(255) PRIMARY KEY,
            data LONGBLOB NOT NULL,
            expires_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_{self._table_name}_expires_at (expires_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

    def _get_drop_table_sql(self) -> "list[str]":
        """Get MySQL/MariaDB DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop indexes and table.
        """
        return [
            f"DROP INDEX idx_{self._table_name}_expires_at ON {self._table_name}",
            f"DROP TABLE IF EXISTS {self._table_name}",
        ]

    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        sql = self._get_create_table_sql()
        async with self._config.provide_connection() as conn, conn.cursor() as cursor:
            await cursor.execute(sql)
        logger.debug("Created session table: %s", self._table_name)

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Get a session value by key.

        Args:
            key: Session ID to retrieve.
            renew_for: If given, renew the expiry time for this duration.

        Returns:
            Session data as bytes if found and not expired, None otherwise.

        Notes:
            Uses NOW() for current time in MySQL.
            Compares expires_at as UTC datetime (timezone-naive in MySQL).
        """
        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > NOW())
        """

        async with self._config.provide_connection() as conn, conn.cursor() as cursor:
            await cursor.execute(sql, (key,))
            row = await cursor.fetchone()

            if row is None:
                return None

            data_value, expires_at = row

            if renew_for is not None and expires_at is not None:
                new_expires_at = self._calculate_expires_at(renew_for)
                if new_expires_at is not None:
                    naive_expires_at = new_expires_at.replace(tzinfo=None)
                    update_sql = f"""
                        UPDATE {self._table_name}
                        SET expires_at = %s, updated_at = NOW()
                        WHERE session_id = %s
                        """
                    await cursor.execute(update_sql, (naive_expires_at, key))

            return bytes(data_value)

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Store a session value.

        Args:
            key: Session ID.
            value: Session data.
            expires_in: Time until expiration.

        Notes:
            Uses INSERT ... ON DUPLICATE KEY UPDATE for efficient UPSERT.
            Stores UTC datetime as timezone-naive DATETIME in MySQL.
            Uses alias syntax (AS new) instead of deprecated VALUES() function.
        """
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        naive_expires_at = expires_at.replace(tzinfo=None) if expires_at else None

        sql = f"""
        INSERT INTO {self._table_name} (session_id, data, expires_at)
        VALUES (%s, %s, %s) AS new
        ON DUPLICATE KEY UPDATE
            data = new.data,
            expires_at = new.expires_at,
            updated_at = NOW()
        """

        async with self._config.provide_connection() as conn, conn.cursor() as cursor:
            await cursor.execute(sql, (key, data, naive_expires_at))
            await conn.commit()

        if self._should_cleanup():
            await self.delete_expired()

    async def delete(self, key: str) -> None:
        """Delete a session by key.

        Args:
            key: Session ID to delete.
        """
        sql = f"DELETE FROM {self._table_name} WHERE session_id = %s"

        async with self._config.provide_connection() as conn, conn.cursor() as cursor:
            await cursor.execute(sql, (key,))

    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        sql = f"DELETE FROM {self._table_name}"

        async with self._config.provide_connection() as conn, conn.cursor() as cursor:
            await cursor.execute(sql)
        logger.debug("Deleted all sessions from table: %s", self._table_name)

    async def exists(self, key: str) -> bool:
        """Check if a session key exists and is not expired.

        Args:
            key: Session ID to check.

        Returns:
            True if the session exists and is not expired.

        Notes:
            Uses NOW() for current time comparison.
        """
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > NOW())
        """

        async with self._config.provide_connection() as conn, conn.cursor() as cursor:
            await cursor.execute(sql, (key,))
            result = await cursor.fetchone()
            return result is not None

    async def expires_in(self, key: str) -> "int | None":
        """Get the time in seconds until the session expires.

        Args:
            key: Session ID to check.

        Returns:
            Seconds until expiration, or None if no expiry or key doesn't exist.

        Notes:
            MySQL DATETIME is timezone-naive, but we treat it as UTC.
            Compare against UTC now in Python layer for accuracy.
        """
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = %s
        """

        async with self._config.provide_connection() as conn, conn.cursor() as cursor:
            await cursor.execute(sql, (key,))
            row = await cursor.fetchone()

            if row is None or row[0] is None:
                return None

            expires_at_naive = row[0]
            expires_at_utc = expires_at_naive.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)

            if expires_at_utc <= now:
                return 0

            delta = expires_at_utc - now
            return int(delta.total_seconds())

    async def delete_expired(self) -> int:
        """Delete all expired sessions.

        Returns:
            Number of sessions deleted.

        Notes:
            Uses NOW() for current time comparison.
            ROW_COUNT() returns the number of affected rows.
        """
        sql = f"DELETE FROM {self._table_name} WHERE expires_at <= NOW()"

        async with self._config.provide_connection() as conn, conn.cursor() as cursor:
            await cursor.execute(sql)
            count: int = cursor.rowcount
            if count > 0:
                logger.debug("Cleaned up %d expired sessions", count)
            return count
