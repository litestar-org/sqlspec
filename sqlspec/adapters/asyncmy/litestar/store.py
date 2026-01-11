"""AsyncMy session store for Litestar integration."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Final

from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.asyncmy.config import AsyncmyConfig

logger = get_logger("sqlspec.adapters.asyncmy.litestar.store")

__all__ = ("AsyncmyStore",)

MYSQL_TABLE_NOT_FOUND_ERROR: Final = 1146


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

    Example:
        from sqlspec.adapters.asyncmy import AsyncmyConfig
        from sqlspec.adapters.asyncmy.litestar.store import AsyncmyStore

        config = AsyncmyConfig(connection_config={"host": "localhost", ...})
        store = AsyncmyStore(config)
        await store.create_table()

    Notes:
        MySQL DATETIME is timezone-naive, so UTC datetimes are stored without
        timezone info and timezone conversion is handled in Python layer.
    """

    __slots__ = ()

    def __init__(self, config: "AsyncmyConfig") -> None:
        """Initialize AsyncMy session store.

        Args:
            config: AsyncmyConfig instance.

        Notes:
            Table name is read from config.extension_config["litestar"]["session_table"].
        """
        super().__init__(config)

    def _get_create_table_sql(self) -> str:
        """Get MySQL CREATE TABLE SQL with optimized schema.

        Returns:
            SQL statement to create the sessions table with proper indexes.

        Notes:
            - Uses DATETIME(6) for microsecond precision timestamps
            - MySQL doesn't have TIMESTAMPTZ, so we store UTC as timezone-naive
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
            expires_at DATETIME(6),
            created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
            updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
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
        async with self._config.provide_session() as driver:
            await driver.execute_script(sql)
        self._log_table_created()

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Get a session value by key.

        Args:
            key: Session ID to retrieve.
            renew_for: If given, renew the expiry time for this duration.

        Returns:
            Session data as bytes if found and not expired, None otherwise.

        Notes:
            Uses UTC_TIMESTAMP(6) for microsecond precision current time in MySQL.
            Compares expires_at as UTC datetime (timezone-naive in MySQL).
        """
        import asyncmy

        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP(6))
        """

        try:
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
                            SET expires_at = %s, updated_at = UTC_TIMESTAMP(6)
                            WHERE session_id = %s
                            """
                        await cursor.execute(update_sql, (naive_expires_at, key))
                        await conn.commit()

                return bytes(data_value)
        except asyncmy.errors.ProgrammingError as e:  # pyright: ignore
            if "doesn't exist" in str(e) or e.args[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return None
            raise

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
            updated_at = UTC_TIMESTAMP(6)
        """

        async with self._config.provide_connection() as conn, conn.cursor() as cursor:
            await cursor.execute(sql, (key, data, naive_expires_at))
            await conn.commit()

    async def delete(self, key: str) -> None:
        """Delete a session by key.

        Args:
            key: Session ID to delete.
        """
        sql = f"DELETE FROM {self._table_name} WHERE session_id = %s"

        async with self._config.provide_connection() as conn, conn.cursor() as cursor:
            await cursor.execute(sql, (key,))
            await conn.commit()

    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        import asyncmy

        sql = f"DELETE FROM {self._table_name}"

        try:
            async with self._config.provide_connection() as conn, conn.cursor() as cursor:
                await cursor.execute(sql)
                await conn.commit()
            self._log_delete_all()
        except asyncmy.errors.ProgrammingError as e:  # pyright: ignore
            if "doesn't exist" in str(e) or e.args[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                logger.debug("Table %s does not exist, skipping delete_all", self._table_name)
                return
            raise

    async def exists(self, key: str) -> bool:
        """Check if a session key exists and is not expired.

        Args:
            key: Session ID to check.

        Returns:
            True if the session exists and is not expired.

        Notes:
            Uses UTC_TIMESTAMP(6) for microsecond precision current time comparison.
        """
        import asyncmy

        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP(6))
        """

        try:
            async with self._config.provide_connection() as conn, conn.cursor() as cursor:
                await cursor.execute(sql, (key,))
                result = await cursor.fetchone()
                return result is not None
        except asyncmy.errors.ProgrammingError as e:  # pyright: ignore
            if "doesn't exist" in str(e) or e.args[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return False
            raise

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
            Uses UTC_TIMESTAMP(6) for microsecond precision current time comparison.
            ROW_COUNT() returns the number of affected rows.
        """
        sql = f"DELETE FROM {self._table_name} WHERE expires_at <= UTC_TIMESTAMP(6)"

        async with self._config.provide_connection() as conn, conn.cursor() as cursor:
            await cursor.execute(sql)
            await conn.commit()
            count: int = cursor.rowcount
            if count > 0:
                self._log_delete_expired(count)
            return count
