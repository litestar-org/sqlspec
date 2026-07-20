"""aiomysql session store for Litestar integration."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Final, cast

import pymysql.err

from sqlspec.adapters.aiomysql._typing import AiomysqlCursor, AiomysqlRawCursor
from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.aiomysql.config import AiomysqlConfig

__all__ = ("AiomysqlStore",)

logger = get_logger("sqlspec.adapters.aiomysql.litestar.store")


MYSQL_TABLE_NOT_FOUND_ERROR: Final = 1146




class AiomysqlStore(BaseSQLSpecStore["AiomysqlConfig"]):
    """MySQL/MariaDB session store using aiomysql driver.

    Implements server-side session storage for Litestar using MySQL/MariaDB
    via the aiomysql driver. Provides efficient session management with:
    - Native async MySQL operations
    - UPSERT support using ON DUPLICATE KEY UPDATE
    - Automatic expiration handling
    - Efficient cleanup of expired sessions
    - Timezone-aware expiration (stored as UTC in DATETIME)

    Args:
        config: AiomysqlConfig instance.
    """

    __slots__ = ("_table_options",)

    def __init__(self, config: "AiomysqlConfig") -> None:
        """Initialize aiomysql session store.

        Args:
            config: AiomysqlConfig instance.
        """
        super().__init__(config)
        litestar_config = cast("dict[str, Any]", config.extension_config.get("litestar", {}))
        self._table_options: str = _mysql_table_options(litestar_config)

    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        sql = self._table_ddl()
        async with self._config.provide_session() as driver:
            await driver.execute_script(sql)
        self._log_table_created()
        await self.reconcile_schema(assume_existing=True)

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
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP(6))
        """

        try:
            async with (
                self._config.provide_connection() as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
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
        except pymysql.err.ProgrammingError as e:
            if "doesn't exist" in str(e) or e.args[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Store a session value.

        Args:
            key: Session ID.
            value: Session data.
            expires_in: Time until expiration.
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

        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
            await cursor.execute(sql, (key, data, naive_expires_at))
            await conn.commit()

    async def delete(self, key: str) -> None:
        """Delete a session by key.

        Args:
            key: Session ID to delete.
        """
        sql = f"DELETE FROM {self._table_name} WHERE session_id = %s"

        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
            await cursor.execute(sql, (key,))
            await conn.commit()

    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        sql = f"DELETE FROM {self._table_name}"

        try:
            async with (
                self._config.provide_connection() as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                await cursor.execute(sql)
                await conn.commit()
            self._log_delete_all()
        except pymysql.err.ProgrammingError as e:
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
        """
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP(6))
        """

        try:
            async with (
                self._config.provide_connection() as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                await cursor.execute(sql, (key,))
                result = await cursor.fetchone()
                return result is not None
        except pymysql.err.ProgrammingError as e:
            if "doesn't exist" in str(e) or e.args[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return False
            raise

    async def expires_in(self, key: str) -> "int | None":
        """Get the time in seconds until the session expires.

        Args:
            key: Session ID to check.

        Returns:
            Seconds until expiration, or None if no expiry or key doesn't exist.
        """
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = %s
        """

        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
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
        """
        sql = f"DELETE FROM {self._table_name} WHERE expires_at <= UTC_TIMESTAMP(6)"

        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
            await cursor.execute(sql)
            await conn.commit()
            count: int = cursor.rowcount
            if count > 0:
                self._log_delete_expired(count)
            return count

    def _table_ddl(self) -> str:
        """Get MySQL CREATE TABLE SQL with optimized schema.

        Returns:
            SQL statement to create the sessions table with proper indexes.
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            session_id VARCHAR(255) PRIMARY KEY,
            data LONGBLOB NOT NULL,
            expires_at DATETIME(6),
            created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
            updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
            INDEX idx_{self._table_name}_expires_at (expires_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{self._table_options}
        """

    def _drop_table_sql(self) -> "list[str]":
        """Get MySQL/MariaDB DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop indexes and table.
        """
        return [
            f"DROP INDEX idx_{self._table_name}_expires_at ON {self._table_name}",
            f"DROP TABLE IF EXISTS {self._table_name}",
        ]


def _mysql_table_options(litestar_config: "dict[str, Any]") -> str:
    """Format the litestar ``table_options`` config value for DDL interpolation.

    Args:
        litestar_config: The ``extension_config["litestar"]`` mapping.

    Returns:
        A leading-space-prefixed options string, or an empty string when unset.
    """
    value = litestar_config.get("table_options")
    if not isinstance(value, str):
        return ""
    value = value.strip()
    return f" {value}" if value else ""
