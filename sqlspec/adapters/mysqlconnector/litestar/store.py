"""MysqlConnector session store for Litestar integration."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Final, cast

from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.logging import get_logger
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.adapters.mysqlconnector.config import MysqlConnectorAsyncConfig, MysqlConnectorSyncConfig

__all__ = ("MysqlConnectorAsyncStore", "MysqlConnectorSyncStore")

logger = get_logger("sqlspec.adapters.mysqlconnector.litestar.store")


MYSQL_TABLE_NOT_FOUND_ERROR: Final = 1146


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


class MysqlConnectorAsyncStore(BaseSQLSpecStore["MysqlConnectorAsyncConfig"]):
    """MySQL/MariaDB session store using mysql-connector async driver."""

    __slots__ = ("_table_options",)

    def __init__(self, config: "MysqlConnectorAsyncConfig") -> None:
        super().__init__(config)
        litestar_config = cast("dict[str, Any]", config.extension_config.get("litestar", {}))
        self._table_options: str = _mysql_table_options(litestar_config)

    async def create_table(self) -> None:
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        sql = self._table_ddl()
        async with self._config.provide_session() as driver:
            await driver.execute_script(sql)
        self._log_table_created()
        await self.reconcile_schema(assume_existing=True)

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        import mysql.connector

        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP(6))
        """

        try:
            async with self._config.provide_connection() as conn:
                cursor = await conn.cursor()
                try:
                    await cursor.execute(sql, (key,))
                    row = await cursor.fetchone()
                finally:
                    await cursor.close()

                if row is None:
                    return None

                data_value: Any = row[0]
                expires_at: Any = row[1]

                if renew_for is not None and expires_at is not None:
                    new_expires_at = self._calculate_expires_at(renew_for)
                    if new_expires_at is not None:
                        naive_expires_at = new_expires_at.replace(tzinfo=None)
                        update_sql = f"""
                            UPDATE {self._table_name}
                            SET expires_at = %s, updated_at = UTC_TIMESTAMP(6)
                            WHERE session_id = %s
                            """
                        update_cursor = await conn.cursor()
                        try:
                            await update_cursor.execute(update_sql, (naive_expires_at, key))
                        finally:
                            await update_cursor.close()
                        await conn.commit()

                return bytes(cast("bytes", data_value))
        except mysql.connector.Error as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "errno", None) == MYSQL_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
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

        async with self._config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute(sql, (key, data, naive_expires_at))
            finally:
                await cursor.close()
            await conn.commit()

    async def delete(self, key: str) -> None:
        sql = f"DELETE FROM {self._table_name} WHERE session_id = %s"

        async with self._config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute(sql, (key,))
            finally:
                await cursor.close()
            await conn.commit()

    async def delete_all(self) -> None:
        import mysql.connector

        sql = f"DELETE FROM {self._table_name}"

        try:
            async with self._config.provide_connection() as conn:
                cursor = await conn.cursor()
                try:
                    await cursor.execute(sql)
                finally:
                    await cursor.close()
                await conn.commit()
            self._log_delete_all()
        except mysql.connector.Error as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "errno", None) == MYSQL_TABLE_NOT_FOUND_ERROR:
                logger.debug("Table %s does not exist, skipping delete_all", self._table_name)
                return
            raise

    async def exists(self, key: str) -> bool:
        import mysql.connector

        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP(6))
        """

        try:
            async with self._config.provide_connection() as conn:
                cursor = await conn.cursor()
                try:
                    await cursor.execute(sql, (key,))
                    result = await cursor.fetchone()
                finally:
                    await cursor.close()
                return result is not None
        except mysql.connector.Error as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "errno", None) == MYSQL_TABLE_NOT_FOUND_ERROR:
                return False
            raise

    async def expires_in(self, key: str) -> "int | None":
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = %s
        """

        async with self._config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute(sql, (key,))
                row = await cursor.fetchone()
            finally:
                await cursor.close()

            if row is None or row[0] is None:
                return None

            expires_at_naive: datetime = cast("datetime", row[0])
            expires_at_utc = expires_at_naive.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)

            if expires_at_utc <= now:
                return 0

            delta = expires_at_utc - now
            return int(delta.total_seconds())

    async def delete_expired(self) -> int:
        sql = f"DELETE FROM {self._table_name} WHERE expires_at <= UTC_TIMESTAMP(6)"

        async with self._config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute(sql)
                await conn.commit()
                count: int = cursor.rowcount
            finally:
                await cursor.close()
            if count > 0:
                self._log_delete_expired(count)
            return count

    def _table_ddl(self) -> str:
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
        return [
            f"DROP INDEX idx_{self._table_name}_expires_at ON {self._table_name}",
            f"DROP TABLE IF EXISTS {self._table_name}",
        ]


class MysqlConnectorSyncStore(BaseSQLSpecStore["MysqlConnectorSyncConfig"]):
    """MySQL/MariaDB session store using mysql-connector sync driver."""

    __slots__ = ("_table_options",)

    def __init__(self, config: "MysqlConnectorSyncConfig") -> None:
        super().__init__(config)
        litestar_config = cast("dict[str, Any]", config.extension_config.get("litestar", {}))
        self._table_options: str = _mysql_table_options(litestar_config)

    async def create_table(self) -> None:
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        await async_(self._create_table)()
        await self.reconcile_schema(assume_existing=True)

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        return await async_(self._get)(key, renew_for)

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        await async_(self._set)(key, value, expires_in)

    async def delete(self, key: str) -> None:
        await async_(self._delete)(key)

    async def delete_all(self) -> None:
        await async_(self._delete_all)()

    async def exists(self, key: str) -> bool:
        return await async_(self._exists)(key)

    async def expires_in(self, key: str) -> "int | None":
        return await async_(self._expires_in)(key)

    async def delete_expired(self) -> int:
        return await async_(self._delete_expired)()

    def _table_ddl(self) -> str:
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
        return [
            f"DROP INDEX idx_{self._table_name}_expires_at ON {self._table_name}",
            f"DROP TABLE IF EXISTS {self._table_name}",
        ]

    def _create_table(self) -> None:
        sql = self._table_ddl()
        with self._config.provide_session() as driver:
            driver.execute_script(sql)
            driver.commit()
        self._log_table_created()

    def _get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        import mysql.connector

        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP(6))
        """

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, (key,))
                    row = cursor.fetchone()
                finally:
                    cursor.close()

                if row is None:
                    return None

                data_value: Any = row[0]
                expires_at: Any = row[1]

                if renew_for is not None and expires_at is not None:
                    new_expires_at = self._calculate_expires_at(renew_for)
                    if new_expires_at is not None:
                        naive_expires_at = new_expires_at.replace(tzinfo=None)
                        update_sql = f"""
                            UPDATE {self._table_name}
                            SET expires_at = %s, updated_at = UTC_TIMESTAMP(6)
                            WHERE session_id = %s
                            """
                        update_cursor = conn.cursor()
                        try:
                            update_cursor.execute(update_sql, (naive_expires_at, key))
                        finally:
                            update_cursor.close()
                        conn.commit()

                return bytes(cast("bytes", data_value))
        except mysql.connector.Error as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "errno", None) == MYSQL_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    def _set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
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

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (key, data, naive_expires_at))
            finally:
                cursor.close()
            conn.commit()

    def _delete(self, key: str) -> None:
        sql = f"DELETE FROM {self._table_name} WHERE session_id = %s"

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (key,))
            finally:
                cursor.close()
            conn.commit()

    def _delete_all(self) -> None:
        import mysql.connector

        sql = f"DELETE FROM {self._table_name}"

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql)
                finally:
                    cursor.close()
                conn.commit()
            self._log_delete_all()
        except mysql.connector.Error as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "errno", None) == MYSQL_TABLE_NOT_FOUND_ERROR:
                logger.debug("Table %s does not exist, skipping delete_all", self._table_name)
                return
            raise

    def _exists(self, key: str) -> bool:
        import mysql.connector

        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP(6))
        """

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, (key,))
                    result = cursor.fetchone()
                finally:
                    cursor.close()
                return result is not None
        except mysql.connector.Error as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "errno", None) == MYSQL_TABLE_NOT_FOUND_ERROR:
                return False
            raise

    def _expires_in(self, key: str) -> "int | None":
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = %s
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (key,))
                row = cursor.fetchone()
            finally:
                cursor.close()

            if row is None or row[0] is None:
                return None

            expires_at_naive: datetime = cast("datetime", row[0])
            expires_at_utc = expires_at_naive.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)

            if expires_at_utc <= now:
                return 0

            delta = expires_at_utc - now
            return int(delta.total_seconds())

    def _delete_expired(self) -> int:
        sql = f"DELETE FROM {self._table_name} WHERE expires_at <= UTC_TIMESTAMP(6)"

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                conn.commit()
                count: int = cursor.rowcount
            finally:
                cursor.close()
            if count > 0:
                self._log_delete_expired(count)
            return count
