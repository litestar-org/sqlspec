"""MysqlConnector session store for Litestar integration."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Final, cast

import mysql.connector

from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.logging import get_logger
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.adapters.mysqlconnector.config import MysqlConnectorAsyncConfig, MysqlConnectorSyncConfig

logger = get_logger("sqlspec.adapters.mysqlconnector.litestar.store")

__all__ = ("MysqlConnectorAsyncStore", "MysqlConnectorSyncStore")

MYSQL_TABLE_NOT_FOUND_ERROR: Final = 1146


class MysqlConnectorAsyncStore(BaseSQLSpecStore["MysqlConnectorAsyncConfig"]):
    """MySQL/MariaDB session store using mysql-connector async driver."""

    __slots__ = ()

    def __init__(self, config: "MysqlConnectorAsyncConfig") -> None:
        super().__init__(config)

    def _get_create_table_sql(self) -> str:
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
        return [
            f"DROP INDEX idx_{self._table_name}_expires_at ON {self._table_name}",
            f"DROP TABLE IF EXISTS {self._table_name}",
        ]

    async def create_table(self) -> None:
        sql = self._get_create_table_sql()
        async with self._config.provide_session() as driver:
            await driver.execute_script(sql)
        self._log_table_created()

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
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


class MysqlConnectorSyncStore(BaseSQLSpecStore["MysqlConnectorSyncConfig"]):
    """MySQL/MariaDB session store using mysql-connector sync driver."""

    __slots__ = ()

    def __init__(self, config: "MysqlConnectorSyncConfig") -> None:
        super().__init__(config)

    def _get_create_table_sql(self) -> str:
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
        return [
            f"DROP INDEX idx_{self._table_name}_expires_at ON {self._table_name}",
            f"DROP TABLE IF EXISTS {self._table_name}",
        ]

    def _create_table(self) -> None:
        sql = self._get_create_table_sql()
        with self._config.provide_session() as driver:
            driver.execute_script(sql)
            driver.commit()
        self._log_table_created()

    async def create_table(self) -> None:
        await async_(self._create_table)()

    def _get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
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

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        return await async_(self._get)(key, renew_for)

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

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        await async_(self._set)(key, value, expires_in)

    def _delete(self, key: str) -> None:
        sql = f"DELETE FROM {self._table_name} WHERE session_id = %s"

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (key,))
            finally:
                cursor.close()
            conn.commit()

    async def delete(self, key: str) -> None:
        await async_(self._delete)(key)

    def _delete_all(self) -> None:
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

    async def delete_all(self) -> None:
        await async_(self._delete_all)()

    def _exists(self, key: str) -> bool:
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

    async def exists(self, key: str) -> bool:
        return await async_(self._exists)(key)

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

    async def expires_in(self, key: str) -> "int | None":
        return await async_(self._expires_in)(key)

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

    async def delete_expired(self) -> int:
        return await async_(self._delete_expired)()
