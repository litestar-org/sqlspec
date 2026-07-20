"""mssql-python Litestar Store implementation."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.adapters.mssql_python.config import MssqlPythonConfig

__all__ = ("MssqlPythonStore",)


class MssqlPythonStore(BaseSQLSpecStore["MssqlPythonConfig"]):
    """SQL Server-backed session store using mssql-python sync sessions."""

    __slots__ = ()

    def __init__(self, config: "MssqlPythonConfig") -> None:
        super().__init__(config)

    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        await async_(self._create_table)()
        await self.reconcile_schema(assume_existing=True)

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Get a session value by key."""
        return await async_(self._get)(key, renew_for)

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Store a session value."""
        await async_(self._set)(key, value, expires_in)

    async def delete(self, key: str) -> None:
        """Delete a session by key."""
        await async_(self._delete)(key)

    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        await async_(self._delete_all)()

    async def exists(self, key: str) -> bool:
        """Check if a session key exists and is not expired."""
        return await async_(self._exists)(key)

    async def expires_in(self, key: str) -> "int | None":
        """Get the time in seconds until the session expires."""
        return await async_(self._expires_in)(key)

    async def delete_expired(self) -> int:
        """Delete all expired sessions."""
        return await async_(self._delete_expired)()

    def _table_ddl(self) -> str:
        """Get SQL Server CREATE TABLE SQL with idempotent guards."""
        return f"""
        IF NOT EXISTS (
            SELECT 1
            FROM sys.tables
            WHERE name = N'{self._table_name}'
              AND schema_id = SCHEMA_ID(N'dbo')
        )
        BEGIN
            CREATE TABLE {self._table_name} (
                session_id NVARCHAR(255) PRIMARY KEY,
                data VARBINARY(MAX) NOT NULL,
                expires_at DATETIME2(6) NULL,
                created_at DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
                updated_at DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME()
            );

            CREATE INDEX IX_{self._table_name}_expires_at
                ON {self._table_name}(expires_at)
                WHERE expires_at IS NOT NULL;
        END;
        """

    def _drop_table_sql(self) -> "list[str]":
        """Get SQL Server DROP TABLE statements."""
        return [f"IF OBJECT_ID(N'dbo.{self._table_name}', N'U') IS NOT NULL DROP TABLE dbo.{self._table_name};"]

    def _create_table(self) -> None:
        with self._config.provide_session() as driver:
            driver.execute_script(self._table_ddl())
            driver.commit()
        self._log_table_created()

    def _get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = %s
          AND (expires_at IS NULL OR expires_at > SYSUTCDATETIME())
        """
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (key,))
                row = cursor.fetchone()
            finally:
                cursor.close()

            if row is None:
                return None

            expires_at = _normalize_utc(_row_value(row, "expires_at", 1))
            if renew_for is not None and expires_at is not None:
                new_expires_at = self._calculate_expires_at(renew_for)
                if new_expires_at is not None:
                    update_cursor = conn.cursor()
                    try:
                        update_cursor.execute(
                            f"""
                            UPDATE {self._table_name}
                            SET expires_at = %s, updated_at = SYSUTCDATETIME()
                            WHERE session_id = %s
                            """,
                            (new_expires_at, key),
                        )
                    finally:
                        update_cursor.close()
                    conn.commit()

            return _coerce_bytes(_row_value(row, "data", 0))

    def _set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        sql = f"""
        MERGE INTO {self._table_name} AS target
        USING (SELECT %s AS session_id, %s AS data, %s AS expires_at) AS src
           ON target.session_id = src.session_id
        WHEN MATCHED THEN
            UPDATE SET
                data = src.data,
                expires_at = src.expires_at,
                updated_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (session_id, data, expires_at)
            VALUES (src.session_id, src.data, src.expires_at);
        """
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (key, data, expires_at))
            finally:
                cursor.close()
            conn.commit()

    def _delete(self, key: str) -> None:
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f"DELETE FROM {self._table_name} WHERE session_id = %s", (key,))
            finally:
                cursor.close()
            conn.commit()

    def _delete_all(self) -> None:
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f"TRUNCATE TABLE {self._table_name}")
            finally:
                cursor.close()
            conn.commit()
        self._log_delete_all()

    def _exists(self, key: str) -> bool:
        sql = f"""
        SELECT 1
        FROM {self._table_name}
        WHERE session_id = %s
          AND (expires_at IS NULL OR expires_at > SYSUTCDATETIME())
        """
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (key,))
                return cursor.fetchone() is not None
            finally:
                cursor.close()

    def _expires_in(self, key: str) -> "int | None":
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f"SELECT expires_at FROM {self._table_name} WHERE session_id = %s", (key,))
                row = cursor.fetchone()
            finally:
                cursor.close()

        if row is None:
            return None
        expires_at = _normalize_utc(_row_value(row, "expires_at", 0))
        if expires_at is None:
            return None
        remaining = expires_at - datetime.now(timezone.utc)
        return max(0, int(remaining.total_seconds()))

    def _delete_expired(self) -> int:
        sql = f"""
        DELETE FROM {self._table_name}
        WHERE expires_at IS NOT NULL
          AND expires_at < SYSUTCDATETIME()
        """
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                count = int(getattr(cursor, "rowcount", 0) or 0)
            finally:
                cursor.close()
            conn.commit()
        if count > 0:
            self._log_delete_expired(count)
        return count


def _row_value(row: object, key: str, index: int) -> Any:
    """Return a value from dict-like or sequence-like driver rows."""
    if isinstance(row, dict):
        if key in row:
            return row[key]
        upper_key = key.upper()
        if upper_key in row:
            return row[upper_key]
        return None
    if isinstance(row, (list, tuple)) and len(row) > index:
        return row[index]
    return getattr(row, key, None)


def _normalize_utc(value: Any) -> "datetime | None":
    if value is None:
        return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _coerce_bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, str):
        return value.encode("utf-8")
    return bytes(value)
