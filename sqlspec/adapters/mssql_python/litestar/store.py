"""mssql-python Litestar Store implementation."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from sqlspec.extensions.litestar.store import BaseSQLSpecStore

if TYPE_CHECKING:
    from sqlspec.adapters.mssql_python.config import MssqlPythonAsyncConfig

__all__ = ("MssqlPythonStore",)


class MssqlPythonStore(BaseSQLSpecStore["MssqlPythonAsyncConfig"]):
    """SQL Server-backed session store using mssql-python async sessions."""

    __slots__ = ()

    def __init__(self, config: "MssqlPythonAsyncConfig") -> None:
        super().__init__(config)

    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        async with self._config.provide_session() as session:
            await session.execute_script(self._get_create_table_sql())
        self._log_table_created()

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Get a session value by key."""
        async with self._config.provide_session() as session:
            row = await session.select_one_or_none(
                f"SELECT data, expires_at FROM {self._table_name} WHERE session_id = ?", (key,)
            )
            if row is None:
                return None

            expires_at = _normalize_utc(_row_value(row, "expires_at", 1))
            if expires_at is not None and expires_at < datetime.now(timezone.utc):
                await self.delete(key)
                return None

            if renew_for is not None and expires_at is not None:
                new_expires_at = self._calculate_expires_at(renew_for)
                if new_expires_at is not None:
                    await session.execute(
                        f"""
                        UPDATE {self._table_name}
                        SET expires_at = ?, updated_at = SYSUTCDATETIME()
                        WHERE session_id = ?
                        """,
                        (new_expires_at, key),
                    )
                    await session.commit()

            return _coerce_bytes(_row_value(row, "data", 0))

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Store a session value."""
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        async with self._config.provide_session() as session:
            await session.execute(
                f"""
                MERGE INTO {self._table_name} AS target
                USING (SELECT ? AS session_id, ? AS data, ? AS expires_at) AS src
                   ON target.session_id = src.session_id
                WHEN MATCHED THEN
                    UPDATE SET
                        data = src.data,
                        expires_at = src.expires_at,
                        updated_at = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN
                    INSERT (session_id, data, expires_at)
                    VALUES (src.session_id, src.data, src.expires_at);
                """,
                (key, data, expires_at),
            )
            await session.commit()

    async def delete(self, key: str) -> None:
        """Delete a session by key."""
        async with self._config.provide_session() as session:
            await session.execute(f"DELETE FROM {self._table_name} WHERE session_id = ?", (key,))
            await session.commit()

    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        async with self._config.provide_session() as session:
            await session.execute_script(f"TRUNCATE TABLE {self._table_name}")
            await session.commit()
        self._log_delete_all()

    async def exists(self, key: str) -> bool:
        """Check if a session key exists and is not expired."""
        async with self._config.provide_session() as session:
            row = await session.select_one_or_none(
                f"""
                SELECT 1 AS exists_flag
                FROM {self._table_name}
                WHERE session_id = ?
                  AND (expires_at IS NULL OR expires_at > SYSUTCDATETIME())
                """,
                (key,),
            )
            return row is not None

    async def expires_in(self, key: str) -> "int | None":
        """Get the time in seconds until the session expires."""
        async with self._config.provide_session() as session:
            row = await session.select_one_or_none(
                f"SELECT expires_at FROM {self._table_name} WHERE session_id = ?", (key,)
            )
            if row is None:
                return None
            expires_at = _normalize_utc(_row_value(row, "expires_at", 0))
            if expires_at is None:
                return None
            remaining = expires_at - datetime.now(timezone.utc)
            return max(0, int(remaining.total_seconds()))

    async def delete_expired(self) -> int:
        """Delete all expired sessions."""
        async with self._config.provide_session() as session:
            result = await session.execute(
                f"""
                DELETE FROM {self._table_name}
                WHERE expires_at IS NOT NULL
                  AND expires_at < SYSUTCDATETIME()
                """
            )
            await session.commit()

        count = int(getattr(result, "rows_affected", 0) or 0)
        if count > 0:
            self._log_delete_expired(count)
        return count

    def _get_create_table_sql(self) -> str:
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

    def _get_drop_table_sql(self) -> "list[str]":
        """Get SQL Server DROP TABLE statements."""
        return [f"IF OBJECT_ID(N'dbo.{self._table_name}', N'U') IS NOT NULL DROP TABLE dbo.{self._table_name};"]


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
