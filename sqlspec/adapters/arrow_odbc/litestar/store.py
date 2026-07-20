"""arrow-odbc Litestar Store implementation."""

import base64
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.adapters.arrow_odbc.config import ArrowOdbcConfig

__all__ = ("ArrowOdbcStore",)

_MAX_INLINE_DATA_LENGTH = 3500


class ArrowOdbcStore(BaseSQLSpecStore["ArrowOdbcConfig"]):
    """SQL Server-backed session store using arrow-odbc sessions."""

    __slots__ = ()

    def __init__(self, config: "ArrowOdbcConfig") -> None:
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

    def _create_table(self) -> None:
        with self._config.provide_session() as driver:
            driver.execute_script(self._table_ddl())
            driver.commit()
        self._log_table_created()

    def _get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        with self._config.provide_session() as driver:
            row = driver.select_one_or_none(
                f"SELECT data, expires_at FROM {self._table_name} WHERE session_id = ?", (key,)
            )
            if row is None:
                return None

            expires_at = _normalize_utc(_row_value(row, "expires_at", 1))
            if expires_at is not None and expires_at < datetime.now(timezone.utc):
                self._delete(key)
                return None

            if renew_for is not None and expires_at is not None:
                new_expires_at = self._calculate_expires_at(renew_for)
                if new_expires_at is not None:
                    driver.execute(
                        f"""
                        UPDATE {self._table_name}
                        SET expires_at = ?, updated_at = SYSUTCDATETIME()
                        WHERE session_id = ?
                        """,
                        (_format_datetime(new_expires_at), key),
                    )
                    driver.commit()

            data = _row_value(row, "data", 0)
            if data is None:
                data = self._get_chunked_data(driver, key)
            return _decode_bytes(data)

    def _set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        data = _encode_bytes(self._value_to_bytes(value))
        expires_at = self._calculate_expires_at(expires_in)
        with self._config.provide_session() as driver:
            inline_data = data if len(data) <= _MAX_INLINE_DATA_LENGTH else None
            existing = driver.select_one_or_none(
                f"SELECT session_id FROM {self._table_name} WHERE session_id = ?", (key,)
            )
            if existing is None:
                driver.execute(
                    f"""
                    INSERT INTO {self._table_name} (session_id, data, expires_at)
                    VALUES (?, ?, ?)
                    """,
                    (key, inline_data, _format_datetime(expires_at)),
                )
            else:
                driver.execute(
                    f"""
                    UPDATE {self._table_name}
                    SET data = ?, expires_at = ?, updated_at = SYSUTCDATETIME()
                    WHERE session_id = ?
                    """,
                    (inline_data, _format_datetime(expires_at), key),
                )
            driver.execute(f"DELETE FROM {self._chunk_table_name} WHERE session_id = ?", (key,))
            if inline_data is None:
                for index, chunk in enumerate(_chunk_text(data, _MAX_INLINE_DATA_LENGTH)):
                    driver.execute(
                        f"""
                        INSERT INTO {self._chunk_table_name} (session_id, chunk_index, data)
                        VALUES (?, ?, ?)
                        """,
                        (key, index, chunk),
                    )
            driver.commit()

    def _delete(self, key: str) -> None:
        with self._config.provide_session() as driver:
            driver.execute(f"DELETE FROM {self._table_name} WHERE session_id = ?", (key,))
            driver.commit()

    def _delete_all(self) -> None:
        with self._config.provide_session() as driver:
            driver.execute(f"DELETE FROM {self._table_name}")
            driver.commit()
        self._log_delete_all()

    def _exists(self, key: str) -> bool:
        with self._config.provide_session() as driver:
            row = driver.select_one_or_none(
                f"""
                SELECT 1 AS exists_flag
                FROM {self._table_name}
                WHERE session_id = ?
                  AND (expires_at IS NULL OR expires_at > SYSUTCDATETIME())
                """,
                (key,),
            )
            return row is not None

    def _expires_in(self, key: str) -> "int | None":
        with self._config.provide_session() as driver:
            row = driver.select_one_or_none(f"SELECT expires_at FROM {self._table_name} WHERE session_id = ?", (key,))
            if row is None:
                return None
            expires_at = _normalize_utc(_row_value(row, "expires_at", 0))
            if expires_at is None:
                return None
            remaining = expires_at - datetime.now(timezone.utc)
            return max(0, int(remaining.total_seconds()))

    def _delete_expired(self) -> int:
        with self._config.provide_session() as driver:
            count = int(
                driver.select_value(
                    f"""
                    SELECT COUNT(*) AS expired_count
                    FROM {self._table_name}
                    WHERE expires_at IS NOT NULL
                      AND expires_at < SYSUTCDATETIME()
                    """
                )
                or 0
            )
            driver.execute(
                f"""
                DELETE FROM {self._table_name}
                WHERE expires_at IS NOT NULL
                  AND expires_at < SYSUTCDATETIME()
                """
            )
            driver.commit()
        if count > 0:
            self._log_delete_expired(count)
        return count

    @property
    def _chunk_table_name(self) -> str:
        return f"{self._table_name}_chunks"

    def _get_chunked_data(self, driver: Any, key: str) -> str:
        rows = driver.select(
            f"""
            SELECT data
            FROM {self._chunk_table_name}
            WHERE session_id = ?
            ORDER BY chunk_index
            """,
            (key,),
        )
        return "".join(str(_row_value(row, "data", 0) or "") for row in rows)

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
                data NVARCHAR(MAX) NULL,
                expires_at DATETIME2(6) NULL,
                created_at DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
                updated_at DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME()
            );

            CREATE INDEX IX_{self._table_name}_expires_at
                ON {self._table_name}(expires_at)
                WHERE expires_at IS NOT NULL;
        END;

        IF NOT EXISTS (
            SELECT 1
            FROM sys.tables
            WHERE name = N'{self._chunk_table_name}'
              AND schema_id = SCHEMA_ID(N'dbo')
        )
        BEGIN
            CREATE TABLE {self._chunk_table_name} (
                session_id NVARCHAR(255) NOT NULL,
                chunk_index INT NOT NULL,
                data NVARCHAR(3500) NOT NULL,
                CONSTRAINT PK_{self._chunk_table_name} PRIMARY KEY (session_id, chunk_index),
                CONSTRAINT FK_{self._chunk_table_name}_session FOREIGN KEY (session_id)
                    REFERENCES {self._table_name}(session_id) ON DELETE CASCADE
            );
        END;
        """

    def _drop_table_sql(self) -> "list[str]":
        """Get SQL Server DROP TABLE statements."""
        return [
            f"IF OBJECT_ID(N'dbo.{self._chunk_table_name}', N'U') IS NOT NULL DROP TABLE dbo.{self._chunk_table_name};",
            f"IF OBJECT_ID(N'dbo.{self._table_name}', N'U') IS NOT NULL DROP TABLE dbo.{self._table_name};",
        ]


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


def _format_datetime(value: "datetime | None") -> "str | None":
    if value is None:
        return None
    normalized = _normalize_utc(value)
    if normalized is None:
        return None
    return normalized.replace(tzinfo=None).isoformat(timespec="microseconds")


def _encode_bytes(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _decode_bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        value = value.decode("ascii")
    if isinstance(value, bytearray):
        value = bytes(value).decode("ascii")
    if not isinstance(value, str):
        value = str(value)
    return base64.b64decode(value.encode("ascii"))


def _chunk_text(value: str, size: int) -> "list[str]":
    return [value[index : index + size] for index in range(0, len(value), size)]
