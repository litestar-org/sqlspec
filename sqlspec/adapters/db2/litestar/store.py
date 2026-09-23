"""IBM Db2 Litestar Store implementation."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.adapters.db2.config import Db2Config

__all__ = ("Db2Store",)


def _row_value(row: Any, key: str, index: int) -> Any:
    """Extract a column value from a dictionary or tuple row."""
    if isinstance(row, dict):
        return row.get(key)
    return row[index]


def _coerce_bytes(value: Any) -> bytes | None:
    """Coerce row data into bytes if not None."""
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, memoryview):
        return value.tobytes()
    return bytes(value)


def _normalize_utc(dt: Any) -> datetime | None:
    """Normalize datetime to UTC timezone."""
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return None


class Db2Store(BaseSQLSpecStore["Db2Config"]):
    """IBM Db2-backed session store using synchronous Db2 sessions."""

    __slots__ = ()

    def __init__(self, config: "Db2Config") -> None:
        """Initialize Db2 session store."""
        super().__init__(config)

    async def create_table(self) -> None:
        """Create the session table if it does not exist."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        await async_(self._create_table)()
        await self.reconcile_schema(assume_existing=True)

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Get a session value by key."""
        return await async_(self._get)(key, renew_for)

    async def set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
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

    async def expires_in(self, key: str) -> int | None:
        """Get the time in seconds until the session expires."""
        return await async_(self._expires_in)(key)

    async def delete_expired(self) -> int:
        """Delete all expired sessions."""
        return await async_(self._delete_expired)()

    def _table_ddl(self) -> str:
        """Get Db2 CREATE TABLE SQL."""
        return f"""
        CREATE TABLE {self._table_name} (
            session_id VARCHAR(255) NOT NULL PRIMARY KEY,
            data BLOB(10M) NOT NULL,
            expires_at TIMESTAMP,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP
        )
        """

    def _drop_table_sql(self) -> list[str]:
        """Get Db2 DROP TABLE statements."""
        return [f"DROP TABLE {self._table_name}"]

    def _create_table(self) -> None:
        """Execute table and index creation in Db2."""
        with self._config.provide_session() as driver:
            tables = driver.data_dictionary.get_tables(driver)
            table_names = {t["table_name"].upper() for t in tables if t.get("table_name")}
            bare_table_name = self._table_name.split(".")[-1].upper()
            if bare_table_name not in table_names:
                driver.execute_script(self._table_ddl())
                driver.execute_script(f"CREATE INDEX IX_{bare_table_name}_exp ON {self._table_name}(expires_at)")
                driver.commit()
        self._log_table_created()

    def _get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Retrieve session data by key synchronously."""
        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = ?
          AND (expires_at IS NULL OR expires_at > CURRENT TIMESTAMP)
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
                            SET expires_at = ?, updated_at = CURRENT TIMESTAMP
                            WHERE session_id = ?
                            """,
                            (new_expires_at, key),
                        )
                    finally:
                        update_cursor.close()
                    conn.commit()

            return _coerce_bytes(_row_value(row, "data", 0))

    def _set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        """Upsert session data using atomic MERGE INTO."""
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        sql = f"""
        MERGE INTO {self._table_name} AS target
        USING (
            SELECT
                CAST(? AS VARCHAR(255)) AS session_id,
                CAST(? AS BLOB(10M)) AS data,
                CAST(? AS TIMESTAMP) AS expires_at
            FROM SYSIBM.SYSDUMMY1
        ) AS src
           ON target.session_id = src.session_id
        WHEN MATCHED THEN
            UPDATE SET
                data = src.data,
                expires_at = src.expires_at,
                updated_at = CURRENT TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (session_id, data, expires_at, created_at, updated_at)
            VALUES (src.session_id, src.data, src.expires_at, CURRENT TIMESTAMP, CURRENT TIMESTAMP)
        """
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (key, data, expires_at))
            finally:
                cursor.close()
            conn.commit()

    def _delete(self, key: str) -> None:
        """Delete session by key."""
        sql = f"DELETE FROM {self._table_name} WHERE session_id = ?"
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (key,))
            finally:
                cursor.close()
            conn.commit()

    def _delete_all(self) -> None:
        """Truncate all rows in session table."""
        sql = f"DELETE FROM {self._table_name}"
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
            finally:
                cursor.close()
            conn.commit()

    def _exists(self, key: str) -> bool:
        """Check key presence."""
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = ?
          AND (expires_at IS NULL OR expires_at > CURRENT TIMESTAMP)
        """
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (key,))
                row = cursor.fetchone()
                return row is not None
            finally:
                cursor.close()

    def _expires_in(self, key: str) -> int | None:
        """Calculate seconds until session expiration."""
        sql = f"SELECT expires_at FROM {self._table_name} WHERE session_id = ?"
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (key,))
                row = cursor.fetchone()
            finally:
                cursor.close()

            if row is None:
                return None
            expires_at = _normalize_utc(_row_value(row, "expires_at", 0))
            if expires_at is None:
                return None
            now = datetime.now(timezone.utc)
            remaining = int((expires_at - now).total_seconds())
            return max(remaining, 0)

    def _delete_expired(self) -> int:
        """Remove expired sessions and return deleted row count."""
        sql = f"DELETE FROM {self._table_name} WHERE expires_at IS NOT NULL AND expires_at <= CURRENT TIMESTAMP"
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                affected = getattr(cursor, "rowcount", 0)
            finally:
                cursor.close()
            conn.commit()
            return int(affected) if affected and affected > 0 else 0
