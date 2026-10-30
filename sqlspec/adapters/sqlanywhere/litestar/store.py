"""SAP SQL Anywhere Litestar Store implementation."""

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlspec.adapters.sqlanywhere._typing import SqlanywhereCursor
from sqlspec.adapters.sqlanywhere.core import quote_double_identifier, split_schema_and_table
from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.adapters.sqlanywhere.config import SqlanywhereConfig

__all__ = ("SqlanywhereStore",)


class SqlanywhereStore(BaseSQLSpecStore["SqlanywhereConfig"]):
    """SAP SQL Anywhere-backed session store using sqlanydb sync sessions."""

    __slots__ = ()

    def __init__(self, config: "SqlanywhereConfig") -> None:
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
        """Get SQL Anywhere CREATE TABLE IF NOT EXISTS DDL."""
        _, table_name = split_schema_and_table(self._table_name)
        table_ref = _table_ref(self._table_name)
        index_name = quote_double_identifier(f"IX_{table_name}_expires_at")
        return (
            f"CREATE TABLE IF NOT EXISTS {table_ref} ("
            "session_id VARCHAR(255) NOT NULL PRIMARY KEY, "
            "data LONG BINARY NOT NULL, "
            "expires_at TIMESTAMP NULL, "
            "created_at TIMESTAMP DEFAULT CURRENT UTC TIMESTAMP NOT NULL, "
            "updated_at TIMESTAMP DEFAULT CURRENT UTC TIMESTAMP NOT NULL"
            ");\n"
            f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_ref}(expires_at);"
        )

    def _drop_table_sql(self) -> "list[str]":
        """Get SQL Anywhere DROP TABLE IF EXISTS statements."""
        table_ref = _table_ref(self._table_name)
        return [f"DROP TABLE IF EXISTS {table_ref};"]

    def _create_table(self) -> None:
        with self._config.provide_session() as driver:
            driver.execute_script(self._table_ddl())
            driver.commit()
        self._log_table_created()

    def _get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        table_ref = _table_ref(self._table_name)
        sql = f"""
        SELECT data, expires_at FROM {table_ref}
        WHERE session_id = ?
          AND (expires_at IS NULL OR expires_at > CURRENT UTC TIMESTAMP)
        """
        with self._config.provide_connection() as conn:
            with SqlanywhereCursor(conn) as cursor:
                cursor.execute(sql, (key,))
                row = cursor.fetchone()

            if row is None:
                return None

            expires_at = _normalize_utc(_row_value(row, "expires_at", 1))
            if renew_for is not None and expires_at is not None:
                new_expires_at = self._calculate_expires_at(renew_for)
                if new_expires_at is not None:
                    with SqlanywhereCursor(conn) as update_cursor:
                        update_cursor.execute(
                            f"""
                            UPDATE {table_ref}
                            SET expires_at = ?, updated_at = CURRENT UTC TIMESTAMP
                            WHERE session_id = ?
                            """,
                            (new_expires_at, key),
                        )
                    conn.commit()

            return _coerce_bytes(_row_value(row, "data", 0))

    def _set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        table_ref = _table_ref(self._table_name)
        sql = f"""
        INSERT INTO {table_ref} (session_id, data, expires_at, updated_at)
        ON EXISTING UPDATE
        VALUES (?, ?, ?, CURRENT UTC TIMESTAMP)
        """
        with self._config.provide_connection() as conn, SqlanywhereCursor(conn) as cursor:
            cursor.execute(sql, (key, data, expires_at))
            conn.commit()

    def _delete(self, key: str) -> None:
        table_ref = _table_ref(self._table_name)
        with self._config.provide_connection() as conn, SqlanywhereCursor(conn) as cursor:
            cursor.execute(f"DELETE FROM {table_ref} WHERE session_id = ?", (key,))
            conn.commit()

    def _delete_all(self) -> None:
        table_ref = _table_ref(self._table_name)
        with self._config.provide_connection() as conn, SqlanywhereCursor(conn) as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_ref}")
            conn.commit()
        self._log_delete_all()

    def _exists(self, key: str) -> bool:
        table_ref = _table_ref(self._table_name)
        sql = f"""
        SELECT 1 FROM {table_ref}
        WHERE session_id = ?
          AND (expires_at IS NULL OR expires_at > CURRENT UTC TIMESTAMP)
        """
        with self._config.provide_connection() as conn, SqlanywhereCursor(conn) as cursor:
            cursor.execute(sql, (key,))
            return cursor.fetchone() is not None

    def _expires_in(self, key: str) -> "int | None":
        table_ref = _table_ref(self._table_name)
        sql = f"""
        SELECT expires_at FROM {table_ref}
        WHERE session_id = ?
        """
        with self._config.provide_connection() as conn, SqlanywhereCursor(conn) as cursor:
            cursor.execute(sql, (key,))
            row = cursor.fetchone()

        if row is None:
            return None

        expires_at = _normalize_utc(_row_value(row, "expires_at", 0))
        if expires_at is None:
            return None

        now = datetime.now(UTC)
        if expires_at <= now:
            return 0
        return int((expires_at - now).total_seconds())

    def _delete_expired(self) -> int:
        table_ref = _table_ref(self._table_name)
        sql = f"DELETE FROM {table_ref} WHERE expires_at <= CURRENT UTC TIMESTAMP"
        with self._config.provide_connection() as conn, SqlanywhereCursor(conn) as cursor:
            cursor.execute(sql)
            conn.commit()
            count = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
        if count > 0:
            self._log_delete_expired(count)
        return count


def _table_ref(table_name: str) -> str:
    schema_name, bare_table_name = split_schema_and_table(table_name)
    return f"{quote_double_identifier(schema_name)}.{quote_double_identifier(bare_table_name)}"


def _row_value(row: Any, key: str, index: int) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    return row[index]


def _coerce_bytes(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    return str(value).encode("utf-8")


def _normalize_utc(value: "datetime | None") -> "datetime | None":
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
