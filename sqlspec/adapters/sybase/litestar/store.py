"""Sybase ASE Litestar Store implementation."""

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlspec.adapters.sybase._typing import SybaseCursor
from sqlspec.adapters.sybase.core import (
    create_mapped_exception,
    escape_sql_literal,
    quote_bracket_identifier,
    resolve_rowcount,
    split_schema_and_table,
)
from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.adapters.sybase.config import SybaseConfig

__all__ = ("SybaseStore",)


class SybaseStore(BaseSQLSpecStore["SybaseConfig"]):
    """Sybase ASE-backed session store using pymssql sync sessions."""

    __slots__ = ()

    def __init__(self, config: "SybaseConfig") -> None:
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
        """Get Sybase ASE CREATE TABLE SQL with OBJECT_ID existence guards."""
        _, table_name = split_schema_and_table(self._table_name)
        table_ref = _table_ref(self._table_name)
        escaped_table_ref = escape_sql_literal(table_ref)
        index_bare = f"IX_{table_name}_expires_at"
        escaped_index_bare = escape_sql_literal(index_bare)
        index_name = quote_bracket_identifier(index_bare)
        create_table_sql = escape_sql_literal(
            f"CREATE TABLE {table_ref} ("
            "session_id VARCHAR(255) NOT NULL PRIMARY KEY, "
            "data IMAGE NOT NULL, "
            "expires_at BIGDATETIME NULL, "
            "created_at BIGDATETIME DEFAULT GETUTCDATE() NOT NULL, "
            "updated_at BIGDATETIME DEFAULT GETUTCDATE() NOT NULL"
            ")"
        )
        create_index_sql = escape_sql_literal(f"CREATE INDEX {index_name} ON {table_ref}(expires_at)")
        return (
            f"IF OBJECT_ID('{escaped_table_ref}', 'U') IS NULL EXECUTE('{create_table_sql}');\n"
            f"IF NOT EXISTS (SELECT 1 FROM dbo.sysindexes WHERE name = '{escaped_index_bare}' "
            f"AND id = OBJECT_ID('{escaped_table_ref}')) EXECUTE('{create_index_sql}');"
        )

    def _drop_table_sql(self) -> "list[str]":
        """Get Sybase ASE DROP TABLE statements."""
        table_ref = _table_ref(self._table_name)
        escaped = escape_sql_literal(table_ref)
        return [f"IF OBJECT_ID('{escaped}', 'U') IS NOT NULL EXECUTE('DROP TABLE {escaped}');"]

    def _create_table(self) -> None:
        with self._config.provide_session() as driver:
            driver.execute_script(self._table_ddl())
            driver.commit()
        self._log_table_created()

    def _get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        table_ref = _table_ref(self._table_name)
        sql = f"""
        SELECT data, expires_at FROM {table_ref}
        WHERE session_id = %s
          AND (expires_at IS NULL OR expires_at > GETUTCDATE())
        """
        try:
            with self._config.provide_connection() as conn:
                with SybaseCursor(conn) as cursor:
                    cursor.execute(sql, (key,))
                    row = cursor.fetchone()

                if row is None:
                    return None

                expires_at = _normalize_utc(_row_value(row, "expires_at", 1))
                if renew_for is not None and expires_at is not None:
                    new_expires_at = self._calculate_expires_at(renew_for)
                    if new_expires_at is not None:
                        with SybaseCursor(conn) as update_cursor:
                            update_cursor.execute(
                                f"""
                                UPDATE {table_ref}
                                SET expires_at = %s, updated_at = GETUTCDATE()
                                WHERE session_id = %s
                                """,
                                (new_expires_at, key),
                            )
                        conn.commit()

                return _coerce_bytes(_row_value(row, "data", 0))
        except Exception as exc:
            raise create_mapped_exception(exc) from exc

    def _set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        table_ref = _table_ref(self._table_name)
        sql = f"""
        IF EXISTS (SELECT 1 FROM {table_ref} HOLDLOCK WHERE session_id = %s)
            UPDATE {table_ref}
            SET data = %s, expires_at = %s, updated_at = GETUTCDATE()
            WHERE session_id = %s
        ELSE
            INSERT INTO {table_ref} (session_id, data, expires_at)
            VALUES (%s, %s, %s)
        """
        try:
            with self._config.provide_connection() as conn, SybaseCursor(conn) as cursor:
                cursor.execute(sql, (key, data, expires_at, key, key, data, expires_at))
                conn.commit()
        except Exception as exc:
            raise create_mapped_exception(exc) from exc

    def _delete(self, key: str) -> None:
        table_ref = _table_ref(self._table_name)
        try:
            with self._config.provide_connection() as conn, SybaseCursor(conn) as cursor:
                cursor.execute(f"DELETE FROM {table_ref} WHERE session_id = %s", (key,))
                conn.commit()
        except Exception as exc:
            raise create_mapped_exception(exc) from exc

    def _delete_all(self) -> None:
        table_ref = _table_ref(self._table_name)
        try:
            with self._config.provide_connection() as conn, SybaseCursor(conn) as cursor:
                cursor.execute(f"TRUNCATE TABLE {table_ref}")
                conn.commit()
            self._log_delete_all()
        except Exception as exc:
            raise create_mapped_exception(exc) from exc

    def _exists(self, key: str) -> bool:
        table_ref = _table_ref(self._table_name)
        sql = f"""
        SELECT 1
        FROM {table_ref}
        WHERE session_id = %s
          AND (expires_at IS NULL OR expires_at > GETUTCDATE())
        """
        try:
            with self._config.provide_connection() as conn, SybaseCursor(conn) as cursor:
                cursor.execute(sql, (key,))
                return cursor.fetchone() is not None
        except Exception as exc:
            raise create_mapped_exception(exc) from exc

    def _expires_in(self, key: str) -> "int | None":
        table_ref = _table_ref(self._table_name)
        try:
            with self._config.provide_connection() as conn, SybaseCursor(conn) as cursor:
                cursor.execute(f"SELECT expires_at FROM {table_ref} WHERE session_id = %s", (key,))
                row = cursor.fetchone()

            if row is None:
                return None
            expires_at = _normalize_utc(_row_value(row, "expires_at", 0))
            if expires_at is None:
                return None
            remaining = expires_at - datetime.now(UTC)
            return max(0, int(remaining.total_seconds()))
        except Exception as exc:
            raise create_mapped_exception(exc) from exc

    def _delete_expired(self) -> int:
        table_ref = _table_ref(self._table_name)
        sql = f"""
        DELETE FROM {table_ref}
        WHERE expires_at IS NOT NULL
          AND expires_at < GETUTCDATE()
        """
        try:
            with self._config.provide_connection() as conn, SybaseCursor(conn) as cursor:
                cursor.execute(sql)
                count = resolve_rowcount(cursor)
                conn.commit()
            if count > 0:
                self._log_delete_expired(count)
            return count
        except Exception as exc:
            raise create_mapped_exception(exc) from exc


def _table_ref(table_name: str) -> str:
    schema_name, bare_table_name = split_schema_and_table(table_name)
    return f"{quote_bracket_identifier(schema_name)}.{quote_bracket_identifier(bare_table_name)}"


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
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


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
