"""Teradata Vantage Litestar Store implementation."""

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlspec.adapters.teradata._typing import TeradataCursor
from sqlspec.adapters.teradata.core import format_identifier as quote_double_identifier
from sqlspec.adapters.teradata.core import resolve_rowcount
from sqlspec.adapters.teradata.core import split_teradata_table_name as split_schema_and_table
from sqlspec.adapters.teradata.events.store import _TABLE_EXISTS_SQL
from sqlspec.config import LitestarConfig
from sqlspec.extensions.litestar.store import BaseSQLSpecStore, CompanionIndex
from sqlspec.migrations.schema import SchemaTarget
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.adapters.teradata.config import TeradataConfig

__all__ = ("TeradataLitestarConfig", "TeradataStore")


class TeradataLitestarConfig(LitestarConfig):
    """Configuration for Teradata Vantage Litestar session store."""


class TeradataStore(BaseSQLSpecStore["TeradataConfig"]):
    """Teradata Vantage-backed session store using teradatasql sync sessions."""

    __slots__ = ()

    def __init__(self, config: "TeradataConfig") -> None:
        super().__init__(config)

    @staticmethod
    def _ensure_table_name(table_name: str) -> None:
        schema, table = split_schema_and_table(table_name)
        if schema is not None:
            BaseSQLSpecStore._ensure_table_name(schema)
        BaseSQLSpecStore._ensure_table_name(table)

    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        await async_(self._create_table)()
        await self.reconcile_schema(assume_existing=True)

    async def drop_table(self) -> None:
        """Drop the session table when it exists."""
        await async_(self._drop_table)()

    def _drop_table(self) -> None:
        schema, table = split_schema_and_table(self._table_name)
        with self._config.provide_session() as driver:
            if driver.select_one_or_none(_TABLE_EXISTS_SQL, (schema, table)) is not None:
                driver.execute(self._drop_table_sql()[0])
                driver.commit()

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
        return (
            f"CREATE MULTISET TABLE {_table_ref(self._table_name)} ("
            "session_id VARCHAR(255) NOT NULL PRIMARY KEY, data BLOB NOT NULL, "
            "expires_at TIMESTAMP(6) WITH TIME ZONE)"
        )

    def _schema_target(self) -> SchemaTarget:
        schema, table = split_schema_and_table(self._table_name)
        return SchemaTarget.from_ddl(table, self._table_ddl(), schema=schema, dialect="teradata")

    def get_companion_indexes(self) -> list[CompanionIndex]:
        schema, table = split_schema_and_table(self._table_name)
        name = f"idx_{table}_expires_at"
        return [
            CompanionIndex(
                name=name,
                table=table,
                schema=schema,
                ddl=f"CREATE INDEX {name} (expires_at) ON {_table_ref(self._table_name)}",
            )
        ]

    def _drop_table_sql(self) -> list[str]:
        return [f"DROP TABLE {_table_ref(self._table_name)}"]

    def _create_table(self) -> None:
        schema, table = split_schema_and_table(self._table_name)
        with self._config.provide_session() as driver:
            if driver.select_one_or_none(_TABLE_EXISTS_SQL, (schema, table)) is None:
                driver.execute(self._table_ddl())
                driver.commit()
            self.reconcile_companion_objects_sync(driver)
            driver.commit()
        self._log_table_created()

    def _get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        table_ref = _table_ref(self._table_name)
        sql = f"""
        SELECT data, expires_at FROM {table_ref}
        WHERE session_id = ?
          AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP(6))
        """
        with self._config.provide_connection() as conn:
            with TeradataCursor(conn) as cursor:
                cursor.execute(sql, (key,))
                row = cursor.fetchone()

            if row is None:
                return None

            expires_at = _normalize_utc(_row_value(row, "expires_at", 1))
            if renew_for is not None and expires_at is not None:
                new_expires_at = self._calculate_expires_at(renew_for)
                if new_expires_at is not None:
                    with TeradataCursor(conn) as update_cursor:
                        update_cursor.execute(
                            f"""
                            UPDATE {table_ref}
                            SET expires_at = ?
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
        with self._config.provide_connection() as conn, TeradataCursor(conn) as cursor:
            cursor.execute(
                f"UPDATE {table_ref} SET data = ?, expires_at = ? WHERE session_id = ?", (data, expires_at, key)
            )
            if resolve_rowcount(cursor) == 0:
                cursor.execute(
                    f"INSERT INTO {table_ref} (session_id, data, expires_at) VALUES (?, ?, ?)", (key, data, expires_at)
                )
            conn.commit()

    def _delete(self, key: str) -> None:
        table_ref = _table_ref(self._table_name)
        with self._config.provide_connection() as conn, TeradataCursor(conn) as cursor:
            cursor.execute(f"DELETE FROM {table_ref} WHERE session_id = ?", (key,))
            conn.commit()

    def _delete_all(self) -> None:
        table_ref = _table_ref(self._table_name)
        with self._config.provide_connection() as conn, TeradataCursor(conn) as cursor:
            cursor.execute(f"DELETE FROM {table_ref} ALL")
            conn.commit()
        self._log_delete_all()

    def _exists(self, key: str) -> bool:
        table_ref = _table_ref(self._table_name)
        sql = f"""
        SELECT 1 FROM {table_ref}
        WHERE session_id = ?
          AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP(6))
        """
        with self._config.provide_connection() as conn, TeradataCursor(conn) as cursor:
            cursor.execute(sql, (key,))
            return cursor.fetchone() is not None

    def _expires_in(self, key: str) -> "int | None":
        table_ref = _table_ref(self._table_name)
        sql = f"""
        SELECT expires_at FROM {table_ref}
        WHERE session_id = ?
        """
        with self._config.provide_connection() as conn, TeradataCursor(conn) as cursor:
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
        sql = f"DELETE FROM {table_ref} WHERE expires_at <= CURRENT_TIMESTAMP(6)"
        with self._config.provide_connection() as conn, TeradataCursor(conn) as cursor:
            cursor.execute(sql)
            conn.commit()
            count = resolve_rowcount(cursor)
        if count > 0:
            self._log_delete_expired(count)
        return count


def _table_ref(table_name: str) -> str:
    schema_name, bare_table_name = split_schema_and_table(table_name)
    return (
        f"{quote_double_identifier(schema_name)}.{quote_double_identifier(bare_table_name)}"
        if schema_name
        else quote_double_identifier(bare_table_name)
    )


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
