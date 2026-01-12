"""CockroachDB session store for Litestar integration using asyncpg."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sqlspec.extensions.litestar.store import BaseSQLSpecStore

if TYPE_CHECKING:
    from sqlspec.adapters.cockroach_asyncpg.config import CockroachAsyncpgConfig


__all__ = ("CockroachAsyncpgStore",)


class CockroachAsyncpgStore(BaseSQLSpecStore["CockroachAsyncpgConfig"]):
    """CockroachDB session store using asyncpg driver."""

    __slots__ = ()

    def __init__(self, config: "CockroachAsyncpgConfig") -> None:
        super().__init__(config)

    def _get_create_table_sql(self) -> str:
        """Get CockroachDB CREATE TABLE SQL with optimized schema."""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            session_id TEXT PRIMARY KEY,
            data BYTEA NOT NULL,
            expires_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_{self._table_name}_expires_at
        ON {self._table_name}(expires_at) WHERE expires_at IS NOT NULL;
        """

    def _get_drop_table_sql(self) -> "list[str]":
        return [f"DROP INDEX IF EXISTS idx_{self._table_name}_expires_at", f"DROP TABLE IF EXISTS {self._table_name}"]

    async def create_table(self) -> None:
        sql = self._get_create_table_sql()
        async with self._config.provide_session() as driver:
            await driver.execute_script(sql)
        self._log_table_created()

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = $1
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        async with self._config.provide_connection() as conn:
            row = await conn.fetchrow(sql, key)

            if row is None:
                return None

            if renew_for is not None and row["expires_at"] is not None:
                new_expires_at = self._calculate_expires_at(renew_for)
                if new_expires_at is not None:
                    update_sql = f"""
                    UPDATE {self._table_name}
                    SET expires_at = $1, updated_at = CURRENT_TIMESTAMP
                    WHERE session_id = $2
                    """
                    await conn.execute(update_sql, new_expires_at, key)

            return bytes(row["data"])

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)

        sql = f"""
        INSERT INTO {self._table_name} (session_id, data, expires_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (session_id)
        DO UPDATE SET
            data = EXCLUDED.data,
            expires_at = EXCLUDED.expires_at,
            updated_at = CURRENT_TIMESTAMP
        """

        async with self._config.provide_connection() as conn:
            await conn.execute(sql, key, data, expires_at)

    async def delete(self, key: str) -> None:
        sql = f"DELETE FROM {self._table_name} WHERE session_id = $1"

        async with self._config.provide_connection() as conn:
            await conn.execute(sql, key)

    async def delete_all(self) -> None:
        sql = f"DELETE FROM {self._table_name}"

        async with self._config.provide_connection() as conn:
            await conn.execute(sql)
        self._log_delete_all()

    async def exists(self, key: str) -> bool:
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = $1
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        async with self._config.provide_connection() as conn:
            row = await conn.fetchrow(sql, key)
            return row is not None

    async def expires_in(self, key: str) -> "int | None":
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = $1
        """

        async with self._config.provide_connection() as conn:
            row = await conn.fetchrow(sql, key)

            if row is None or row["expires_at"] is None:
                return None

            expires_at = row["expires_at"]
            now = datetime.now(timezone.utc)

            if expires_at <= now:
                return 0

            delta = expires_at - now
            return int(delta.total_seconds())

    async def delete_expired(self) -> int:
        sql = f"DELETE FROM {self._table_name} WHERE expires_at <= CURRENT_TIMESTAMP"

        async with self._config.provide_connection() as conn:
            result = await conn.execute(sql)
            count = int(result.split()[-1]) if result else 0
            if count > 0:
                self._log_delete_expired(count)
            return count
