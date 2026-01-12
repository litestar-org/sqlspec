"""CockroachDB session stores for Litestar integration using psycopg."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.adapters.cockroach_psycopg.config import CockroachPsycopgAsyncConfig, CockroachPsycopgSyncConfig


__all__ = ("CockroachPsycopgAsyncStore", "CockroachPsycopgSyncStore")


class CockroachPsycopgAsyncStore(BaseSQLSpecStore["CockroachPsycopgAsyncConfig"]):
    """CockroachDB session store using psycopg async driver."""

    __slots__ = ()

    def __init__(self, config: "CockroachPsycopgAsyncConfig") -> None:
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
            await driver.commit()
        self._log_table_created()

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        conn_context = self._config.provide_connection()
        async with conn_context as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql.encode(), (key,))
                row = await cur.fetchone()

            if row is None:
                return None

            if renew_for is not None and row["expires_at"] is not None:
                new_expires_at = self._calculate_expires_at(renew_for)
                if new_expires_at is not None:
                    update_sql = f"""
                    UPDATE {self._table_name}
                    SET expires_at = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE session_id = %s
                    """
                    await conn.execute(update_sql.encode(), (new_expires_at, key))
                    await conn.commit()

            return bytes(row["data"])

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)

        sql = f"""
        INSERT INTO {self._table_name} (session_id, data, expires_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (session_id)
        DO UPDATE SET
            data = EXCLUDED.data,
            expires_at = EXCLUDED.expires_at,
            updated_at = CURRENT_TIMESTAMP
        """

        conn_context = self._config.provide_connection()
        async with conn_context as conn:
            await conn.execute(sql.encode(), (key, data, expires_at))
            await conn.commit()

    async def delete(self, key: str) -> None:
        sql = f"DELETE FROM {self._table_name} WHERE session_id = %s"

        conn_context = self._config.provide_connection()
        async with conn_context as conn:
            await conn.execute(sql.encode(), (key,))
            await conn.commit()

    async def delete_all(self) -> None:
        sql = f"DELETE FROM {self._table_name}"

        conn_context = self._config.provide_connection()
        async with conn_context as conn:
            await conn.execute(sql.encode())
            await conn.commit()
        self._log_delete_all()

    async def exists(self, key: str) -> bool:
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        conn_context = self._config.provide_connection()
        async with conn_context as conn, conn.cursor() as cur:
            await cur.execute(sql.encode(), (key,))
            row = await cur.fetchone()
            return row is not None

    async def expires_in(self, key: str) -> "int | None":
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = %s
        """

        conn_context = self._config.provide_connection()
        async with conn_context as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql.encode(), (key,))
                row = await cur.fetchone()

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

        conn_context = self._config.provide_connection()
        async with conn_context as conn, conn.cursor() as cur:
            await cur.execute(sql.encode())
            await conn.commit()
            count = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            if count > 0:
                self._log_delete_expired(count)
            return count


class CockroachPsycopgSyncStore(BaseSQLSpecStore["CockroachPsycopgSyncConfig"]):
    """CockroachDB session store using psycopg sync driver."""

    __slots__ = ()

    def __init__(self, config: "CockroachPsycopgSyncConfig") -> None:
        super().__init__(config)

    def _get_create_table_sql(self) -> str:
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
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        with self._config.provide_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql.encode(), (key,))
                row = cur.fetchone()

            if row is None:
                return None

            if renew_for is not None and row["expires_at"] is not None:
                new_expires_at = self._calculate_expires_at(renew_for)
                if new_expires_at is not None:
                    update_sql = f"""
                    UPDATE {self._table_name}
                    SET expires_at = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE session_id = %s
                    """
                    conn.execute(update_sql.encode(), (new_expires_at, key))
                    conn.commit()

            return bytes(row["data"])

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        return await async_(self._get)(key, renew_for)

    def _set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)

        sql = f"""
        INSERT INTO {self._table_name} (session_id, data, expires_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (session_id)
        DO UPDATE SET
            data = EXCLUDED.data,
            expires_at = EXCLUDED.expires_at,
            updated_at = CURRENT_TIMESTAMP
        """

        with self._config.provide_connection() as conn:
            conn.execute(sql.encode(), (key, data, expires_at))
            conn.commit()

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        await async_(self._set)(key, value, expires_in=expires_in)

    def _delete(self, key: str) -> None:
        sql = f"DELETE FROM {self._table_name} WHERE session_id = %s"

        with self._config.provide_connection() as conn:
            conn.execute(sql.encode(), (key,))
            conn.commit()

    async def delete(self, key: str) -> None:
        await async_(self._delete)(key)

    def _delete_all(self) -> None:
        sql = f"DELETE FROM {self._table_name}"

        with self._config.provide_connection() as conn:
            conn.execute(sql.encode())
            conn.commit()
        self._log_delete_all()

    async def delete_all(self) -> None:
        await async_(self._delete_all)()

    def _exists(self, key: str) -> bool:
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql.encode(), (key,))
            row = cur.fetchone()
            return row is not None

    async def exists(self, key: str) -> bool:
        return await async_(self._exists)(key)

    def _expires_in(self, key: str) -> "int | None":
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = %s
        """

        with self._config.provide_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql.encode(), (key,))
                row = cur.fetchone()

            if row is None or row["expires_at"] is None:
                return None

            expires_at = row["expires_at"]
            now = datetime.now(timezone.utc)

            if expires_at <= now:
                return 0

            delta = expires_at - now
            return int(delta.total_seconds())

    async def expires_in(self, key: str) -> "int | None":
        return await async_(self._expires_in)(key)

    def _delete_expired(self) -> int:
        sql = f"DELETE FROM {self._table_name} WHERE expires_at <= CURRENT_TIMESTAMP"

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql.encode())
            conn.commit()
            count = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            if count > 0:
                self._log_delete_expired(count)
            return count

    async def delete_expired(self) -> int:
        return await async_(self._delete_expired)()
