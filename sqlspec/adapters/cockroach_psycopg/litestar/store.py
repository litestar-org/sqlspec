"""CockroachDB session stores for Litestar integration using psycopg."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

from psycopg.rows import dict_row

from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.adapters.cockroach_psycopg.config import CockroachPsycopgAsyncConfig, CockroachPsycopgSyncConfig


__all__ = ("CockroachPsycopgAsyncStore", "CockroachPsycopgSyncStore")


class CockroachPsycopgAsyncStore(BaseSQLSpecStore["CockroachPsycopgAsyncConfig"]):
    """CockroachDB session store using psycopg async driver."""

    __slots__ = ()
    extension_config_options = BaseSQLSpecStore.extension_config_options | frozenset({
        "enable_hash_sharded_indexes",
        "hash_shard_bucket_count",
        "ttl_expiration_expression",
    })

    def __init__(self, config: "CockroachPsycopgAsyncConfig") -> None:
        super().__init__(config)

    async def create_table(self) -> None:
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        sql = self._table_ddl()
        async with self._config.provide_session() as driver:
            await driver.execute_script(sql)
            await driver.commit()
        self._log_table_created()
        await self.reconcile_schema(assume_existing=True)

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        conn_context = self._config.provide_connection()
        async with conn_context as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
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
            async with conn.cursor(row_factory=dict_row) as cur:
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

    def _table_ddl(self) -> str:
        """Get CockroachDB CREATE TABLE SQL with optimized schema."""
        settings = cast("dict[str, Any]", self._config.extension_config.get("litestar", {}))
        table_options, hash_clause = _cockroach_storage_clauses(settings)
        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            session_id TEXT PRIMARY KEY,
            data BYTEA NOT NULL,
            expires_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        ){table_options};

        CREATE INDEX IF NOT EXISTS idx_{self._table_name}_expires_at
        ON {self._table_name}(expires_at){hash_clause} WHERE expires_at IS NOT NULL;
        """

    def _drop_table_sql(self) -> "list[str]":
        return [f"DROP INDEX IF EXISTS idx_{self._table_name}_expires_at", f"DROP TABLE IF EXISTS {self._table_name}"]


class CockroachPsycopgSyncStore(BaseSQLSpecStore["CockroachPsycopgSyncConfig"]):
    """CockroachDB session store using psycopg sync driver."""

    __slots__ = ()
    extension_config_options = BaseSQLSpecStore.extension_config_options | frozenset({
        "enable_hash_sharded_indexes",
        "hash_shard_bucket_count",
        "ttl_expiration_expression",
    })

    def __init__(self, config: "CockroachPsycopgSyncConfig") -> None:
        super().__init__(config)

    async def create_table(self) -> None:
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        await async_(self._create_table)()
        await self.reconcile_schema(assume_existing=True)

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        return await async_(self._get)(key, renew_for)

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        await async_(self._set)(key, value, expires_in=expires_in)

    async def delete(self, key: str) -> None:
        await async_(self._delete)(key)

    async def delete_all(self) -> None:
        await async_(self._delete_all)()

    async def exists(self, key: str) -> bool:
        return await async_(self._exists)(key)

    async def expires_in(self, key: str) -> "int | None":
        return await async_(self._expires_in)(key)

    async def delete_expired(self) -> int:
        return await async_(self._delete_expired)()

    def _table_ddl(self) -> str:
        settings = cast("dict[str, Any]", self._config.extension_config.get("litestar", {}))
        table_options, hash_clause = _cockroach_storage_clauses(settings)
        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            session_id TEXT PRIMARY KEY,
            data BYTEA NOT NULL,
            expires_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        ){table_options};

        CREATE INDEX IF NOT EXISTS idx_{self._table_name}_expires_at
        ON {self._table_name}(expires_at){hash_clause} WHERE expires_at IS NOT NULL;
        """

    def _drop_table_sql(self) -> "list[str]":
        return [f"DROP INDEX IF EXISTS idx_{self._table_name}_expires_at", f"DROP TABLE IF EXISTS {self._table_name}"]

    def _create_table(self) -> None:
        sql = self._table_ddl()
        with self._config.provide_session() as driver:
            driver.execute_script(sql)
            driver.commit()
        self._log_table_created()

    def _get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        with self._config.provide_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
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

    def _delete(self, key: str) -> None:
        sql = f"DELETE FROM {self._table_name} WHERE session_id = %s"

        with self._config.provide_connection() as conn:
            conn.execute(sql.encode(), (key,))
            conn.commit()

    def _delete_all(self) -> None:
        sql = f"DELETE FROM {self._table_name}"

        with self._config.provide_connection() as conn:
            conn.execute(sql.encode())
            conn.commit()
        self._log_delete_all()

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

    def _expires_in(self, key: str) -> "int | None":
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = %s
        """

        with self._config.provide_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
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

    def _delete_expired(self) -> int:
        sql = f"DELETE FROM {self._table_name} WHERE expires_at <= CURRENT_TIMESTAMP"

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql.encode())
            conn.commit()
            count = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            if count > 0:
                self._log_delete_expired(count)
            return count


def _cockroach_storage_clauses(settings: "dict[str, Any]") -> "tuple[str, str]":
    ttl = settings.get("ttl_expiration_expression", False)
    if ttl not in {False, None, True, "expires_at"}:
        msg = "extension_config['litestar']['ttl_expiration_expression'] must be true, false, or 'expires_at'"
        raise ValueError(msg)
    table_options = " WITH (ttl_expiration_expression = 'expires_at')" if ttl else ""
    if not settings.get("enable_hash_sharded_indexes", False):
        return table_options, ""
    bucket_count = settings.get("hash_shard_bucket_count")
    if bucket_count is None:
        return table_options, " USING HASH"
    if not isinstance(bucket_count, int) or isinstance(bucket_count, bool) or bucket_count <= 0:
        msg = "extension_config['litestar']['hash_shard_bucket_count'] must be a positive integer"
        raise ValueError(msg)
    return table_options, f" USING HASH WITH (bucket_count = {bucket_count})"
