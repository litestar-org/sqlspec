"""Snowflake server-side session persistence for Litestar."""

from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar

from sqlspec.adapters.snowflake.config import SnowflakeConfig
from sqlspec.extensions.litestar.store import BaseSQLSpecStore, CompanionIndex
from sqlspec.utils.sync_tools import async_

__all__ = ("SnowflakeStore",)


class SnowflakeStore(BaseSQLSpecStore[SnowflakeConfig]):
    """Persist session bytes in Snowflake BINARY columns."""

    __slots__ = ()
    _dialect: ClassVar[str] = "snowflake"

    def get_companion_indexes(self) -> list[CompanionIndex]:
        """Return the empty index collection for standard Snowflake tables."""
        return []

    async def create_table(self) -> None:
        """Create and reconcile the configured session table."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        await async_(self._create_table)()
        await self.reconcile_schema(assume_existing=True)

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Read an unexpired session, optionally renewing its finite TTL."""
        return await async_(self._get)(key, renew_for)

    async def set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        """Write session bytes and an optional expiration time."""
        await async_(self._set)(key, value, expires_in)

    async def delete(self, key: str) -> None:
        """Delete the session identified by key."""
        await async_(self._delete)(key)

    async def delete_all(self) -> None:
        """Delete every session in the configured table."""
        await async_(self._delete_all)()

    async def exists(self, key: str) -> bool:
        """Return whether an unexpired session exists."""
        return await async_(self._exists)(key)

    async def expires_in(self, key: str) -> int | None:
        """Return remaining TTL seconds or None for missing/permanent sessions."""
        return await async_(self._expires_in)(key)

    async def delete_expired(self) -> int:
        """Delete expired sessions and return the affected row count."""
        return await async_(self._delete_expired)()

    def _table_ddl(self) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {self._table_name} ("
            "session_id VARCHAR(255) PRIMARY KEY, data BINARY NOT NULL, expires_at TIMESTAMP_TZ)"
        )

    def _drop_table_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {self._table_name}"]

    @staticmethod
    def _decode_binary(raw: Any) -> bytes | None:
        if raw is None:
            return None
        return bytes.fromhex(raw) if isinstance(raw, str) else bytes(raw)

    @staticmethod
    def _timestamp_to_datetime(value: str | datetime | None) -> datetime | None:
        if value is None:
            return None
        parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)

    def _create_table(self) -> None:
        with self._config.provide_session() as driver:
            driver.execute_script(self._table_ddl())
        self._log_table_created()

    def _get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        sql = (
            f'SELECT data AS "data", expires_at AS "expires_at" FROM {self._table_name} WHERE session_id = ? '
            "AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP())"
        )
        with self._config.provide_session() as driver:
            row = driver.select_one_or_none(sql, (key,))
            if row is None:
                return None
            expires_at = row["expires_at"]
            if self._should_renew(renew_for) and expires_at is not None:
                renewed = self._calculate_expires_at(renew_for)
                if renewed is not None:
                    driver.execute(f"UPDATE {self._table_name} SET expires_at = ? WHERE session_id = ?", (renewed, key))
                    driver.commit()
            return self._decode_binary(row["data"])

    def _set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        sql = (
            f"MERGE INTO {self._table_name} t "
            "USING (SELECT ? AS session_id, TO_BINARY(?, 'HEX') AS data, ? AS expires_at) s "
            "ON t.session_id = s.session_id "
            "WHEN MATCHED THEN UPDATE SET data = s.data, expires_at = s.expires_at "
            "WHEN NOT MATCHED THEN INSERT (session_id, data, expires_at) VALUES (s.session_id, s.data, s.expires_at)"
        )
        with self._config.provide_session() as driver:
            driver.execute(sql, (key, self._value_to_bytes(value).hex(), self._calculate_expires_at(expires_in)))
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
        sql = (
            f"SELECT 1 AS found FROM {self._table_name} WHERE session_id = ? "
            "AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP()) LIMIT 1"
        )
        with self._config.provide_session() as driver:
            return driver.select_one_or_none(sql, (key,)) is not None

    def _expires_in(self, key: str) -> int | None:
        with self._config.provide_session() as driver:
            row = driver.select_one_or_none(
                f'SELECT expires_at AS "expires_at" FROM {self._table_name} WHERE session_id = ?', (key,)
            )
        if row is None:
            return None
        expires_at = self._timestamp_to_datetime(row["expires_at"])
        return None if expires_at is None else max(int((expires_at - datetime.now(UTC)).total_seconds()), 0)

    def _delete_expired(self) -> int:
        with self._config.provide_session() as driver:
            result = driver.execute(f"DELETE FROM {self._table_name} WHERE expires_at <= CURRENT_TIMESTAMP()")
            driver.commit()
        count = max(result.get_affected_count(), 0)
        if count:
            self._log_delete_expired(count)
        return count
