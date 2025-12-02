"""Spanner session store for Litestar integration."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

from google.cloud.spanner_v1 import param_types

from sqlspec.adapters.spanner._type_handlers import bytes_to_spanner, spanner_to_bytes
from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.logging import get_logger
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from google.cloud.spanner_v1.database import Database
    from google.cloud.spanner_v1.transaction import Transaction

    from sqlspec.adapters.spanner.config import SpannerSyncConfig

logger = get_logger("adapters.spanner.litestar.store")

__all__ = ("SpannerSyncStore",)


class SpannerSyncStore(BaseSQLSpecStore["SpannerSyncConfig"]):
    """Spanner-backed Litestar session store using sync driver wrapped as async."""

    __slots__ = ("_index_options", "_shard_count", "_table_options")

    def __init__(self, config: "SpannerSyncConfig") -> None:
        super().__init__(config)
        litestar_cfg = cast("dict[str, Any]", getattr(config, "extension_config", {}).get("litestar", {}))
        self._shard_count: int = int(litestar_cfg.get("shard_count", 0)) if litestar_cfg.get("shard_count") else 0
        self._table_options: str | None = litestar_cfg.get("table_options")
        self._index_options: str | None = litestar_cfg.get("index_options")

    def _database(self) -> "Database":
        return self._config.get_database()

    def _datetime_to_timestamp(self, dt: "datetime | None") -> "datetime | None":
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt

    def _timestamp_to_datetime(self, ts: "datetime | None") -> "datetime | None":
        if ts is None:
            return None
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts

    def _build_params(
        self, key: str, expires_at: "datetime | None" = None, data: "bytes | None" = None
    ) -> dict[str, Any]:
        return {
            "session_id": key,
            "data": bytes_to_spanner(data),
            "expires_at": self._datetime_to_timestamp(expires_at),
        }

    def _get_param_types(self, session_id: bool = True, expires_at: bool = False, data: bool = False) -> dict[str, Any]:
        types: dict[str, Any] = {}
        if session_id:
            types["session_id"] = param_types.STRING
        if expires_at:
            types["expires_at"] = param_types.TIMESTAMP
        if data:
            types["data"] = param_types.BYTES
        return types

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        return await async_(self._get)(key, renew_for)

    def _get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        sql = f"""
        SELECT data, expires_at
        FROM {self._table_name}
        WHERE session_id = @session_id
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP())
        """
        if self._shard_count > 1:
            sql = f"""
            SELECT data, expires_at
            FROM {self._table_name}
            WHERE shard_id = MOD(FARM_FINGERPRINT(@session_id), {self._shard_count})
            AND session_id = @session_id
            AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP())
            """

        with self._config.provide_session() as driver:
            result = driver.select_one_or_none(sql, {"session_id": key})

        if result is None:
            return None

        data = result.get("data")
        expires_at = self._timestamp_to_datetime(result.get("expires_at"))

        if renew_for is not None and expires_at is not None:
            new_expires = self._calculate_expires_at(renew_for)
            update_sql = f"""
            UPDATE {self._table_name}
            SET expires_at = @expires_at, updated_at = PENDING_COMMIT_TIMESTAMP()
            WHERE session_id = @session_id
            """
            if self._shard_count > 1:
                update_sql = f"{update_sql} AND shard_id = MOD(FARM_FINGERPRINT(@session_id), {self._shard_count})"
            params = self._build_params(key, new_expires)
            types = self._get_param_types(expires_at=True)

            def _renew_txn(transaction: "Transaction") -> None:
                transaction.execute_update(update_sql, params=params, param_types=types)  # type: ignore[no-untyped-call]

            self._database().run_in_transaction(_renew_txn)  # type: ignore[no-untyped-call]

        return spanner_to_bytes(data)

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        await async_(self._set)(key, value, expires_in)

    def _set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        params = self._build_params(key, expires_at, data)
        types = self._get_param_types(session_id=True, expires_at=True, data=True)

        update_sql = f"""
        UPDATE {self._table_name}
        SET data = @data,
            expires_at = @expires_at,
            updated_at = PENDING_COMMIT_TIMESTAMP()
        WHERE session_id = @session_id
        """
        if self._shard_count > 1:
            update_sql = f"{update_sql} AND shard_id = MOD(FARM_FINGERPRINT(@session_id), {self._shard_count})"
        insert_sql = f"""
        INSERT {self._table_name} (session_id, data, expires_at, created_at, updated_at)
        VALUES (@session_id, @data, @expires_at, PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP())
        """

        def _set_txn(transaction: "Transaction") -> None:
            row_ct = transaction.execute_update(update_sql, params=params, param_types=types)  # type: ignore[no-untyped-call]
            if row_ct == 0:
                transaction.execute_update(insert_sql, params=params, param_types=types)  # type: ignore[no-untyped-call]

        self._database().run_in_transaction(_set_txn)  # type: ignore[no-untyped-call]

    async def delete(self, key: str) -> None:
        await async_(self._delete)(key)

    def _delete(self, key: str) -> None:
        sql = f"DELETE FROM {self._table_name} WHERE session_id = @session_id"
        if self._shard_count > 1:
            sql = f"{sql} AND shard_id = MOD(FARM_FINGERPRINT(@session_id), {self._shard_count})"
        params = {"session_id": key}
        types = self._get_param_types(session_id=True)

        def _delete_txn(transaction: "Transaction") -> None:
            transaction.execute_update(sql, params=params, param_types=types)  # type: ignore[no-untyped-call]

        self._database().run_in_transaction(_delete_txn)  # type: ignore[no-untyped-call]

    async def delete_all(self) -> None:
        await async_(self._delete_all)()

    def _delete_all(self) -> None:
        sql = f"DELETE FROM {self._table_name} WHERE TRUE"

        def _delete_all_txn(transaction: "Transaction") -> None:
            transaction.execute_update(sql)  # type: ignore[no-untyped-call]

        self._database().run_in_transaction(_delete_all_txn)  # type: ignore[no-untyped-call]

    async def exists(self, key: str) -> bool:
        return await async_(self._exists)(key)

    def _exists(self, key: str) -> bool:
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = @session_id
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP())
        LIMIT 1
        """
        with self._config.provide_session() as driver:
            row = driver.select_one_or_none(sql, {"session_id": key})
            return row is not None

    async def expires_in(self, key: str) -> "int | None":
        return await async_(self._expires_in)(key)

    def _expires_in(self, key: str) -> "int | None":
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = @session_id
        """
        if self._shard_count > 1:
            sql = f"{sql} AND shard_id = MOD(FARM_FINGERPRINT(@session_id), {self._shard_count})"
        with self._config.provide_session() as driver:
            row = driver.select_one_or_none(sql, {"session_id": key})
            if row is None:
                return None
            expires_at = self._timestamp_to_datetime(row.get("expires_at"))
            if expires_at is None:
                return None
            delta = expires_at - datetime.now(timezone.utc)
            return max(int(delta.total_seconds()), 0)

    async def delete_expired(self) -> int:
        return await async_(self._delete_expired)()

    def _delete_expired(self) -> int:
        sql = f"""
        DELETE FROM {self._table_name}
        WHERE expires_at IS NOT NULL AND expires_at <= CURRENT_TIMESTAMP()
        """
        deleted_count: list[int] = [0]

        def _delete_expired_txn(transaction: "Transaction") -> None:
            row_ct = transaction.execute_update(sql)  # type: ignore[no-untyped-call]
            deleted_count[0] = row_ct

        self._database().run_in_transaction(_delete_expired_txn)  # type: ignore[no-untyped-call]
        return deleted_count[0]

    async def create_table(self) -> None:
        await async_(self._create_table)()

    def _create_table(self) -> None:
        database = self._config.get_database()
        existing_tables = {t.table_id for t in database.list_tables()}  # type: ignore[no-untyped-call]

        if self._table_name not in existing_tables:
            ddl_statements = [self._get_create_table_sql(), self._get_create_index_sql()]
            database.update_ddl(ddl_statements).result(300)  # type: ignore[no-untyped-call]

    def _get_create_table_sql(self) -> str:
        shard_column = ""
        pk = "PRIMARY KEY (session_id)"
        if self._shard_count > 1:
            shard_column = f",\n  shard_id INT64 AS (MOD(FARM_FINGERPRINT(session_id), {self._shard_count})) STORED"
            pk = "PRIMARY KEY (shard_id, session_id)"
        options = ""
        if self._table_options:
            options = f"\nOPTIONS ({self._table_options})"
        return f"""
CREATE TABLE {self._table_name} (
  session_id STRING(128) NOT NULL,
  data BYTES(MAX) NOT NULL,
  expires_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  updated_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true){shard_column}
) {pk}{options}
"""

    def _get_create_index_sql(self) -> str:
        leading = "expires_at"
        if self._shard_count > 1:
            leading = "shard_id, expires_at"
        opts = ""
        if self._index_options:
            opts = f" OPTIONS ({self._index_options})"
        return f"CREATE INDEX idx_{self._table_name}_expires_at ON {self._table_name}({leading}){opts}"

    def _get_drop_table_sql(self) -> "list[str]":
        return [f"DROP INDEX idx_{self._table_name}_expires_at", f"DROP TABLE {self._table_name}"]
