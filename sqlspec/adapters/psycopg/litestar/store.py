"""Psycopg session stores for Litestar integration.

Provides both async and sync PostgreSQL session stores using psycopg3.
"""

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, NotRequired, cast

from sqlspec.adapters.psycopg._typing import psycopg_dict_row as dict_row
from sqlspec.adapters.psycopg._typing import psycopg_errors as errors
from sqlspec.adapters.psycopg.core import build_postgres_storage_params_sql
from sqlspec.config import LitestarConfig
from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig


__all__ = ("PsycopgAsyncStore", "PsycopgLitestarConfig", "PsycopgSyncStore")


class PsycopgLitestarConfig(LitestarConfig):
    """Psycopg-specific Litestar settings.

    Use inside ``extension_config["litestar"]`` with this adapter's session store.
    """

    fillfactor: NotRequired[int]
    """Table fillfactor. Default: 80."""

    autovacuum_vacuum_scale_factor: NotRequired[float]
    """Table autovacuum vacuum scale factor."""

    autovacuum_analyze_scale_factor: NotRequired[float]
    """Table autovacuum analyze scale factor."""


class PsycopgAsyncStore(BaseSQLSpecStore["PsycopgAsyncConfig"]):
    """PostgreSQL session store using Psycopg async driver.

    Implements server-side session storage for Litestar using PostgreSQL
    via the Psycopg (psycopg3) async driver. Provides efficient session
    management with:
    - Native async PostgreSQL operations
    - UPSERT support using ON CONFLICT
    - Automatic expiration handling
    - Efficient cleanup of expired sessions

    Args:
        config: PsycopgAsyncConfig instance.
    """

    __slots__ = ()
    extension_config_options = BaseSQLSpecStore.extension_config_options | frozenset({
        "autovacuum_analyze_scale_factor",
        "autovacuum_vacuum_scale_factor",
        "fillfactor",
    })

    def __init__(self, config: "PsycopgAsyncConfig") -> None:
        """Initialize Psycopg async session store.

        Args:
            config: PsycopgAsyncConfig instance.
        """
        super().__init__(config)

    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
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
        """Get a session value by key.

        Args:
            key: Session ID to retrieve.
            renew_for: If given, renew the expiry time for this duration.

        Returns:
            Session data as bytes if found and not expired, None otherwise.
        """
        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        try:
            conn_context = self._config.provide_connection()
            async with conn_context as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(cast("Any", sql), (key,))
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
                        await conn.execute(cast("Any", update_sql), (new_expires_at, key))
                        await conn.commit()

                return bytes(row["data"])
        except errors.UndefinedTable:
            return None

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Store a session value.

        Args:
            key: Session ID.
            value: Session data.
            expires_in: Time until expiration.
        """
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
            await conn.execute(cast("Any", sql), (key, data, expires_at))
            await conn.commit()

    async def delete(self, key: str) -> None:
        """Delete a session by key.

        Args:
            key: Session ID to delete.
        """
        sql = f"DELETE FROM {self._table_name} WHERE session_id = %s"

        try:
            conn_context = self._config.provide_connection()
            async with conn_context as conn:
                await conn.execute(cast("Any", sql), (key,))
                await conn.commit()
        except errors.UndefinedTable:
            return

    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        sql = f"DELETE FROM {self._table_name}"

        try:
            conn_context = self._config.provide_connection()
            async with conn_context as conn:
                await conn.execute(cast("Any", sql))
                await conn.commit()
            self._log_delete_all()
        except errors.UndefinedTable:
            return

    async def exists(self, key: str) -> bool:
        """Check if a session key exists and is not expired.

        Args:
            key: Session ID to check.

        Returns:
            True if the session exists and is not expired.
        """
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        try:
            conn_context = self._config.provide_connection()
            async with conn_context as conn, conn.cursor() as cur:
                await cur.execute(cast("Any", sql), (key,))
                result = await cur.fetchone()
                return result is not None
        except errors.UndefinedTable:
            return False

    async def expires_in(self, key: str) -> "int | None":
        """Get the time in seconds until the session expires.

        Args:
            key: Session ID to check.

        Returns:
            Seconds until expiration, or None if no expiry or key doesn't exist.
        """
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = %s
        """

        try:
            conn_context = self._config.provide_connection()
            async with conn_context as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(cast("Any", sql), (key,))
                    row = await cur.fetchone()

                if row is None or row["expires_at"] is None:
                    return None

                expires_at = row["expires_at"]
                now = datetime.now(UTC)
                if expires_at <= now:
                    return 0

                delta = expires_at - now
                return int(delta.total_seconds())
        except errors.UndefinedTable:
            return None

    async def delete_expired(self) -> int:
        """Delete all expired sessions.

        Returns:
            Number of sessions deleted.
        """
        sql = f"DELETE FROM {self._table_name} WHERE expires_at <= CURRENT_TIMESTAMP"

        try:
            conn_context = self._config.provide_connection()
            async with conn_context as conn, conn.cursor() as cur:
                await cur.execute(cast("Any", sql))
                await conn.commit()
                count = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
                if count > 0:
                    self._log_delete_expired(count)
                return count
        except errors.UndefinedTable:
            return 0

    def _table_ddl(self) -> str:
        """Get PostgreSQL CREATE TABLE SQL with optimized schema.

        Returns:
            SQL statement to create the sessions table with proper indexes.
        """
        tuning_clause = build_postgres_storage_params_sql(
            self._config.extension_config.get("litestar", {}),
            default_fillfactor=80,
            default_vacuum_scale=0.05,
            default_analyze_scale=0.02,
            include_autovacuum=True,
            section="litestar",
        )
        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            session_id TEXT PRIMARY KEY,
            data BYTEA NOT NULL,
            expires_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        ){tuning_clause};

        CREATE INDEX IF NOT EXISTS idx_{self._table_name}_expires_at
        ON {self._table_name}(expires_at) WHERE expires_at IS NOT NULL;
        """

    def _drop_table_sql(self) -> "list[str]":
        """Get PostgreSQL DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop indexes and table.
        """
        return [f"DROP INDEX IF EXISTS idx_{self._table_name}_expires_at", f"DROP TABLE IF EXISTS {self._table_name}"]


class PsycopgSyncStore(BaseSQLSpecStore["PsycopgSyncConfig"]):
    """PostgreSQL session store using Psycopg sync driver.

    Implements server-side session storage for Litestar using PostgreSQL
    via the synchronous Psycopg (psycopg3) driver. Uses Litestar's sync_to_thread
    utility to provide an async interface compatible with the Store protocol.

    Provides efficient session management with:
        - Sync operations wrapped for async compatibility
        - UPSERT support using ON CONFLICT
        - Automatic expiration handling
        - Efficient cleanup of expired sessions

    Args:
        config: PsycopgSyncConfig instance.
    """

    __slots__ = ()
    extension_config_options = BaseSQLSpecStore.extension_config_options | frozenset({
        "autovacuum_analyze_scale_factor",
        "autovacuum_vacuum_scale_factor",
        "fillfactor",
    })

    def __init__(self, config: "PsycopgSyncConfig") -> None:
        """Initialize Psycopg sync session store.

        Args:
            config: PsycopgSyncConfig instance.
        """
        super().__init__(config)

    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        await async_(self._create_table)()
        await self.reconcile_schema(assume_existing=True)

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Get a session value by key.

        Args:
            key: Session ID to retrieve.
            renew_for: If given, renew the expiry time for this duration.

        Returns:
            Session data as bytes if found and not expired, None otherwise.
        """
        return await async_(self._get)(key, renew_for)

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Store a session value.

        Args:
            key: Session ID.
            value: Session data.
            expires_in: Time until expiration.
        """
        await async_(self._set)(key, value, expires_in)

    async def delete(self, key: str) -> None:
        """Delete a session by key.

        Args:
            key: Session ID to delete.
        """
        await async_(self._delete)(key)

    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        await async_(self._delete_all)()

    async def exists(self, key: str) -> bool:
        """Check if a session key exists and is not expired.

        Args:
            key: Session ID to check.

        Returns:
            True if the session exists and is not expired.
        """
        return await async_(self._exists)(key)

    async def expires_in(self, key: str) -> "int | None":
        """Get the time in seconds until the session expires.

        Args:
            key: Session ID to check.

        Returns:
            Seconds until expiration, or None if no expiry or key doesn't exist.
        """
        return await async_(self._expires_in)(key)

    async def delete_expired(self) -> int:
        """Delete all expired sessions.

        Returns:
            Number of sessions deleted.
        """
        return await async_(self._delete_expired)()

    def _table_ddl(self) -> str:
        """Get PostgreSQL CREATE TABLE SQL with optimized schema.

        Returns:
            SQL statement to create the sessions table with proper indexes.
        """
        tuning_clause = build_postgres_storage_params_sql(
            self._config.extension_config.get("litestar", {}),
            default_fillfactor=80,
            default_vacuum_scale=0.05,
            default_analyze_scale=0.02,
            include_autovacuum=True,
            section="litestar",
        )
        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            session_id TEXT PRIMARY KEY,
            data BYTEA NOT NULL,
            expires_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        ){tuning_clause};

        CREATE INDEX IF NOT EXISTS idx_{self._table_name}_expires_at
        ON {self._table_name}(expires_at) WHERE expires_at IS NOT NULL;
        """

    def _drop_table_sql(self) -> "list[str]":
        """Get PostgreSQL DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop indexes and table.
        """
        return [f"DROP INDEX IF EXISTS idx_{self._table_name}_expires_at", f"DROP TABLE IF EXISTS {self._table_name}"]

    def _create_table(self) -> None:
        """Synchronous implementation of create_table."""
        sql = self._table_ddl()
        with self._config.provide_session() as driver:
            driver.execute_script(sql)
            driver.commit()
        self._log_table_created()

    def _get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Synchronous implementation of get."""
        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        try:
            with self._config.provide_connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(cast("Any", sql), (key,))
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
                        conn.execute(cast("Any", update_sql), (new_expires_at, key))
                        conn.commit()

                return bytes(row["data"])
        except errors.UndefinedTable:
            return None

    def _set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Synchronous implementation of set."""
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
            conn.execute(cast("Any", sql), (key, data, expires_at))
            conn.commit()

    def _delete(self, key: str) -> None:
        """Synchronous implementation of delete."""
        sql = f"DELETE FROM {self._table_name} WHERE session_id = %s"

        try:
            with self._config.provide_connection() as conn:
                conn.execute(cast("Any", sql), (key,))
                conn.commit()
        except errors.UndefinedTable:
            return

    def _delete_all(self) -> None:
        """Synchronous implementation of delete_all."""
        sql = f"DELETE FROM {self._table_name}"

        try:
            with self._config.provide_connection() as conn:
                conn.execute(cast("Any", sql))
                conn.commit()
            self._log_delete_all()
        except errors.UndefinedTable:
            return

    def _exists(self, key: str) -> bool:
        """Synchronous implementation of exists."""
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        try:
            with self._config.provide_connection() as conn, conn.cursor() as cur:
                cur.execute(cast("Any", sql), (key,))
                result = cur.fetchone()
                return result is not None
        except errors.UndefinedTable:
            return False

    def _expires_in(self, key: str) -> "int | None":
        """Synchronous implementation of expires_in."""
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = %s
        """

        try:
            with self._config.provide_connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(cast("Any", sql), (key,))
                    row = cur.fetchone()

                if row is None or row["expires_at"] is None:
                    return None

                expires_at = row["expires_at"]
                now = datetime.now(UTC)

                if expires_at <= now:
                    return 0

                delta = expires_at - now
                return int(delta.total_seconds())
        except errors.UndefinedTable:
            return None

    def _delete_expired(self) -> int:
        """Synchronous implementation of delete_expired."""
        sql = f"DELETE FROM {self._table_name} WHERE expires_at <= CURRENT_TIMESTAMP"

        try:
            with self._config.provide_connection() as conn, conn.cursor() as cur:
                cur.execute(cast("Any", sql))
                conn.commit()
                count = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
                if count > 0:
                    self._log_delete_expired(count)
                return count
        except errors.UndefinedTable:
            return 0
