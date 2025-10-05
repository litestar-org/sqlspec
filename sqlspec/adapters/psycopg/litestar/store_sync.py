"""Psycopg sync session store for Litestar integration."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, cast

from litestar.utils.sync import AsyncCallable

from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from sqlspec.adapters.psycopg._types import PsycopgSyncConnection
    from sqlspec.adapters.psycopg.config import PsycopgSyncConfig

logger = get_logger("adapters.psycopg.litestar.store_sync")

__all__ = ("PsycopgSyncStore",)


class PsycopgSyncStore(BaseSQLSpecStore):
    """PostgreSQL session store using Psycopg sync driver.

    Implements server-side session storage for Litestar using PostgreSQL
    via the synchronous Psycopg (psycopg3) driver. Uses Litestar's sync_to_thread
    utility to provide an async interface compatible with the Store protocol.

    Provides efficient session management with:
    - Sync operations wrapped for async compatibility
    - UPSERT support using ON CONFLICT
    - Automatic expiration handling
    - Efficient cleanup of expired sessions

    Note:
        For high-concurrency applications, consider using PsycopgAsyncStore instead,
        as it provides native async operations without threading overhead.

    Args:
        config: PsycopgSyncConfig instance.
        table_name: Name of the session table. Defaults to "sessions".
        cleanup_probability: Probability of running cleanup on set (0.0-1.0).

    Example:
        from sqlspec.adapters.psycopg import PsycopgSyncConfig
        from sqlspec.adapters.psycopg.litestar.store_sync import PsycopgSyncStore

        config = PsycopgSyncConfig(pool_config={"conninfo": "postgresql://..."})
        store = PsycopgSyncStore(config)
        await store.create_table()
    """

    __slots__ = ()

    def __init__(
        self,
        config: "PsycopgSyncConfig",
        table_name: str = "sessions",
        cleanup_probability: float = 0.01,
    ) -> None:
        """Initialize Psycopg sync session store.

        Args:
            config: PsycopgSyncConfig instance.
            table_name: Name of the session table.
            cleanup_probability: Probability of cleanup on set (0.0-1.0).
        """
        super().__init__(config, table_name, cleanup_probability)

    def _get_create_table_sql(self) -> str:
        """Get PostgreSQL CREATE TABLE SQL with optimized schema.

        Returns:
            SQL statement to create the sessions table with proper indexes.

        Notes:
            - Uses TIMESTAMPTZ for timezone-aware expiration timestamps
            - Partial index WHERE expires_at IS NOT NULL reduces index size/maintenance
            - FILLFACTOR 80 leaves space for HOT updates, reducing table bloat
            - Audit columns (created_at, updated_at) help with debugging
            - Table name is internally controlled, not user input (S608 suppressed)
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            session_id TEXT PRIMARY KEY,
            data BYTEA NOT NULL,
            expires_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) WITH (fillfactor = 80);

        CREATE INDEX IF NOT EXISTS idx_{self._table_name}_expires_at
        ON {self._table_name}(expires_at) WHERE expires_at IS NOT NULL;

        ALTER TABLE {self._table_name} SET (
            autovacuum_vacuum_scale_factor = 0.05,
            autovacuum_analyze_scale_factor = 0.02
        );
        """

    def _create_table_sync(self) -> None:
        """Synchronous implementation of create_table."""
        sql = self._get_create_table_sql()
        with cast("AbstractContextManager[PsycopgSyncConnection]", self._config.provide_connection()) as conn:
            with conn.cursor() as cur:
                for statement in sql.strip().split(";"):
                    statement = statement.strip()
                    if statement:
                        cur.execute(statement)
            conn.commit()
        logger.debug("Created session table: %s", self._table_name)

    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        await AsyncCallable(self._create_table_sync)()

    def _get_sync(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Synchronous implementation of get.

        Notes:
            Uses CURRENT_TIMESTAMP for SQL standard compliance.
        """
        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        with cast("AbstractContextManager[PsycopgSyncConnection]", self._config.provide_connection()) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (key,))
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
                    conn.execute(update_sql, (new_expires_at, key))
                    conn.commit()

            return bytes(row["data"])

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Get a session value by key.

        Args:
            key: Session ID to retrieve.
            renew_for: If given, renew the expiry time for this duration.

        Returns:
            Session data as bytes if found and not expired, None otherwise.
        """
        return await AsyncCallable(self._get_sync)(key, renew_for)

    def _set_sync(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Synchronous implementation of set.

        Notes:
            Uses EXCLUDED to reference the proposed insert values in ON CONFLICT.
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

        with cast("AbstractContextManager[PsycopgSyncConnection]", self._config.provide_connection()) as conn:
            conn.execute(sql, (key, data, expires_at))
            conn.commit()

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Store a session value.

        Args:
            key: Session ID.
            value: Session data.
            expires_in: Time until expiration.
        """
        await AsyncCallable(self._set_sync)(key, value, expires_in)

        if self._should_cleanup():
            await self.delete_expired()

    def _delete_sync(self, key: str) -> None:
        """Synchronous implementation of delete."""
        sql = f"DELETE FROM {self._table_name} WHERE session_id = %s"

        with cast("AbstractContextManager[PsycopgSyncConnection]", self._config.provide_connection()) as conn:
            conn.execute(sql, (key,))
            conn.commit()

    async def delete(self, key: str) -> None:
        """Delete a session by key.

        Args:
            key: Session ID to delete.
        """
        await AsyncCallable(self._delete_sync)(key)

    def _delete_all_sync(self) -> None:
        """Synchronous implementation of delete_all."""
        sql = f"DELETE FROM {self._table_name}"

        with cast("AbstractContextManager[PsycopgSyncConnection]", self._config.provide_connection()) as conn:
            conn.execute(sql)
            conn.commit()
        logger.debug("Deleted all sessions from table: %s", self._table_name)

    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        await AsyncCallable(self._delete_all_sync)()

    def _exists_sync(self, key: str) -> bool:
        """Synchronous implementation of exists."""
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        with (
            cast("AbstractContextManager[PsycopgSyncConnection]", self._config.provide_connection()) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(sql, (key,))
            result = cur.fetchone()
            return result is not None

    async def exists(self, key: str) -> bool:
        """Check if a session key exists and is not expired.

        Args:
            key: Session ID to check.

        Returns:
            True if the session exists and is not expired.
        """
        return await AsyncCallable(self._exists_sync)(key)

    def _expires_in_sync(self, key: str) -> "int | None":
        """Synchronous implementation of expires_in."""
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = %s
        """

        with cast("AbstractContextManager[PsycopgSyncConnection]", self._config.provide_connection()) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (key,))
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
        """Get the time in seconds until the session expires.

        Args:
            key: Session ID to check.

        Returns:
            Seconds until expiration, or None if no expiry or key doesn't exist.
        """
        return await AsyncCallable(self._expires_in_sync)(key)

    def _delete_expired_sync(self) -> int:
        """Synchronous implementation of delete_expired."""
        sql = f"DELETE FROM {self._table_name} WHERE expires_at <= CURRENT_TIMESTAMP"

        with (
            cast("AbstractContextManager[PsycopgSyncConnection]", self._config.provide_connection()) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(sql)
            conn.commit()
            count = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            if count > 0:
                logger.debug("Cleaned up %d expired sessions", count)
            return count

    async def delete_expired(self) -> int:
        """Delete all expired sessions.

        Returns:
            Number of sessions deleted.
        """
        return await AsyncCallable(self._delete_expired_sync)()
