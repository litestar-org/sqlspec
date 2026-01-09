"""Psycopg session stores for Litestar integration.

Provides both async and sync PostgreSQL session stores using psycopg3.
"""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig


__all__ = ("PsycopgAsyncStore", "PsycopgSyncStore")


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

    Example:
        from sqlspec.adapters.psycopg import PsycopgAsyncConfig
        from sqlspec.adapters.psycopg.litestar.store import PsycopgAsyncStore

        config = PsycopgAsyncConfig(connection_config={"conninfo": "postgresql://..."})
        store = PsycopgAsyncStore(config)
        await store.create_table()
    """

    __slots__ = ()

    def __init__(self, config: "PsycopgAsyncConfig") -> None:
        """Initialize Psycopg async session store.

        Args:
            config: PsycopgAsyncConfig instance.

        Notes:
            Table name is read from config.extension_config["litestar"]["session_table"].
        """
        super().__init__(config)

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

    def _get_drop_table_sql(self) -> "list[str]":
        """Get PostgreSQL DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop indexes and table.
        """
        return [f"DROP INDEX IF EXISTS idx_{self._table_name}_expires_at", f"DROP TABLE IF EXISTS {self._table_name}"]

    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        sql = self._get_create_table_sql()
        async with self._config.provide_session() as driver:
            await driver.execute_script(sql)
            await driver.commit()
        self._log_table_created()

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Get a session value by key.

        Args:
            key: Session ID to retrieve.
            renew_for: If given, renew the expiry time for this duration.

        Returns:
            Session data as bytes if found and not expired, None otherwise.

        Notes:
            Uses CURRENT_TIMESTAMP instead of NOW() for SQL standard compliance.
            The query planner can use the partial index for expires_at > CURRENT_TIMESTAMP.
        """
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
        """Store a session value.

        Args:
            key: Session ID.
            value: Session data.
            expires_in: Time until expiration.

        Notes:
            Uses EXCLUDED to reference the proposed insert values in ON CONFLICT.
            Updates updated_at timestamp on every write for audit trail.
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
            await conn.execute(sql.encode(), (key, data, expires_at))
            await conn.commit()

    async def delete(self, key: str) -> None:
        """Delete a session by key.

        Args:
            key: Session ID to delete.
        """
        sql = f"DELETE FROM {self._table_name} WHERE session_id = %s"

        conn_context = self._config.provide_connection()
        async with conn_context as conn:
            await conn.execute(sql.encode(), (key,))
            await conn.commit()

    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        sql = f"DELETE FROM {self._table_name}"

        conn_context = self._config.provide_connection()
        async with conn_context as conn:
            await conn.execute(sql.encode())
            await conn.commit()
        self._log_delete_all()

    async def exists(self, key: str) -> bool:
        """Check if a session key exists and is not expired.

        Args:
            key: Session ID to check.

        Returns:
            True if the session exists and is not expired.

        Notes:
            Uses CURRENT_TIMESTAMP for consistency with get() method.
        """
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        conn_context = self._config.provide_connection()
        async with conn_context as conn, conn.cursor() as cur:
            await cur.execute(sql.encode(), (key,))
            result = await cur.fetchone()
            return result is not None

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
        """Delete all expired sessions.

        Returns:
            Number of sessions deleted.

        Notes:
            Uses CURRENT_TIMESTAMP for consistency.
            For very large tables (10M+ rows), consider batching deletes
            to avoid holding locks too long.
        """
        sql = f"DELETE FROM {self._table_name} WHERE expires_at <= CURRENT_TIMESTAMP"

        conn_context = self._config.provide_connection()
        async with conn_context as conn, conn.cursor() as cur:
            await cur.execute(sql.encode())
            await conn.commit()
            count = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            if count > 0:
                self._log_delete_expired(count)
            return count


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

    Note:
        For high-concurrency applications, consider using PsycopgAsyncStore instead,
        as it provides native async operations without threading overhead.

    Args:
        config: PsycopgSyncConfig instance.

    Example:
        from sqlspec.adapters.psycopg import PsycopgSyncConfig
        from sqlspec.adapters.psycopg.litestar.store import PsycopgSyncStore

        config = PsycopgSyncConfig(connection_config={"conninfo": "postgresql://..."})
        store = PsycopgSyncStore(config)
        await store.create_table()
    """

    __slots__ = ()

    def __init__(self, config: "PsycopgSyncConfig") -> None:
        """Initialize Psycopg sync session store.

        Args:
            config: PsycopgSyncConfig instance.

        Notes:
            Table name is read from config.extension_config["litestar"]["session_table"].
        """
        super().__init__(config)

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

    def _get_drop_table_sql(self) -> "list[str]":
        """Get PostgreSQL DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop indexes and table.
        """
        return [f"DROP INDEX IF EXISTS idx_{self._table_name}_expires_at", f"DROP TABLE IF EXISTS {self._table_name}"]

    def _create_table(self) -> None:
        """Synchronous implementation of create_table."""
        sql = self._get_create_table_sql()
        with self._config.provide_session() as driver:
            driver.execute_script(sql)
            driver.commit()
        self._log_table_created()

    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        await async_(self._create_table)()

    def _get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Synchronous implementation of get.

        Notes:
            Uses CURRENT_TIMESTAMP for SQL standard compliance.
        """
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
        """Get a session value by key.

        Args:
            key: Session ID to retrieve.
            renew_for: If given, renew the expiry time for this duration.

        Returns:
            Session data as bytes if found and not expired, None otherwise.
        """
        return await async_(self._get)(key, renew_for)

    def _set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
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

        with self._config.provide_connection() as conn:
            conn.execute(sql.encode(), (key, data, expires_at))
            conn.commit()

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Store a session value.

        Args:
            key: Session ID.
            value: Session data.
            expires_in: Time until expiration.
        """
        await async_(self._set)(key, value, expires_in)

    def _delete(self, key: str) -> None:
        """Synchronous implementation of delete."""
        sql = f"DELETE FROM {self._table_name} WHERE session_id = %s"

        with self._config.provide_connection() as conn:
            conn.execute(sql.encode(), (key,))
            conn.commit()

    async def delete(self, key: str) -> None:
        """Delete a session by key.

        Args:
            key: Session ID to delete.
        """
        await async_(self._delete)(key)

    def _delete_all(self) -> None:
        """Synchronous implementation of delete_all."""
        sql = f"DELETE FROM {self._table_name}"

        with self._config.provide_connection() as conn:
            conn.execute(sql.encode())
            conn.commit()
        self._log_delete_all()

    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        await async_(self._delete_all)()

    def _exists(self, key: str) -> bool:
        """Synchronous implementation of exists."""
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql.encode(), (key,))
            result = cur.fetchone()
            return result is not None

    async def exists(self, key: str) -> bool:
        """Check if a session key exists and is not expired.

        Args:
            key: Session ID to check.

        Returns:
            True if the session exists and is not expired.
        """
        return await async_(self._exists)(key)

    def _expires_in(self, key: str) -> "int | None":
        """Synchronous implementation of expires_in."""
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
        """Get the time in seconds until the session expires.

        Args:
            key: Session ID to check.

        Returns:
            Seconds until expiration, or None if no expiry or key doesn't exist.
        """
        return await async_(self._expires_in)(key)

    def _delete_expired(self) -> int:
        """Synchronous implementation of delete_expired."""
        sql = f"DELETE FROM {self._table_name} WHERE expires_at <= CURRENT_TIMESTAMP"

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql.encode())
            conn.commit()
            count = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            if count > 0:
                self._log_delete_expired(count)
            return count

    async def delete_expired(self) -> int:
        """Delete all expired sessions.

        Returns:
            Number of sessions deleted.
        """
        return await async_(self._delete_expired)()
