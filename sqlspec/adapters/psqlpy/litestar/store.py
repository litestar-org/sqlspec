"""Psqlpy session store for Litestar integration."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sqlspec.extensions.litestar.store import BaseSQLSpecStore

if TYPE_CHECKING:
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig


__all__ = ("PsqlpyStore",)


class PsqlpyStore(BaseSQLSpecStore["PsqlpyConfig"]):
    """PostgreSQL session store using Psqlpy driver.

    Implements server-side session storage for Litestar using PostgreSQL
    via the Psqlpy driver (Rust-based async driver). Provides efficient
    session management with:
    - Native async PostgreSQL operations via Rust
    - UPSERT support using ON CONFLICT
    - Automatic expiration handling
    - Efficient cleanup of expired sessions

    Args:
        config: PsqlpyConfig instance.

    Example:
        from sqlspec.adapters.psqlpy import PsqlpyConfig
        from sqlspec.adapters.psqlpy.litestar.store import PsqlpyStore

        config = PsqlpyConfig(connection_config={"dsn": "postgresql://..."})
        store = PsqlpyStore(config)
        await store.create_table()
    """

    __slots__ = ()

    def __init__(self, config: "PsqlpyConfig") -> None:
        """Initialize Psqlpy session store.

        Args:
            config: PsqlpyConfig instance.

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
        WHERE session_id = $1
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        async with self._config.provide_connection() as conn:
            query_result = await conn.fetch(sql, [key])
            rows = query_result.result()

            if not rows:
                return None

            row = rows[0]

            if renew_for is not None and row["expires_at"] is not None:
                new_expires_at = self._calculate_expires_at(renew_for)
                if new_expires_at is not None:
                    update_sql = f"""
                    UPDATE {self._table_name}
                    SET expires_at = $1, updated_at = CURRENT_TIMESTAMP
                    WHERE session_id = $2
                    """
                    await conn.execute(update_sql, [new_expires_at, key])

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
        VALUES ($1, $2, $3)
        ON CONFLICT (session_id)
        DO UPDATE SET
            data = EXCLUDED.data,
            expires_at = EXCLUDED.expires_at,
            updated_at = CURRENT_TIMESTAMP
        """

        async with self._config.provide_connection() as conn:
            await conn.execute(sql, [key, data, expires_at])

    async def delete(self, key: str) -> None:
        """Delete a session by key.

        Args:
            key: Session ID to delete.
        """
        sql = f"DELETE FROM {self._table_name} WHERE session_id = $1"

        async with self._config.provide_connection() as conn:
            await conn.execute(sql, [key])

    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        sql = f"DELETE FROM {self._table_name}"

        async with self._config.provide_connection() as conn:
            await conn.execute(sql)
        self._log_delete_all()

    async def exists(self, key: str) -> bool:
        """Check if a session key exists and is not expired.

        Args:
            key: Session ID to check.

        Returns:
            True if the session exists and is not expired.

        Notes:
            Uses CURRENT_TIMESTAMP for consistency with get() method.
            Uses fetch() instead of fetch_val() to handle zero-row case.
        """
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = $1
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        async with self._config.provide_connection() as conn:
            query_result = await conn.fetch(sql, [key])
            rows = query_result.result()
            return len(rows) > 0

    async def expires_in(self, key: str) -> "int | None":
        """Get the time in seconds until the session expires.

        Args:
            key: Session ID to check.

        Returns:
            Seconds until expiration, or None if no expiry or key doesn't exist.

        Notes:
            Uses fetch() to handle the case where the key doesn't exist.
        """
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = $1
        """

        async with self._config.provide_connection() as conn:
            query_result = await conn.fetch(sql, [key])
            rows = query_result.result()

            if not rows:
                return None

            expires_at = rows[0]["expires_at"]

            if expires_at is None:
                return None

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
            Uses RETURNING to get deleted row count since psqlpy QueryResult
            doesn't expose command tags.
            For very large tables (10M+ rows), consider batching deletes
            to avoid holding locks too long.
        """
        sql = f"""
        DELETE FROM {self._table_name}
        WHERE expires_at <= CURRENT_TIMESTAMP
        RETURNING session_id
        """

        async with self._config.provide_connection() as conn:
            query_result = await conn.fetch(sql, [])
            rows = query_result.result()
            count = len(rows)
            if count > 0:
                self._log_delete_expired(count)
            return count
