"""Oracle session store for Litestar integration."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, cast

from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager

    from sqlspec.adapters.oracledb._types import OracleAsyncConnection
    from sqlspec.adapters.oracledb.config import OracleAsyncConfig

logger = get_logger("adapters.oracledb.litestar.store")

ORACLE_SMALL_BLOB_LIMIT = 32000

__all__ = ("OracleAsyncStore",)


class OracleAsyncStore(BaseSQLSpecStore):
    """Oracle session store using async OracleDB driver.

    Implements server-side session storage for Litestar using Oracle Database
    via the async python-oracledb driver. Provides efficient session management with:
    - Native async Oracle operations
    - MERGE statement for atomic UPSERT
    - Automatic expiration handling
    - Efficient cleanup of expired sessions

    Args:
        config: OracleAsyncConfig instance.
        table_name: Name of the session table. Defaults to "sessions".
        cleanup_probability: Probability of running cleanup on set (0.0-1.0).

    Example:
        from sqlspec.adapters.oracledb import OracleAsyncConfig
        from sqlspec.adapters.oracledb.litestar.store import OracleAsyncStore

        config = OracleAsyncConfig(pool_config={"dsn": "oracle://..."})
        store = OracleAsyncStore(config)
        await store.create_table()
    """

    __slots__ = ()

    def __init__(
        self,
        config: "OracleAsyncConfig",
        table_name: str = "sessions",
        cleanup_probability: float = 0.01,
    ) -> None:
        """Initialize Oracle session store.

        Args:
            config: OracleAsyncConfig instance.
            table_name: Name of the session table.
            cleanup_probability: Probability of cleanup on set (0.0-1.0).
        """
        super().__init__(config, table_name, cleanup_probability)

    def _get_create_table_sql(self) -> str:
        """Get Oracle CREATE TABLE SQL with optimized schema.

        Returns:
            SQL statement to create the sessions table with proper indexes.

        Notes:
            - Uses TIMESTAMP WITH TIME ZONE for timezone-aware expiration timestamps
            - Partial index WHERE expires_at IS NOT NULL reduces index size/maintenance
            - BLOB type for data storage (Oracle native binary type)
            - Audit columns (created_at, updated_at) help with debugging
            - Table name is internally controlled, not user input (S608 suppressed)
        """
        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._table_name} (
                session_id VARCHAR2(255) PRIMARY KEY,
                data BLOB NOT NULL,
                expires_at TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
            )';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """

    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        sql = self._get_create_table_sql()
        conn_context = cast(
            "AbstractAsyncContextManager[OracleAsyncConnection]",
            self._config.provide_connection(),
        )
        async with conn_context as conn:
            cursor = conn.cursor()
            await cursor.execute(sql)
            await conn.commit()

        index_sql = f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._table_name}_expires_at
                ON {self._table_name}(expires_at)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """
        conn_context = cast(
            "AbstractAsyncContextManager[OracleAsyncConnection]",
            self._config.provide_connection(),
        )
        async with conn_context as conn:
            cursor = conn.cursor()
            await cursor.execute(index_sql)
            await conn.commit()

        logger.debug("Created session table: %s", self._table_name)

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Get a session value by key.

        Args:
            key: Session ID to retrieve.
            renew_for: If given, renew the expiry time for this duration.

        Returns:
            Session data as bytes if found and not expired, None otherwise.

        Notes:
            Uses SYSTIMESTAMP for Oracle current timestamp.
            The query uses the index for expires_at > SYSTIMESTAMP.
        """
        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = :session_id
        AND (expires_at IS NULL OR expires_at > SYSTIMESTAMP)
        """

        async with cast("AbstractAsyncContextManager[OracleAsyncConnection]", self._config.provide_connection()) as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, {"session_id": key})
            row = await cursor.fetchone()

            if row is None:
                return None

            data_blob, expires_at = row

            if renew_for is not None and expires_at is not None:
                new_expires_at = self._calculate_expires_at(renew_for)
                if new_expires_at is not None:
                    update_sql = f"""
                    UPDATE {self._table_name}
                    SET expires_at = :expires_at, updated_at = SYSTIMESTAMP
                    WHERE session_id = :session_id
                    """
                    await cursor.execute(update_sql, {"expires_at": new_expires_at, "session_id": key})
                    await conn.commit()

            try:
                return await data_blob.read()
            except AttributeError:
                return bytes(data_blob)

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Store a session value.

        Args:
            key: Session ID.
            value: Session data.
            expires_in: Time until expiration.

        Notes:
            Uses MERGE for atomic UPSERT operation in Oracle.
            Updates updated_at timestamp on every write for audit trail.
            For large BLOBs, uses empty_blob() and then writes data separately.
        """
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)

        async with cast("AbstractAsyncContextManager[OracleAsyncConnection]", self._config.provide_connection()) as conn:
            cursor = conn.cursor()

            if len(data) > ORACLE_SMALL_BLOB_LIMIT:
                merge_sql = f"""
                MERGE INTO {self._table_name} t
                USING (SELECT :session_id AS session_id FROM DUAL) s
                ON (t.session_id = s.session_id)
                WHEN MATCHED THEN
                    UPDATE SET
                        data = EMPTY_BLOB(),
                        expires_at = :expires_at,
                        updated_at = SYSTIMESTAMP
                WHEN NOT MATCHED THEN
                    INSERT (session_id, data, expires_at, created_at, updated_at)
                    VALUES (:session_id, EMPTY_BLOB(), :expires_at, SYSTIMESTAMP, SYSTIMESTAMP)
                """
                await cursor.execute(merge_sql, {"session_id": key, "expires_at": expires_at})

                select_sql = f"SELECT data FROM {self._table_name} WHERE session_id = :session_id FOR UPDATE"
                await cursor.execute(select_sql, {"session_id": key})
                row = await cursor.fetchone()
                if row:
                    blob = row[0]
                    await blob.write(data)

                await conn.commit()
            else:
                sql = f"""
                MERGE INTO {self._table_name} t
                USING (SELECT :session_id AS session_id FROM DUAL) s
                ON (t.session_id = s.session_id)
                WHEN MATCHED THEN
                    UPDATE SET
                        data = :data,
                        expires_at = :expires_at,
                        updated_at = SYSTIMESTAMP
                WHEN NOT MATCHED THEN
                    INSERT (session_id, data, expires_at, created_at, updated_at)
                    VALUES (:session_id, :data, :expires_at, SYSTIMESTAMP, SYSTIMESTAMP)
                """
                await cursor.execute(sql, {"session_id": key, "data": data, "expires_at": expires_at})
                await conn.commit()

        if self._should_cleanup():
            await self.delete_expired()

    async def delete(self, key: str) -> None:
        """Delete a session by key.

        Args:
            key: Session ID to delete.
        """
        sql = f"DELETE FROM {self._table_name} WHERE session_id = :session_id"

        async with cast("AbstractAsyncContextManager[OracleAsyncConnection]", self._config.provide_connection()) as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, {"session_id": key})
            await conn.commit()

    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        sql = f"DELETE FROM {self._table_name}"

        async with cast("AbstractAsyncContextManager[OracleAsyncConnection]", self._config.provide_connection()) as conn:
            cursor = conn.cursor()
            await cursor.execute(sql)
            await conn.commit()
        logger.debug("Deleted all sessions from table: %s", self._table_name)

    async def exists(self, key: str) -> bool:
        """Check if a session key exists and is not expired.

        Args:
            key: Session ID to check.

        Returns:
            True if the session exists and is not expired.

        Notes:
            Uses SYSTIMESTAMP for consistency with get() method.
        """
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = :session_id
        AND (expires_at IS NULL OR expires_at > SYSTIMESTAMP)
        """

        async with cast("AbstractAsyncContextManager[OracleAsyncConnection]", self._config.provide_connection()) as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, {"session_id": key})
            result = await cursor.fetchone()
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
        WHERE session_id = :session_id
        """

        async with cast("AbstractAsyncContextManager[OracleAsyncConnection]", self._config.provide_connection()) as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, {"session_id": key})
            row = await cursor.fetchone()

            if row is None or row[0] is None:
                return None

            expires_at = row[0]

            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)

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
            Uses SYSTIMESTAMP for consistency.
            Oracle automatically commits DDL, so we explicitly commit for DML.
        """
        sql = f"DELETE FROM {self._table_name} WHERE expires_at <= SYSTIMESTAMP"

        async with cast("AbstractAsyncContextManager[OracleAsyncConnection]", self._config.provide_connection()) as conn:
            cursor = conn.cursor()
            await cursor.execute(sql)
            count = cursor.rowcount if cursor.rowcount is not None else 0
            await conn.commit()
            if count > 0:
                logger.debug("Cleaned up %d expired sessions", count)
            return count
