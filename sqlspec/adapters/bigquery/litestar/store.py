"""BigQuery session store for Litestar integration."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.adapters.bigquery.config import BigQueryConfig


__all__ = ("BigQueryStore",)


class BigQueryStore(BaseSQLSpecStore["BigQueryConfig"]):
    """BigQuery session store using synchronous BigQuery driver.

    Implements server-side session storage for Litestar using Google BigQuery.
    Uses Litestar's sync_to_thread utility to provide an async interface
    compatible with the Store protocol.

    Provides efficient session management with:
    - Sync operations wrapped for async compatibility
    - MERGE for UPSERT functionality
    - Native TIMESTAMP type support
    - Automatic expiration handling
    - Efficient cleanup of expired sessions
    - Table clustering on session_id for optimized lookups

    Note:
        BigQuery is designed for analytical (OLAP) workloads and scales to petabytes.
        For typical session store workloads, clustering by session_id provides good
        performance. Consider partitioning by created_at if session volume exceeds
        millions of rows per day.

    Args:
        config: BigQueryConfig instance.

    Example:
        from sqlspec.adapters.bigquery import BigQueryConfig
        from sqlspec.adapters.bigquery.litestar.store import BigQueryStore

        config = BigQueryConfig(connection_config={"project": "my-project"})
        store = BigQueryStore(config)
        await store.create_table()
    """

    __slots__ = ()

    def __init__(self, config: "BigQueryConfig") -> None:
        """Initialize BigQuery session store.

        Args:
            config: BigQueryConfig instance.

        Notes:
            Table name is read from config.extension_config["litestar"]["session_table"].
        """
        super().__init__(config)

    def _get_create_table_sql(self) -> str:
        """Get BigQuery CREATE TABLE SQL with optimized schema.

        Returns:
            SQL statement to create the sessions table with clustering.

        Notes:
            - Uses TIMESTAMP for timezone-aware expiration timestamps
            - BYTES for binary session data storage
            - Clustered by session_id for efficient lookups
            - No indexes needed - BigQuery uses columnar storage
            - Table name is internally controlled, not user input
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            session_id STRING NOT NULL,
            data BYTES NOT NULL,
            expires_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        CLUSTER BY session_id
        """

    def _get_drop_table_sql(self) -> "list[str]":
        """Get BigQuery DROP TABLE SQL statements.

        Returns:
            List containing DROP TABLE statement.

        Notes:
            BigQuery doesn't have separate indexes to drop.
        """
        return [f"DROP TABLE IF EXISTS {self._table_name}"]

    def _datetime_to_timestamp(self, dt: "datetime | None") -> "datetime | None":
        """Convert datetime to BigQuery TIMESTAMP.

        Args:
            dt: Datetime to convert (must be UTC-aware).

        Returns:
            UTC datetime object, or None if dt is None.

        Notes:
            BigQuery TIMESTAMP type expects UTC datetime objects.
            The BigQuery client library handles the conversion.
        """
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt

    def _timestamp_to_datetime(self, ts: "datetime | None") -> "datetime | None":
        """Convert TIMESTAMP back to datetime.

        Args:
            ts: Datetime object from BigQuery.

        Returns:
            UTC-aware datetime, or None if ts is None.
        """
        if ts is None:
            return None
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts

    def _create_table(self) -> None:
        """Synchronous implementation of create_table."""
        sql = self._get_create_table_sql()
        with self._config.provide_session() as driver:
            driver.execute_script(sql)
        self._log_table_created()

    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        await async_(self._create_table)()

    def _get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Synchronous implementation of get."""
        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = @session_id
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP())
        """

        with self._config.provide_session() as driver:
            result = driver.select_one(sql, {"session_id": key})

            if result is None:
                return None

            data = result.get("data")
            expires_at = result.get("expires_at")

            if renew_for is not None and expires_at is not None:
                new_expires_at = self._calculate_expires_at(renew_for)
                new_expires_at_ts = self._datetime_to_timestamp(new_expires_at)
                if new_expires_at_ts is not None:
                    update_sql = f"""
                    UPDATE {self._table_name}
                    SET expires_at = @expires_at
                    WHERE session_id = @session_id
                    """
                    driver.execute(update_sql, {"expires_at": new_expires_at_ts, "session_id": key})

            return bytes(data) if data is not None else None

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
            Uses MERGE for UPSERT functionality in BigQuery.
            BigQuery requires source data to come from a table or inline VALUES.
        """
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        expires_at_ts = self._datetime_to_timestamp(expires_at)

        sql = f"""
        MERGE {self._table_name} AS target
        USING (SELECT @session_id AS session_id, @data AS data, @expires_at AS expires_at) AS source
        ON target.session_id = source.session_id
        WHEN MATCHED THEN
            UPDATE SET data = source.data, expires_at = source.expires_at
        WHEN NOT MATCHED THEN
            INSERT (session_id, data, expires_at, created_at)
            VALUES (source.session_id, source.data, source.expires_at, CURRENT_TIMESTAMP())
        """

        with self._config.provide_session() as driver:
            driver.execute(sql, {"session_id": key, "data": data, "expires_at": expires_at_ts})

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
        sql = f"DELETE FROM {self._table_name} WHERE session_id = @session_id"

        with self._config.provide_session() as driver:
            driver.execute(sql, {"session_id": key})

    async def delete(self, key: str) -> None:
        """Delete a session by key.

        Args:
            key: Session ID to delete.
        """
        await async_(self._delete)(key)

    def _delete_all(self) -> None:
        """Synchronous implementation of delete_all."""
        sql = f"DELETE FROM {self._table_name} WHERE TRUE"

        with self._config.provide_session() as driver:
            driver.execute(sql)
        self._log_delete_all()

    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        await async_(self._delete_all)()

    def _exists(self, key: str) -> bool:
        """Synchronous implementation of exists."""
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = @session_id
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP())
        LIMIT 1
        """

        with self._config.provide_session() as driver:
            result = driver.select_one(sql, {"session_id": key})
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
        WHERE session_id = @session_id
        """

        with self._config.provide_session() as driver:
            result = driver.select_one(sql, {"session_id": key})

            if result is None:
                return None

            expires_at = result.get("expires_at")
            if expires_at is None:
                return None

            expires_at_dt = self._timestamp_to_datetime(expires_at)
            if expires_at_dt is None:
                return None

            now = datetime.now(timezone.utc)
            if expires_at_dt <= now:
                return 0

            delta = expires_at_dt - now
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
        sql = f"DELETE FROM {self._table_name} WHERE expires_at <= CURRENT_TIMESTAMP()"

        with self._config.provide_session() as driver:
            result = driver.execute(sql)
            count = result.get_affected_count()
            if count > 0:
                self._log_delete_expired(count)
            return count

    async def delete_expired(self) -> int:
        """Delete all expired sessions.

        Returns:
            Number of sessions deleted.
        """
        return await async_(self._delete_expired)()
