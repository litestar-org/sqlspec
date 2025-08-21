"""SQLSpec-based store implementation for Litestar integration."""

import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Union

from litestar.stores.base import Store

from sqlspec import sql
from sqlspec.core.statement import StatementConfig
from sqlspec.driver._async import AsyncDriverAdapterBase
from sqlspec.driver._sync import SyncDriverAdapterBase
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.sync_tools import ensure_async_, with_ensure_async_

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlspec.config import AsyncConfigT, DatabaseConfigProtocol, SyncConfigT

logger = get_logger("extensions.litestar.store")

__all__ = ("SessionStore", "SessionStoreError")


class SessionStoreError(SQLSpecError):
    """Exception raised by session store operations."""


class SessionStore(Store):
    """SQLSpec-based session store for Litestar.

    This store uses SQLSpec's builder API to create dialect-aware SQL operations
    for session management, including efficient upsert/merge operations.
    """

    __slots__ = (
        "_config",
        "_created_at_column",
        "_data_column",
        "_expires_at_column",
        "_session_id_column",
        "_table_created",
        "_table_name",
    )

    def __init__(
        self,
        config: Union["SyncConfigT", "AsyncConfigT", "DatabaseConfigProtocol"],
        *,
        table_name: str = "litestar_sessions",
        session_id_column: str = "session_id",
        data_column: str = "data",
        expires_at_column: str = "expires_at",
        created_at_column: str = "created_at",
    ) -> None:
        """Initialize the session store.

        Args:
            config: SQLSpec database configuration
            table_name: Name of the session table
            session_id_column: Name of the session ID column
            data_column: Name of the session data column
            expires_at_column: Name of the expires at column
            created_at_column: Name of the created at column
        """
        self._config = config
        self._table_name = table_name
        self._session_id_column = session_id_column
        self._data_column = data_column
        self._expires_at_column = expires_at_column
        self._created_at_column = created_at_column
        self._table_created = False

    async def _ensure_table_exists(self, driver: Union[SyncDriverAdapterBase, AsyncDriverAdapterBase]) -> None:
        """Ensure the session table exists with proper schema.

        Args:
            driver: Database driver instance
        """
        if self._table_created:
            return

        # Get the dialect for the driver
        dialect = getattr(driver, "statement_config", StatementConfig()).dialect or "generic"

        # Create table with appropriate types for the dialect
        if dialect in {"postgres", "postgresql"}:
            data_type = "JSONB"
            timestamp_type = "TIMESTAMP WITH TIME ZONE"
        elif dialect in {"mysql", "mariadb"}:
            data_type = "JSON"
            timestamp_type = "DATETIME"
        elif dialect == "sqlite":
            data_type = "TEXT"
            timestamp_type = "DATETIME"
        elif dialect == "oracle":
            data_type = "JSON"  # Use native Oracle JSON column (stores as RAW internally)
            timestamp_type = "TIMESTAMP"
        else:
            data_type = "TEXT"
            timestamp_type = "TIMESTAMP"

        create_table_sql = (
            sql.create_table(self._table_name)
            .if_not_exists()
            .column(self._session_id_column, "VARCHAR(255)", primary_key=True)
            .column(self._data_column, data_type, not_null=True)
            .column(self._expires_at_column, timestamp_type, not_null=True)
            .column(self._created_at_column, timestamp_type, not_null=True, default="CURRENT_TIMESTAMP")
        )

        try:
            await ensure_async_(driver.execute)(create_table_sql)

            # Create index on expires_at for efficient cleanup
            index_sql = sql.raw(
                f"CREATE INDEX IF NOT EXISTS idx_{self._table_name}_{self._expires_at_column} "
                f"ON {self._table_name} ({self._expires_at_column})"
            )

            await ensure_async_(driver.execute)(index_sql)

            self._table_created = True
            logger.debug("Session table %s created successfully", self._table_name)

        except Exception as e:
            msg = f"Failed to create session table: {e}"
            logger.exception("Failed to create session table %s", self._table_name)
            raise SessionStoreError(msg) from e

    def _get_dialect_upsert_sql(self, dialect: str, session_id: str, data: str, expires_at: datetime) -> Any:
        """Generate dialect-specific upsert SQL using SQL builder API.

        Args:
            dialect: Database dialect
            session_id: Session identifier
            data: JSON-encoded session data
            expires_at: Session expiration time

        Returns:
            SQL statement for upserting session data
        """
        current_time = datetime.now(timezone.utc)

        if dialect in {"postgres", "postgresql"}:
            # PostgreSQL UPSERT using ON CONFLICT
            return (
                sql.insert(self._table_name)
                .columns(self._session_id_column, self._data_column, self._expires_at_column, self._created_at_column)
                .values(session_id, data, expires_at, current_time)
                .on_conflict(self._session_id_column)
                .do_update(
                    **{
                        self._data_column: sql.raw("EXCLUDED." + self._data_column),
                        self._expires_at_column: sql.raw("EXCLUDED." + self._expires_at_column),
                    }
                )
            )

        if dialect in {"mysql", "mariadb"}:
            # MySQL UPSERT using ON DUPLICATE KEY UPDATE
            return (
                sql.insert(self._table_name)
                .columns(self._session_id_column, self._data_column, self._expires_at_column, self._created_at_column)
                .values(session_id, data, expires_at, current_time)
                .on_duplicate_key_update(
                    **{
                        self._data_column: sql.raw(f"VALUES({self._data_column})"),
                        self._expires_at_column: sql.raw(f"VALUES({self._expires_at_column})"),
                    }
                )
            )

        if dialect == "sqlite":
            # SQLite UPSERT using ON CONFLICT
            return (
                sql.insert(self._table_name)
                .columns(self._session_id_column, self._data_column, self._expires_at_column, self._created_at_column)
                .values(session_id, data, expires_at, current_time)
                .on_conflict(self._session_id_column)
                .do_update(
                    **{
                        self._data_column: sql.raw("EXCLUDED." + self._data_column),
                        self._expires_at_column: sql.raw("EXCLUDED." + self._expires_at_column),
                    }
                )
            )

        if dialect == "oracle":
            # Oracle MERGE statement with JSON column support
            return (
                sql.merge()
                .into(self._table_name, alias="t")
                .using(
                    sql.raw(
                        f"(SELECT ? as {self._session_id_column}, JSON(?) as {self._data_column}, ? as {self._expires_at_column}, ? as {self._created_at_column} FROM DUAL)",
                        parameters=[session_id, data, expires_at, current_time],
                    ),
                    alias="s",
                )
                .on(f"t.{self._session_id_column} = s.{self._session_id_column}")
                .when_matched_then_update(
                    set_values={
                        self._data_column: sql.raw(f"s.{self._data_column}"),
                        self._expires_at_column: sql.raw(f"s.{self._expires_at_column}"),
                    }
                )
                .when_not_matched_then_insert(
                    columns=[
                        self._session_id_column,
                        self._data_column,
                        self._expires_at_column,
                        self._created_at_column,
                    ],
                    values=[
                        sql.raw(f"s.{self._session_id_column}"),
                        sql.raw(f"s.{self._data_column}"),
                        sql.raw(f"s.{self._expires_at_column}"),
                        sql.raw(f"s.{self._created_at_column}"),
                    ],
                )
            )

        # Fallback: DELETE + INSERT (less efficient but works everywhere)
        delete_sql = sql.delete().from_(self._table_name).where(sql.column(self._session_id_column) == session_id)

        insert_sql = (
            sql.insert(self._table_name)
            .columns(self._session_id_column, self._data_column, self._expires_at_column, self._created_at_column)
            .values(session_id, data, expires_at, current_time)
        )

        return [delete_sql, insert_sql]

    async def get(self, key: str, renew_for: Union[int, timedelta, None] = None) -> Any:
        """Retrieve session data by session ID.

        Args:
            key: Session identifier
            renew_for: Time to renew the session for (seconds as int or timedelta)

        Returns:
            Session data or None if not found
        """
        async with with_ensure_async_(self._config.provide_session()) as driver:
            await self._ensure_table_exists(driver)
            return await self._get_session_data(driver, key, renew_for)

    async def _get_session_data(
        self,
        driver: Union[SyncDriverAdapterBase, AsyncDriverAdapterBase],
        key: str,
        renew_for: Union[int, timedelta, None],
    ) -> Any:
        """Internal method to get session data.

        Args:
            driver: Database driver
            key: Session identifier
            renew_for: Time to renew the session for (seconds as int or timedelta)

        Returns:
            Session data or None
        """
        current_time = datetime.now(timezone.utc)

        select_sql = (
            sql.select(self._data_column)
            .from_(self._table_name)
            .where((sql.column(self._session_id_column) == key) & (sql.column(self._expires_at_column) > current_time))
        )

        try:
            result = await ensure_async_(driver.execute)(select_sql)

            if result.data:
                data_json = result.data[0][self._data_column]
                data = from_json(data_json)

                # If renew_for is specified, update the expiration time
                if renew_for is not None:
                    renewal_delta = renew_for if isinstance(renew_for, timedelta) else timedelta(seconds=renew_for)
                    new_expires_at = datetime.now(timezone.utc) + renewal_delta
                    await self._update_expiration(driver, key, new_expires_at)

                return data

        except Exception:
            logger.exception("Failed to retrieve session %s", key)
            return None
        return None

    async def _update_expiration(
        self, driver: Union[SyncDriverAdapterBase, AsyncDriverAdapterBase], key: str, expires_at: datetime
    ) -> None:
        """Update the expiration time for a session.

        Args:
            driver: Database driver
            key: Session identifier
            expires_at: New expiration time
        """
        update_sql = (
            sql.update(self._table_name)
            .set(self._expires_at_column, expires_at)
            .where(sql.column(self._session_id_column) == key)
        )

        try:
            await ensure_async_(driver.execute)(update_sql)
        except Exception:
            logger.exception("Failed to update expiration for session %s", key)

    async def set(self, key: str, value: Any, expires_in: Union[int, timedelta, None] = None) -> None:
        """Store session data.

        Args:
            key: Session identifier
            value: Session data to store
            expires_in: Expiration time in seconds or timedelta (default: 24 hours)
        """
        if expires_in is None:
            expires_in = 24 * 60 * 60  # 24 hours default
        elif isinstance(expires_in, timedelta):
            expires_in = int(expires_in.total_seconds())

        expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
        data_json = to_json(value)

        async with with_ensure_async_(self._config.provide_session()) as driver:
            await self._ensure_table_exists(driver)
            await self._set_session_data(driver, key, data_json, expires_at)

    async def _set_session_data(
        self,
        driver: Union[SyncDriverAdapterBase, AsyncDriverAdapterBase],
        key: str,
        data_json: str,
        expires_at: datetime,
    ) -> None:
        """Internal method to set session data.

        Args:
            driver: Database driver
            key: Session identifier
            data_json: JSON-encoded session data
            expires_at: Expiration time
        """
        dialect = str(getattr(driver, "statement_config", StatementConfig()).dialect or "generic")
        upsert_sql = self._get_dialect_upsert_sql(dialect, key, data_json, expires_at)

        try:
            if isinstance(upsert_sql, list):
                # Fallback method: execute delete then insert
                for stmt in upsert_sql:
                    await ensure_async_(driver.execute)(stmt)
            else:
                await ensure_async_(driver.execute)(upsert_sql)

        except Exception as e:
            msg = f"Failed to store session: {e}"
            logger.exception("Failed to store session %s", key)
            raise SessionStoreError(msg) from e

    async def delete(self, key: str) -> None:
        """Delete session data.

        Args:
            key: Session identifier
        """
        async with with_ensure_async_(self._config.provide_session()) as driver:
            await self._ensure_table_exists(driver)
            await self._delete_session_data(driver, key)

    async def _delete_session_data(
        self, driver: Union[SyncDriverAdapterBase, AsyncDriverAdapterBase], key: str
    ) -> None:
        """Internal method to delete session data.

        Args:
            driver: Database driver
            key: Session identifier
        """
        delete_sql = sql.delete().from_(self._table_name).where(sql.column(self._session_id_column) == key)

        try:
            await ensure_async_(driver.execute)(delete_sql)

        except Exception as e:
            msg = f"Failed to delete session: {e}"
            logger.exception("Failed to delete session %s", key)
            raise SessionStoreError(msg) from e

    async def exists(self, key: str) -> bool:
        """Check if a session exists and is not expired.

        Args:
            key: Session identifier

        Returns:
            True if session exists and is not expired
        """
        current_time = datetime.now(timezone.utc)

        select_sql = (
            sql.select(sql.count().as_("count"))
            .from_(self._table_name)
            .where((sql.column(self._session_id_column) == key) & (sql.column(self._expires_at_column) > current_time))
        )

        try:
            async with with_ensure_async_(self._config.provide_session()) as driver:
                await self._ensure_table_exists(driver)
                result = await ensure_async_(driver.execute)(select_sql)

            return bool(result.data[0]["count"] > 0)

        except Exception:
            logger.exception("Failed to check if session %s exists", key)
            return False

    async def expires_in(self, key: str) -> int:
        """Get the number of seconds until the session expires.

        Args:
            key: Session identifier

        Returns:
            Number of seconds until expiration, or 0 if expired/not found
        """
        current_time = datetime.now(timezone.utc)

        select_sql = (
            sql.select(sql.column(self._expires_at_column))
            .from_(self._table_name)
            .where(sql.column(self._session_id_column) == key)
        )

        try:
            async with with_ensure_async_(self._config.provide_session()) as driver:
                await self._ensure_table_exists(driver)
                result = await ensure_async_(driver.execute)(select_sql)

            if result.data:
                expires_at_str = result.data[0][self._expires_at_column]
                # Parse the datetime string based on the format
                if isinstance(expires_at_str, str):
                    # Try different datetime formats
                    for fmt in ["%Y-%m-%d %H:%M:%S.%f%z", "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S"]:
                        try:
                            expires_at = datetime.strptime(expires_at_str, fmt)
                            if expires_at.tzinfo is None:
                                expires_at = expires_at.replace(tzinfo=timezone.utc)
                            break
                        except ValueError:
                            continue
                    else:
                        return 0
                elif isinstance(expires_at_str, datetime):
                    expires_at = expires_at_str
                    if expires_at.tzinfo is None:
                        expires_at = expires_at.replace(tzinfo=timezone.utc)
                else:
                    return 0

                delta = expires_at - current_time
                return max(0, int(delta.total_seconds()))

            return 0

        except Exception:
            logger.exception("Failed to get expires_in for session %s", key)
            return 0

    async def delete_all(self, pattern: str = "*") -> None:
        """Delete all sessions matching pattern.

        Args:
            pattern: Pattern to match session IDs (currently supports '*' for all)
        """
        async with with_ensure_async_(self._config.provide_session()) as driver:
            await self._ensure_table_exists(driver)
            await self._delete_all_sessions(driver)

    async def _delete_all_sessions(self, driver: Union[SyncDriverAdapterBase, AsyncDriverAdapterBase]) -> None:
        """Internal method to delete all sessions.

        Args:
            driver: Database driver
        """
        delete_sql = sql.delete().from_(self._table_name)

        try:
            await ensure_async_(driver.execute)(delete_sql)

        except Exception as e:
            msg = f"Failed to delete all sessions: {e}"
            logger.exception("Failed to delete all sessions")
            raise SessionStoreError(msg) from e

    async def delete_expired(self) -> None:
        """Delete expired sessions."""
        current_time = datetime.now(timezone.utc)

        async with with_ensure_async_(self._config.provide_session()) as driver:
            await self._ensure_table_exists(driver)
            await self._delete_expired_sessions(driver, current_time)

    async def _delete_expired_sessions(
        self, driver: Union[SyncDriverAdapterBase, AsyncDriverAdapterBase], current_time: datetime
    ) -> None:
        """Internal method to delete expired sessions.

        Args:
            driver: Database driver
            current_time: Current timestamp
        """
        delete_sql = sql.delete().from_(self._table_name).where(sql.column(self._expires_at_column) <= current_time)

        try:
            await ensure_async_(driver.execute)(delete_sql)

            logger.debug("Deleted expired sessions")

        except Exception:
            logger.exception("Failed to delete expired sessions")

    async def get_all(self, pattern: str = "*") -> "AsyncIterator[tuple[str, Any]]":
        """Get all sessions matching pattern.

        Args:
            pattern: Pattern to match session IDs

        Yields:
            Tuples of (session_id, session_data)
        """
        current_time = datetime.now(timezone.utc)

        async with with_ensure_async_(self._config.provide_session()) as driver:
            await self._ensure_table_exists(driver)
            async for item in self._get_all_sessions(driver, current_time):
                yield item

    async def _get_all_sessions(
        self, driver: Union[SyncDriverAdapterBase, AsyncDriverAdapterBase], current_time: datetime
    ) -> "AsyncIterator[tuple[str, Any]]":
        """Internal method to get all sessions.

        Args:
            driver: Database driver
            current_time: Current timestamp

        Yields:
            Tuples of (session_id, session_data)
        """
        select_sql = (
            sql.select(sql.column(self._session_id_column), sql.column(self._data_column))
            .from_(self._table_name)
            .where(sql.column(self._expires_at_column) > current_time)
        )

        try:
            result = await ensure_async_(driver.execute)(select_sql)

            for row in result.data:
                session_id = row[self._session_id_column]
                data_json = row[self._data_column]
                try:
                    session_data = from_json(data_json)
                    yield session_id, session_data
                except Exception as e:
                    logger.warning("Failed to decode session data for %s: %s", session_id, e)
                    continue

        except Exception:
            logger.exception("Failed to get all sessions")

    @staticmethod
    def generate_session_id() -> str:
        """Generate a new session ID.

        Returns:
            Random session identifier
        """
        return str(uuid.uuid4())
