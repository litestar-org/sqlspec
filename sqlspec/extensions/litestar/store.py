"""SQLSpec-based store implementation for Litestar integration."""

import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Optional, Union

from litestar.stores.base import Store

from sqlspec import sql
from sqlspec.driver._async import AsyncDriverAdapterBase
from sqlspec.driver._sync import SyncDriverAdapterBase
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json
from sqlspec.utils.sync_tools import ensure_async_, with_ensure_async_

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlspec.config import AsyncConfigT, DatabaseConfigProtocol, SyncConfigT

logger = get_logger("extensions.litestar.store")

__all__ = ("SQLSpecSessionStore", "SQLSpecSessionStoreError")


class SQLSpecSessionStoreError(SQLSpecError):
    """Exception raised by session store operations."""


class SQLSpecSessionStore(Store):
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
        "_table_name",
    )

    def __init__(
        self,
        config: Union["SyncConfigT", "AsyncConfigT", "DatabaseConfigProtocol[Any, Any, Any]"],
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

    def _get_dialect_from_config(self) -> str:
        """Get database dialect from configuration without entering async context.

        Returns:
            Database dialect string
        """
        # Try to get dialect from config module name
        config_module = self._config.__class__.__module__.lower()

        if (
            "postgres" in config_module
            or "asyncpg" in config_module
            or "psycopg" in config_module
            or "psqlpy" in config_module
        ):
            return "postgres"
        if "mysql" in config_module or "asyncmy" in config_module:
            return "mysql"
        if "sqlite" in config_module or "aiosqlite" in config_module:
            return "sqlite"
        if "oracle" in config_module:
            return "oracle"
        if "duckdb" in config_module:
            return "duckdb"
        if "bigquery" in config_module:
            return "bigquery"
        try:
            stmt_config = self._config.statement_config
            if stmt_config and stmt_config.dialect:
                return str(stmt_config.dialect)
        except Exception:
            logger.debug("Failed to determine dialect from statement config", exc_info=True)
        return "generic"

    def _get_set_sql(self, dialect: str, session_id: str, data: Any, expires_at: datetime) -> list[Any]:
        """Generate SQL for setting session data (check, then update or insert).

        Args:
            dialect: Database dialect
            session_id: Session identifier
            data: Session data (adapter will handle JSON serialization via type_coercion_map)
            expires_at: Session expiration time

        Returns:
            List of SQL statements: [check_exists, update, insert]
        """
        current_time = datetime.now(timezone.utc)

        # For SQLite, convert datetimes to ISO format strings
        if dialect == "sqlite":
            expires_at_value: Union[str, datetime] = expires_at.isoformat()
            current_time_value: Union[str, datetime] = current_time.isoformat()
        elif dialect == "oracle":
            # Oracle needs special datetime handling - remove timezone info and use raw datetime
            expires_at_value = expires_at.replace(tzinfo=None)
            current_time_value = current_time.replace(tzinfo=None)
        else:
            expires_at_value = expires_at
            current_time_value = current_time

        # For databases that support native upsert, use those features
        if dialect in {"postgres", "postgresql"}:
            return [
                (
                    sql.insert(self._table_name)
                    .columns(
                        self._session_id_column, self._data_column, self._expires_at_column, self._created_at_column
                    )
                    .values(session_id, data, expires_at_value, current_time_value)
                    .on_conflict(self._session_id_column)
                    .do_update(
                        **{
                            self._data_column: sql.raw("EXCLUDED." + self._data_column),
                            self._expires_at_column: sql.raw("EXCLUDED." + self._expires_at_column),
                        }
                    )
                )
            ]

        if dialect in {"mysql", "mariadb"}:
            # MySQL UPSERT using ON DUPLICATE KEY UPDATE
            return [
                (
                    sql.insert(self._table_name)
                    .columns(
                        self._session_id_column, self._data_column, self._expires_at_column, self._created_at_column
                    )
                    .values(session_id, data, expires_at_value, current_time_value)
                    .on_duplicate_key_update(
                        **{
                            self._data_column: sql.raw(f"VALUES({self._data_column})"),
                            self._expires_at_column: sql.raw(f"VALUES({self._expires_at_column})"),
                        }
                    )
                )
            ]

        if dialect == "sqlite":
            # SQLite UPSERT using ON CONFLICT
            return [
                (
                    sql.insert(self._table_name)
                    .columns(
                        self._session_id_column, self._data_column, self._expires_at_column, self._created_at_column
                    )
                    .values(session_id, data, expires_at_value, current_time_value)
                    .on_conflict(self._session_id_column)
                    .do_update(
                        **{
                            self._data_column: sql.raw("EXCLUDED." + self._data_column),
                            self._expires_at_column: sql.raw("EXCLUDED." + self._expires_at_column),
                        }
                    )
                )
            ]

        if dialect == "oracle":
            # Oracle MERGE statement implementation using SQL builder
            merge_builder = (
                sql.merge(self._table_name)
                .using(
                    {
                        self._session_id_column: session_id,
                        self._data_column: data,
                        self._expires_at_column: expires_at_value,
                        self._created_at_column: current_time_value,
                    },
                    alias="s",
                )
                .on(f"t.{self._session_id_column} = s.{self._session_id_column}")
                .when_matched_then_update(
                    {
                        self._data_column: f"s.{self._data_column}",
                        self._expires_at_column: f"s.{self._expires_at_column}",
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
                        f"s.{self._session_id_column}",
                        f"s.{self._data_column}",
                        f"s.{self._expires_at_column}",
                        f"s.{self._created_at_column}",
                    ],
                )
            )

            return [merge_builder.to_statement()]

        # For other databases, use check-update-insert pattern
        check_exists = (
            sql.select(sql.count().as_("count"))
            .from_(self._table_name)
            .where(sql.column(self._session_id_column) == session_id)
        )

        update_sql = (
            sql.update(self._table_name)
            .set(self._data_column, data)
            .set(self._expires_at_column, expires_at_value)
            .where(sql.column(self._session_id_column) == session_id)
        )

        insert_sql = (
            sql.insert(self._table_name)
            .columns(self._session_id_column, self._data_column, self._expires_at_column, self._created_at_column)
            .values(session_id, data, expires_at_value, current_time_value)
        )

        return [check_exists, update_sql, insert_sql]

    async def get(self, key: str, renew_for: Union[int, timedelta, None] = None) -> Any:
        """Retrieve session data by session ID.

        Args:
            key: Session identifier
            renew_for: Time to renew the session for (seconds as int or timedelta)

        Returns:
            Session data or None if not found
        """
        async with with_ensure_async_(self._config.provide_session()) as driver:
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
        select_sql = (
            sql.select(self._data_column)
            .from_(self._table_name)
            .where(
                (sql.column(self._session_id_column) == key)
                & (sql.column(self._expires_at_column) > datetime.now(timezone.utc))
            )
        )

        try:
            result = await ensure_async_(driver.execute)(select_sql)

            if result.data:
                data = result.data[0][self._data_column]

                # For SQLite and DuckDB, data is stored as JSON text and needs to be deserialized
                dialect = str(driver.statement_config.dialect or "generic") if hasattr(driver, 'statement_config') and driver.statement_config else "generic"
                if dialect in {"sqlite", "duckdb"} and isinstance(data, str):
                    try:
                        data = from_json(data)
                    except Exception:
                        logger.warning("Failed to deserialize JSON data for session %s", key)
                        # Return the raw data if JSON parsing fails
                        pass

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
            await ensure_async_(driver.commit)()
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

        # Get dialect before entering async context to avoid event loop issues
        dialect = self._get_dialect_from_config()

        async with with_ensure_async_(self._config.provide_session()) as driver:
            await self._set_session_data(driver, key, value, expires_at, dialect)

    async def _set_session_data(
        self,
        driver: Union[SyncDriverAdapterBase, AsyncDriverAdapterBase],
        key: str,
        data: Any,
        expires_at: datetime,
        dialect: Optional[str] = None,
    ) -> None:
        """Internal method to set session data.

        Args:
            driver: Database driver
            key: Session identifier
            data: Session data
            expires_at: Expiration time
            dialect: Optional dialect override (to avoid accessing driver in event loop)
        """
        if dialect is None:
            dialect = str(driver.statement_config.dialect or "generic")
        sql_statements = self._get_set_sql(dialect, key, data, expires_at)

        try:
            # For databases with native upsert, there's only one statement
            if len(sql_statements) == 1:
                await ensure_async_(driver.execute)(sql_statements[0])

                await ensure_async_(driver.commit)()
            else:
                # For other databases: check-update-insert pattern
                check_sql, update_sql, insert_sql = sql_statements

                # Check if session exists
                result = await ensure_async_(driver.execute)(check_sql)
                # Oracle returns uppercase column names by default
                count_key = "COUNT" if dialect == "oracle" else "count"
                exists = result.data[0][count_key] > 0 if result.data else False

                # Execute appropriate statement
                if exists:
                    await ensure_async_(driver.execute)(update_sql)
                else:
                    await ensure_async_(driver.execute)(insert_sql)

        except Exception as e:
            msg = f"Failed to store session: {e}"
            logger.exception("Failed to store session %s", key)
            raise SQLSpecSessionStoreError(msg) from e

    async def delete(self, key: str) -> None:
        """Delete session data.

        Args:
            key: Session identifier
        """
        async with with_ensure_async_(self._config.provide_session()) as driver:
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

            await ensure_async_(driver.commit)()

        except Exception as e:
            msg = f"Failed to delete session: {e}"
            logger.exception("Failed to delete session %s", key)
            raise SQLSpecSessionStoreError(msg) from e

    async def exists(self, key: str) -> bool:
        """Check if a session exists and is not expired.

        Args:
            key: Session identifier

        Returns:
            True if session exists and is not expired
        """
        current_time = datetime.now(timezone.utc)

        async with with_ensure_async_(self._config.provide_session()) as driver:
            select_sql = (
                sql.select(sql.count().as_("count"))
                .from_(self._table_name)
                .where(
                    (sql.column(self._session_id_column) == key) & (sql.column(self._expires_at_column) > current_time)
                )
            )

            try:
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
                result = await ensure_async_(driver.execute)(select_sql)

            if result.data:
                expires_at = result.data[0][self._expires_at_column]
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=timezone.utc)

                delta = expires_at - current_time
                return max(0, int(delta.total_seconds()))

        except Exception:
            logger.exception("Failed to get expires_in for session %s", key)
        return 0

    async def delete_all(self, _pattern: str = "*") -> None:
        """Delete all sessions matching pattern.

        Args:
            _pattern: Pattern to match session IDs (currently supports '*' for all)
        """
        async with with_ensure_async_(self._config.provide_session()) as driver:
            await self._delete_all_sessions(driver)

    async def _delete_all_sessions(self, driver: Union[SyncDriverAdapterBase, AsyncDriverAdapterBase]) -> None:
        """Internal method to delete all sessions.

        Args:
            driver: Database driver
        """
        delete_sql = sql.delete().from_(self._table_name)

        try:
            await ensure_async_(driver.execute)(delete_sql)

            await ensure_async_(driver.commit)()

        except Exception as e:
            msg = f"Failed to delete all sessions: {e}"
            logger.exception("Failed to delete all sessions")
            raise SQLSpecSessionStoreError(msg) from e

    async def delete_expired(self) -> None:
        """Delete expired sessions."""
        current_time = datetime.now(timezone.utc)

        async with with_ensure_async_(self._config.provide_session()) as driver:
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

            await ensure_async_(driver.commit)()

            logger.debug("Deleted expired sessions")

        except Exception:
            logger.exception("Failed to delete expired sessions")

    async def get_all(self, _pattern: str = "*") -> "AsyncIterator[tuple[str, Any]]":
        """Get all sessions matching pattern.

        Args:
            _pattern: Pattern to match session IDs

        Yields:
            Tuples of (session_id, session_data)
        """
        current_time = datetime.now(timezone.utc)

        async with with_ensure_async_(self._config.provide_session()) as driver:
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

            # Check if we need to deserialize JSON for SQLite
            dialect = str(driver.statement_config.dialect or "generic") if hasattr(driver, 'statement_config') and driver.statement_config else "generic"
            
            for row in result.data:
                session_id = row[self._session_id_column]
                session_data = row[self._data_column]
                
                # For SQLite and DuckDB, data is stored as JSON text and needs to be deserialized
                if dialect in {"sqlite", "duckdb"} and isinstance(session_data, str):
                    try:
                        session_data = from_json(session_data)
                    except Exception:
                        logger.warning("Failed to deserialize JSON data for session %s", session_id)
                        # Return the raw data if JSON parsing fails
                        pass
                        
                yield session_id, session_data

        except Exception:
            logger.exception("Failed to get all sessions")

    @staticmethod
    def generate_session_id() -> str:
        """Generate a new session ID.

        Returns:
            Random session identifier
        """
        return str(uuid.uuid4())
