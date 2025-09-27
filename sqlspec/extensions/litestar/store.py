"""SQLSpec-based store implementation for Litestar integration.

Clean break implementation with separate async/sync stores.
No backwards compatibility with the mixed implementation.
"""

import inspect
import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Union

import anyio
from litestar.stores.base import Store

from sqlspec import sql
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import import_string
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from sqlspec.config import AsyncConfigT, SyncConfigT
    from sqlspec.driver._async import AsyncDriverAdapterBase
    from sqlspec.driver._sync import SyncDriverAdapterBase

logger = get_logger("extensions.litestar.store")

__all__ = ("SQLSpecAsyncSessionStore", "SQLSpecSessionStoreError", "SQLSpecSyncSessionStore")


class SQLSpecSessionStoreError(SQLSpecError):
    """Exception raised by session store operations."""


class SQLSpecAsyncSessionStore(Store):
    """SQLSpec-based session store for async database configurations.

    This store is optimized for async drivers and provides direct async calls
    without any sync/async wrapping overhead.

    Use this store with async database configurations only.
    """

    __slots__ = (
        "_config",
        "_created_at_column",
        "_data_column",
        "_expires_at_column",
        "_handler",
        "_session_id_column",
        "_table_name",
    )

    def __init__(
        self,
        config: "AsyncConfigT",
        *,
        table_name: str = "litestar_sessions",
        session_id_column: str = "session_id",
        data_column: str = "data",
        expires_at_column: str = "expires_at",
        created_at_column: str = "created_at",
    ) -> None:
        """Initialize the async session store.

        Args:
            config: SQLSpec async database configuration
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
        self._handler = self._load_handler()

    def _load_handler(self) -> Any:
        """Load adapter-specific store handler.

        Returns:
            Store handler for the configured adapter
        """
        config_module = self._config.__class__.__module__

        parts = config_module.split(".")
        expected_module_parts = 3
        if len(parts) >= expected_module_parts and parts[0] == "sqlspec" and parts[1] == "adapters":
            adapter_name = parts[2]
            handler_module = f"sqlspec.adapters.{adapter_name}.litestar.store"

            try:
                handler_class = import_string(f"{handler_module}.AsyncStoreHandler")
                logger.debug("Loaded async store handler for adapter: %s", adapter_name)
                return handler_class(self._table_name, self._data_column)
            except ImportError:
                logger.debug("No custom async store handler found for adapter: %s, using default", adapter_name)

        return _DefaultStoreHandler()

    @property
    def table_name(self) -> str:
        """Get the table name."""
        return self._table_name

    @property
    def session_id_column(self) -> str:
        """Get the session ID column name."""
        return self._session_id_column

    @property
    def data_column(self) -> str:
        """Get the data column name."""
        return self._data_column

    @property
    def expires_at_column(self) -> str:
        """Get the expires at column name."""
        return self._expires_at_column

    @property
    def created_at_column(self) -> str:
        """Get the created at column name."""
        return self._created_at_column

    async def get(self, key: str, renew_for: Union[int, timedelta, None] = None) -> Any:
        """Retrieve session data by session ID.

        Args:
            key: Session identifier
            renew_for: Time to renew the session for (seconds as int or timedelta)

        Returns:
            Session data or None if not found
        """
        async with self._config.provide_session() as driver:
            return await self._get_session_data(driver, key, renew_for)

    async def _get_session_data(
        self, driver: "AsyncDriverAdapterBase", key: str, renew_for: Union[int, timedelta, None]
    ) -> Any:
        """Internal method to get session data."""
        current_time = self._handler.get_current_time()

        select_sql = (
            sql.select(self._data_column)
            .from_(self._table_name)
            .where((sql.column(self._session_id_column) == key) & (sql.column(self._expires_at_column) > current_time))
        )

        try:
            result = await driver.execute(select_sql)

            if result.data:
                row = result.data[0]
                data = self._handler.handle_column_casing(row, self._data_column)

                if hasattr(data, "read"):
                    read_result = data.read()
                    if inspect.iscoroutine(read_result):
                        data = await read_result
                    else:
                        data = read_result

                if hasattr(self._handler.deserialize_data, "__await__"):
                    data = await self._handler.deserialize_data(data, driver)
                else:
                    data = self._handler.deserialize_data(data, driver)

                if renew_for is not None:
                    renewal_delta = renew_for if isinstance(renew_for, timedelta) else timedelta(seconds=renew_for)
                    new_expires_at = datetime.now(timezone.utc) + renewal_delta
                    await self._update_expiration(driver, key, new_expires_at)

                return data

        except Exception:
            logger.exception("Failed to retrieve session %s", key)
        return None

    async def _update_expiration(self, driver: "AsyncDriverAdapterBase", key: str, expires_at: datetime) -> None:
        """Update the expiration time for a session."""
        expires_at_value = self._handler.format_datetime(expires_at)

        update_sql = (
            sql.update(self._table_name)
            .set(self._expires_at_column, expires_at_value)
            .where(sql.column(self._session_id_column) == key)
        )

        try:
            await driver.execute(update_sql)
            await driver.commit()
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
            expires_in = 24 * 60 * 60
        elif isinstance(expires_in, timedelta):
            expires_in = int(expires_in.total_seconds())

        async with self._config.provide_session() as driver:
            await self._set_session_data(driver, key, value, datetime.now(timezone.utc) + timedelta(seconds=expires_in))

    async def _set_session_data(
        self, driver: "AsyncDriverAdapterBase", key: str, data: Any, expires_at: datetime
    ) -> None:
        """Internal method to set session data."""
        if hasattr(self._handler.serialize_data, "__await__"):
            data_value = await self._handler.serialize_data(data, driver)
        else:
            data_value = self._handler.serialize_data(data, driver)

        expires_at_value = self._handler.format_datetime(expires_at)
        current_time_value = self._handler.get_current_time()

        sql_statements = self._handler.build_upsert_sql(
            self._table_name,
            self._session_id_column,
            self._data_column,
            self._expires_at_column,
            self._created_at_column,
            key,
            data_value,
            expires_at_value,
            current_time_value,
            driver,
        )

        try:
            if len(sql_statements) == 1:
                await driver.execute(sql_statements[0])
                await driver.commit()
            else:
                check_sql, update_sql, insert_sql = sql_statements

                result = await driver.execute(check_sql)
                count = self._handler.handle_column_casing(result.data[0], "count")
                exists = count > 0

                if exists:
                    await driver.execute(update_sql)
                else:
                    await driver.execute(insert_sql)
                await driver.commit()
        except Exception as e:
            msg = f"Failed to store session: {e}"
            logger.exception("Failed to store session %s", key)
            raise SQLSpecSessionStoreError(msg) from e

    async def delete(self, key: str) -> None:
        """Delete session data.

        Args:
            key: Session identifier
        """
        async with self._config.provide_session() as driver:
            await self._delete_session_data(driver, key)

    async def _delete_session_data(self, driver: "AsyncDriverAdapterBase", key: str) -> None:
        """Internal method to delete session data."""
        delete_sql = sql.delete().from_(self._table_name).where(sql.column(self._session_id_column) == key)

        try:
            await driver.execute(delete_sql)
            await driver.commit()
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
        try:
            async with self._config.provide_session() as driver:
                current_time = self._handler.get_current_time()

                select_sql = (
                    sql.select(sql.count().as_("count"))
                    .from_(self._table_name)
                    .where(
                        (sql.column(self._session_id_column) == key)
                        & (sql.column(self._expires_at_column) > current_time)
                    )
                )

                result = await driver.execute(select_sql)
                count = self._handler.handle_column_casing(result.data[0], "count")
                return bool(count > 0)
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
        current_time_db = self._handler.get_current_time()

        select_sql = (
            sql.select(sql.column(self._expires_at_column))
            .from_(self._table_name)
            .where(
                (sql.column(self._session_id_column) == key) & (sql.column(self._expires_at_column) > current_time_db)
            )
        )

        try:
            async with self._config.provide_session() as driver:
                result = await driver.execute(select_sql)

            if not result.data:
                return 0

            row = result.data[0]
            expires_at = self._handler.handle_column_casing(row, self._expires_at_column)

            if isinstance(expires_at, str):
                try:
                    expires_at = datetime.fromisoformat(expires_at)
                except ValueError:
                    for fmt in [
                        "%Y-%m-%d %H:%M:%S.%f",
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%dT%H:%M:%S.%f",
                        "%Y-%m-%dT%H:%M:%S",
                        "%Y-%m-%dT%H:%M:%S.%fZ",
                        "%Y-%m-%dT%H:%M:%SZ",
                    ]:
                        try:
                            expires_at = datetime.strptime(expires_at, fmt).replace(tzinfo=timezone.utc)
                            break
                        except ValueError:
                            continue
                    else:
                        logger.warning("Failed to parse expires_at datetime: %s", expires_at)
                        return 0

            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)

            delta = expires_at - current_time
            return max(0, int(delta.total_seconds()))

        except Exception:
            logger.exception("Failed to get expires_in for session %s", key)
        return 0

    async def delete_all(self, pattern: str = "*") -> None:
        """Delete all sessions matching pattern.

        Args:
            pattern: Pattern to match session IDs (currently supports '*' for all)
        """
        async with self._config.provide_session() as driver:
            await self._delete_all_sessions(driver)

    async def _delete_all_sessions(self, driver: "AsyncDriverAdapterBase") -> None:
        """Internal method to delete all sessions."""
        delete_sql = sql.delete().from_(self._table_name)

        try:
            await driver.execute(delete_sql)
            await driver.commit()
        except Exception as e:
            msg = f"Failed to delete all sessions: {e}"
            logger.exception("Failed to delete all sessions")
            raise SQLSpecSessionStoreError(msg) from e

    async def delete_expired(self) -> None:
        """Delete expired sessions."""
        async with self._config.provide_session() as driver:
            current_time = self._handler.get_current_time()
            await self._delete_expired_sessions(driver, current_time)

    async def _delete_expired_sessions(
        self, driver: "AsyncDriverAdapterBase", current_time: Union[str, datetime]
    ) -> None:
        """Internal method to delete expired sessions."""
        delete_sql = sql.delete().from_(self._table_name).where(sql.column(self._expires_at_column) <= current_time)

        try:
            await driver.execute(delete_sql)
            await driver.commit()
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
        async with self._config.provide_session() as driver:
            current_time = self._handler.get_current_time()
            async for item in self._get_all_sessions(driver, current_time):
                yield item

    async def _get_all_sessions(
        self, driver: "AsyncDriverAdapterBase", current_time: Union[str, datetime]
    ) -> "AsyncIterator[tuple[str, Any]]":
        select_sql = (
            sql.select(sql.column(self._session_id_column), sql.column(self._data_column))
            .from_(self._table_name)
            .where(sql.column(self._expires_at_column) > current_time)
        )

        try:
            result = await driver.execute(select_sql)

            for row in result.data:
                session_id = self._handler.handle_column_casing(row, self._session_id_column)
                session_data = self._handler.handle_column_casing(row, self._data_column)

                if hasattr(session_data, "read"):
                    read_result = session_data.read()
                    if inspect.iscoroutine(read_result):
                        session_data = await read_result
                    else:
                        session_data = read_result

                session_data = self._handler.deserialize_data(session_data)
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


class SQLSpecSyncSessionStore(Store):
    """SQLSpec-based session store for sync database configurations.

    This store uses sync drivers internally and wraps them with anyio
    for Litestar's async Store interface compatibility.

    Use this store with sync database configurations only.
    """

    __slots__ = (
        "_config",
        "_created_at_column",
        "_data_column",
        "_expires_at_column",
        "_handler",
        "_session_id_column",
        "_table_name",
    )

    def __init__(
        self,
        config: "SyncConfigT",
        *,
        table_name: str = "litestar_sessions",
        session_id_column: str = "session_id",
        data_column: str = "data",
        expires_at_column: str = "expires_at",
        created_at_column: str = "created_at",
    ) -> None:
        """Initialize the sync session store.

        Args:
            config: SQLSpec sync database configuration
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
        self._handler = self._load_handler()

    def _load_handler(self) -> Any:
        """Load adapter-specific store handler."""
        config_module = self._config.__class__.__module__

        parts = config_module.split(".")
        expected_module_parts = 3
        if len(parts) >= expected_module_parts and parts[0] == "sqlspec" and parts[1] == "adapters":
            adapter_name = parts[2]
            handler_module = f"sqlspec.adapters.{adapter_name}.litestar.store"

            try:
                handler_class = import_string(f"{handler_module}.SyncStoreHandler")
                logger.debug("Loaded sync store handler for adapter: %s", adapter_name)
                return handler_class(self._table_name, self._data_column)
            except ImportError:
                logger.debug("No custom sync store handler found for adapter: %s, using default", adapter_name)

        return _DefaultStoreHandler()

    @property
    def table_name(self) -> str:
        """Get the table name."""
        return self._table_name

    @property
    def session_id_column(self) -> str:
        """Get the session ID column name."""
        return self._session_id_column

    @property
    def data_column(self) -> str:
        """Get the data column name."""
        return self._data_column

    @property
    def expires_at_column(self) -> str:
        """Get the expires at column name."""
        return self._expires_at_column

    @property
    def created_at_column(self) -> str:
        """Get the created at column name."""
        return self._created_at_column

    def _get_sync(self, key: str, renew_for: Union[int, timedelta, None]) -> Any:
        """Sync implementation of get."""
        with self._config.provide_session() as driver:
            return self._get_session_data_sync(driver, key, renew_for)

    def _get_session_data_sync(
        self, driver: "SyncDriverAdapterBase", key: str, renew_for: Union[int, timedelta, None]
    ) -> Any:
        """Internal sync method to get session data."""
        current_time = self._handler.get_current_time()

        select_sql = (
            sql.select(self._data_column)
            .from_(self._table_name)
            .where((sql.column(self._session_id_column) == key) & (sql.column(self._expires_at_column) > current_time))
        )

        try:
            result = driver.execute(select_sql)

            if result.data:
                row = result.data[0]
                data = self._handler.handle_column_casing(row, self._data_column)

                if hasattr(data, "read"):
                    data = data.read()

                data = self._handler.deserialize_data(data, driver)

                if renew_for is not None:
                    renewal_delta = renew_for if isinstance(renew_for, timedelta) else timedelta(seconds=renew_for)
                    new_expires_at = datetime.now(timezone.utc) + renewal_delta
                    self._update_expiration_sync(driver, key, new_expires_at)

                return data

        except Exception:
            logger.exception("Failed to retrieve session %s", key)
        return None

    def _update_expiration_sync(self, driver: "SyncDriverAdapterBase", key: str, expires_at: datetime) -> None:
        """Sync method to update expiration time."""
        expires_at_value = self._handler.format_datetime(expires_at)

        update_sql = (
            sql.update(self._table_name)
            .set(self._expires_at_column, expires_at_value)
            .where(sql.column(self._session_id_column) == key)
        )

        try:
            driver.execute(update_sql)
            driver.commit()
        except Exception:
            logger.exception("Failed to update expiration for session %s", key)

    async def get(self, key: str, renew_for: Union[int, timedelta, None] = None) -> Any:
        """Retrieve session data by session ID.

        Args:
            key: Session identifier
            renew_for: Time to renew the session for (seconds as int or timedelta)

        Returns:
            Session data or None if not found
        """
        return await anyio.to_thread.run_sync(self._get_sync, key, renew_for)

    def _set_sync(self, key: str, value: Any, expires_in: Union[int, timedelta, None]) -> None:
        """Sync implementation of set."""
        if expires_in is None:
            expires_in = 24 * 60 * 60
        elif isinstance(expires_in, timedelta):
            expires_in = int(expires_in.total_seconds())

        expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

        with self._config.provide_session() as driver:
            self._set_session_data_sync(driver, key, value, expires_at)

    def _set_session_data_sync(
        self, driver: "SyncDriverAdapterBase", key: str, data: Any, expires_at: datetime
    ) -> None:
        """Internal sync method to set session data."""
        data_value = self._handler.serialize_data(data, driver)
        expires_at_value = self._handler.format_datetime(expires_at)
        current_time_value = self._handler.get_current_time()

        sql_statements = self._handler.build_upsert_sql(
            self._table_name,
            self._session_id_column,
            self._data_column,
            self._expires_at_column,
            self._created_at_column,
            key,
            data_value,
            expires_at_value,
            current_time_value,
            driver,
        )

        try:
            if len(sql_statements) == 1:
                driver.execute(sql_statements[0])
                driver.commit()
            else:
                check_sql, update_sql, insert_sql = sql_statements

                result = driver.execute(check_sql)
                count = self._handler.handle_column_casing(result.data[0], "count")
                exists = count > 0

                if exists:
                    driver.execute(update_sql)
                else:
                    driver.execute(insert_sql)
                driver.commit()
        except Exception as e:
            msg = f"Failed to store session: {e}"
            logger.exception("Failed to store session %s", key)
            raise SQLSpecSessionStoreError(msg) from e

    async def set(self, key: str, value: Any, expires_in: Union[int, timedelta, None] = None) -> None:
        """Store session data.

        Args:
            key: Session identifier
            value: Session data to store
            expires_in: Expiration time in seconds or timedelta (default: 24 hours)
        """
        await anyio.to_thread.run_sync(self._set_sync, key, value, expires_in)

    def _delete_sync(self, key: str) -> None:
        """Sync implementation of delete."""
        with self._config.provide_session() as driver:
            self._delete_session_data_sync(driver, key)

    def _delete_session_data_sync(self, driver: "SyncDriverAdapterBase", key: str) -> None:
        """Internal sync method to delete session data."""
        delete_sql = sql.delete().from_(self._table_name).where(sql.column(self._session_id_column) == key)

        try:
            driver.execute(delete_sql)
            driver.commit()
        except Exception as e:
            msg = f"Failed to delete session: {e}"
            logger.exception("Failed to delete session %s", key)
            raise SQLSpecSessionStoreError(msg) from e

    async def delete(self, key: str) -> None:
        """Delete session data.

        Args:
            key: Session identifier
        """
        await anyio.to_thread.run_sync(self._delete_sync, key)

    def _exists_sync(self, key: str) -> bool:
        """Sync implementation of exists."""
        try:
            with self._config.provide_session() as driver:
                current_time = self._handler.get_current_time()

                select_sql = (
                    sql.select(sql.count().as_("count"))
                    .from_(self._table_name)
                    .where(
                        (sql.column(self._session_id_column) == key)
                        & (sql.column(self._expires_at_column) > current_time)
                    )
                )

                result = driver.execute(select_sql)
                count = self._handler.handle_column_casing(result.data[0], "count")
                return bool(count > 0)
        except Exception:
            logger.exception("Failed to check if session %s exists", key)
            return False

    async def exists(self, key: str) -> bool:
        """Check if a session exists and is not expired.

        Args:
            key: Session identifier

        Returns:
            True if session exists and is not expired
        """
        return await anyio.to_thread.run_sync(self._exists_sync, key)

    def _delete_expired_sync(self) -> None:
        """Sync implementation of delete_expired."""
        with self._config.provide_session() as driver:
            current_time = self._handler.get_current_time()
            delete_sql = sql.delete().from_(self._table_name).where(sql.column(self._expires_at_column) <= current_time)

            try:
                driver.execute(delete_sql)
                driver.commit()
                logger.debug("Deleted expired sessions")
            except Exception:
                logger.exception("Failed to delete expired sessions")

    async def delete_expired(self) -> None:
        """Delete expired sessions."""
        await anyio.to_thread.run_sync(self._delete_expired_sync)

    def _delete_all_sync(self, pattern: str = "*") -> None:
        """Sync implementation of delete_all."""
        with self._config.provide_session() as driver:
            if pattern == "*":
                delete_sql = sql.delete().from_(self._table_name)
            else:
                delete_sql = (
                    sql.delete().from_(self._table_name).where(sql.column(self._session_id_column).like(pattern))
                )
            try:
                driver.execute(delete_sql)
                driver.commit()
                logger.debug("Deleted sessions matching pattern: %s", pattern)
            except Exception:
                logger.exception("Failed to delete sessions matching pattern: %s", pattern)

    async def delete_all(self, pattern: str = "*") -> None:
        """Delete all sessions matching pattern.

        Args:
            pattern: Pattern to match session IDs (currently supports '*' for all)
        """
        await anyio.to_thread.run_sync(self._delete_all_sync, pattern)

    def _expires_in_sync(self, key: str) -> int:
        """Sync implementation of expires_in."""
        current_time = datetime.now(timezone.utc)
        current_time_db = self._handler.get_current_time()

        select_sql = (
            sql.select(sql.column(self._expires_at_column))
            .from_(self._table_name)
            .where(
                (sql.column(self._session_id_column) == key) & (sql.column(self._expires_at_column) > current_time_db)
            )
        )

        try:
            with self._config.provide_session() as driver:
                result = driver.execute(select_sql)

            if not result.data:
                return 0

            row = result.data[0]
            expires_at = self._handler.handle_column_casing(row, self._expires_at_column)

            if isinstance(expires_at, str):
                try:
                    expires_at = datetime.fromisoformat(expires_at)
                except ValueError:
                    for fmt in [
                        "%Y-%m-%d %H:%M:%S.%f",
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%dT%H:%M:%S.%f",
                        "%Y-%m-%dT%H:%M:%S",
                        "%Y-%m-%dT%H:%M:%S.%fZ",
                        "%Y-%m-%dT%H:%M:%SZ",
                    ]:
                        try:
                            expires_at = datetime.strptime(expires_at, fmt).replace(tzinfo=timezone.utc)
                            break
                        except ValueError:
                            continue
                    else:
                        logger.warning("Invalid datetime format for session %s: %s", key, expires_at)
                        return 0

            if isinstance(expires_at, datetime):
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=timezone.utc)

                delta = expires_at - current_time
                return max(0, int(delta.total_seconds()))

        except Exception:
            logger.exception("Failed to get expires_in for session %s", key)
        return 0

    async def expires_in(self, key: str) -> int:
        """Get the number of seconds until the session expires.

        Args:
            key: Session identifier

        Returns:
            Number of seconds until expiration, or 0 if expired/not found
        """
        return await anyio.to_thread.run_sync(self._expires_in_sync, key)

    def _get_all_sync(self, pattern: str = "*") -> "Iterator[tuple[str, Any]]":
        """Sync implementation of get_all."""
        from sqlspec import sql

        current_time_db = self._handler.get_current_time()
        select_sql = (
            sql.select(sql.column(self._session_id_column), sql.column(self._data_column))
            .from_(self._table_name)
            .where(sql.column(self._expires_at_column) > current_time_db)
        )

        with self._config.provide_session() as driver:
            result = driver.execute(select_sql)

            for row in result.data:
                session_id = self._handler.handle_column_casing(row, self._session_id_column)
                data = self._handler.handle_column_casing(row, self._data_column)

                try:
                    deserialized_data = self._handler.deserialize_data(data, driver)
                    if deserialized_data is not None:
                        yield session_id, deserialized_data
                except Exception:
                    logger.warning("Failed to deserialize session data for %s", session_id)

    async def get_all(self, pattern: str = "*") -> "AsyncIterator[tuple[str, Any]]":
        """Get all sessions and their data.

        Args:
            pattern: Pattern to filter keys (not supported yet)

        Yields:
            Tuples of (session_id, session_data) for non-expired sessions
        """
        for session_id, data in await anyio.to_thread.run_sync(lambda: list(self._get_all_sync(pattern))):
            yield session_id, data

    @staticmethod
    def generate_session_id() -> str:
        """Generate a new session ID.

        Returns:
            Random session identifier
        """
        return uuid.uuid4().hex


class _DefaultStoreHandler:
    """Default store handler for adapters without custom handlers.

    This provides basic implementations that work with most databases.
    """

    def serialize_data(self, data: Any, driver: Any = None) -> Any:
        """Serialize session data for storage.

        Args:
            data: Session data to serialize
            driver: Database driver instance (optional)

        Returns:
            Serialized data ready for database storage
        """
        return to_json(data)

    def deserialize_data(self, data: Any, driver: Any = None) -> Any:
        """Deserialize session data from storage.

        Args:
            data: Raw data from database
            driver: Database driver instance (optional)

        Returns:
            Deserialized session data, or None if JSON is invalid
        """
        if isinstance(data, str):
            try:
                return from_json(data)
            except Exception:
                logger.warning("Failed to deserialize JSON data")
                return None
        return data

    def format_datetime(self, dt: datetime) -> Union[str, datetime, Any]:
        """Format datetime for database storage.

        Args:
            dt: Datetime to format

        Returns:
            Formatted datetime value
        """
        return dt

    def get_current_time(self) -> Union[str, datetime, Any]:
        """Get current time in database-appropriate format.

        Returns:
            Current timestamp for database queries
        """
        return datetime.now(timezone.utc)

    def build_upsert_sql(
        self,
        table_name: str,
        session_id_column: str,
        data_column: str,
        expires_at_column: str,
        created_at_column: str,
        session_id: str,
        data_value: Any,
        expires_at_value: Any,
        current_time_value: Any,
        driver: Any = None,
    ) -> "list[Any]":
        """Build SQL statements for upserting session data.

        Args:
            table_name: Name of session table
            session_id_column: Session ID column name
            data_column: Data column name
            expires_at_column: Expires at column name
            created_at_column: Created at column name
            session_id: Session identifier
            data_value: Serialized session data
            expires_at_value: Formatted expiration time
            current_time_value: Formatted current time
            driver: Database driver instance (optional)

        Returns:
            List of SQL statements to execute (check, update, insert pattern)
        """
        check_exists = (
            sql.select(sql.count().as_("count")).from_(table_name).where(sql.column(session_id_column) == session_id)
        )

        update_sql = (
            sql.update(table_name)
            .set(data_column, data_value)
            .set(expires_at_column, expires_at_value)
            .where(sql.column(session_id_column) == session_id)
        )

        insert_sql = (
            sql.insert(table_name)
            .columns(session_id_column, data_column, expires_at_column, created_at_column)
            .values(session_id, data_value, expires_at_value, current_time_value)
        )

        return [check_exists, update_sql, insert_sql]

    def handle_column_casing(self, row: "dict[str, Any]", column: str) -> Any:
        """Handle database-specific column name casing.

        Args:
            row: Result row from database
            column: Column name to look up

        Returns:
            Column value handling different name casing
        """
        if column in row:
            return row[column]
        if column.upper() in row:
            return row[column.upper()]
        if column.lower() in row:
            return row[column.lower()]
        msg = f"Column {column} not found in result row"
        raise KeyError(msg)
