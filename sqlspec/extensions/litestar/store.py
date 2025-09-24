"""SQLSpec-based store implementation for Litestar integration."""

import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    from collections.abc import Callable

from litestar.stores.base import Store

from sqlspec import sql
from sqlspec.driver._async import AsyncDriverAdapterBase
from sqlspec.driver._sync import SyncDriverAdapterBase
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import import_string
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.sync_tools import ensure_async_, with_ensure_async_

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlspec.config import AsyncConfigT, DatabaseConfigProtocol, SyncConfigT

logger = get_logger("extensions.litestar.store")

__all__ = ("BaseStoreHandler", "SQLSpecSessionStore", "SQLSpecSessionStoreError")


class SQLSpecSessionStoreError(SQLSpecError):
    """Exception raised by session store operations."""


class BaseStoreHandler:
    """Base handler for adapter-specific session store operations.

    This provides default implementations that work for most databases.
    Adapters can override specific methods to customize behavior.
    """

    def serialize_data(self, data: Any, driver: Any = None) -> Any:
        """Serialize session data for storage.

        Args:
            data: Session data to serialize
            driver: Database driver instance (optional, used by specialized handlers)

        Returns:
            Serialized data ready for database storage
        """
        return to_json(data)

    def deserialize_data(self, data: Any, driver: Any = None) -> Any:
        """Deserialize session data from storage.

        Args:
            data: Raw data from database
            driver: Database driver instance (optional, used by specialized handlers)

        Returns:
            Deserialized session data, or None if JSON is invalid
        """
        if isinstance(data, str):
            try:
                return from_json(data)
            except Exception:
                logger.warning("Failed to deserialize JSON data")
                return None  # Filter out invalid JSON
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

        Returns:
            List of SQL statements to execute (check, update, insert pattern)
        """
        # Default: check-update-insert pattern for maximum compatibility
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

    def execute_operation(
        self, driver: Union[SyncDriverAdapterBase, AsyncDriverAdapterBase], operation: "Callable[[], Any]"
    ) -> Any:
        """Execute database operation handling async/sync drivers.

        Args:
            driver: Database driver
            operation: Operation to execute

        Returns:
            Operation result
        """
        if isinstance(driver, AsyncDriverAdapterBase):
            return operation()
        return ensure_async_(operation)()


class SQLSpecSessionStore(Store):
    """SQLSpec-based session store for Litestar.

    This store uses SQLSpec's builder API with adapter-specific handlers
    for clean, maintainable session management.
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
        self._handler = self._load_handler()

    def _load_handler(self) -> BaseStoreHandler:
        """Load adapter-specific store handler.

        Returns:
            Store handler for the configured adapter
        """
        # Extract adapter name from config module
        config_module = self._config.__class__.__module__

        # Expected pattern: sqlspec.adapters.{adapter_name}.config
        expected_module_parts = 3
        parts = config_module.split(".")
        if len(parts) >= expected_module_parts and parts[0] == "sqlspec" and parts[1] == "adapters":
            adapter_name = parts[2]
            handler_module = f"sqlspec.adapters.{adapter_name}.litestar.store"

            try:
                handler_class = import_string(f"{handler_module}.StoreHandler")
                logger.debug("Loaded store handler for adapter: %s", adapter_name)
                return handler_class(self._table_name, self._data_column)  # type: ignore[no-any-return]
            except ImportError:
                logger.debug("No custom store handler found for adapter: %s, using default", adapter_name)

        # Fallback to base handler
        return BaseStoreHandler()

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
        async with with_ensure_async_(self._config.provide_session()) as driver:
            return await self._get_session_data(driver, key, renew_for)

    async def _get_session_data(
        self,
        driver: Union[SyncDriverAdapterBase, AsyncDriverAdapterBase],
        key: str,
        renew_for: Union[int, timedelta, None],
    ) -> Any:
        """Internal method to get session data."""
        current_time = self._handler.get_current_time()

        select_sql = (
            sql.select(self._data_column)
            .from_(self._table_name)
            .where((sql.column(self._session_id_column) == key) & (sql.column(self._expires_at_column) > current_time))
        )

        try:
            result = await ensure_async_(driver.execute)(select_sql)

            if result.data:
                row = result.data[0]
                data = self._handler.handle_column_casing(row, self._data_column)

                # Handle Oracle AsyncLOB objects
                if hasattr(data, "read"):
                    import inspect

                    read_result = data.read()
                    if inspect.iscoroutine(read_result):
                        data = await read_result
                    else:
                        data = read_result

                data = await ensure_async_(self._handler.deserialize_data)(data, driver)

                # Handle session renewal
                if renew_for is not None:
                    renewal_delta = renew_for if isinstance(renew_for, timedelta) else timedelta(seconds=renew_for)
                    new_expires_at = datetime.now(timezone.utc) + renewal_delta
                    await self._update_expiration(driver, key, new_expires_at)

                return data

        except Exception:
            logger.exception("Failed to retrieve session %s", key)
        return None

    async def _update_expiration(
        self, driver: Union[SyncDriverAdapterBase, AsyncDriverAdapterBase], key: str, expires_at: datetime
    ) -> None:
        """Update the expiration time for a session."""
        expires_at_value = self._handler.format_datetime(expires_at)

        update_sql = (
            sql.update(self._table_name)
            .set(self._expires_at_column, expires_at_value)
            .where(sql.column(self._session_id_column) == key)
        )

        try:
            if isinstance(driver, AsyncDriverAdapterBase):
                await driver.execute(update_sql)
                await driver.commit()
            else:
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

        async with with_ensure_async_(self._config.provide_session()) as driver:
            await self._set_session_data(driver, key, value, expires_at)

    async def _set_session_data(
        self, driver: Union[SyncDriverAdapterBase, AsyncDriverAdapterBase], key: str, data: Any, expires_at: datetime
    ) -> None:
        """Internal method to set session data."""
        # Prepare values using handler
        data_value = await ensure_async_(self._handler.serialize_data)(data, driver)
        expires_at_value = self._handler.format_datetime(expires_at)
        current_time_value = self._handler.get_current_time()

        # Get SQL statements from handler
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
        )

        try:
            if len(sql_statements) == 1:
                # Single upsert statement
                if isinstance(driver, AsyncDriverAdapterBase):
                    await driver.execute(sql_statements[0])
                    await driver.commit()
                else:
                    await ensure_async_(driver.execute)(sql_statements[0])
                    await ensure_async_(driver.commit)()
            else:
                # Check-update-insert pattern
                check_sql, update_sql, insert_sql = sql_statements

                # Check if session exists
                if isinstance(driver, AsyncDriverAdapterBase):
                    result = await driver.execute(check_sql)
                else:
                    result = await ensure_async_(driver.execute)(check_sql)

                count = self._handler.handle_column_casing(result.data[0], "count")
                exists = count > 0

                # Execute appropriate statement
                if isinstance(driver, AsyncDriverAdapterBase):
                    if exists:
                        await driver.execute(update_sql)
                    else:
                        await driver.execute(insert_sql)
                    await driver.commit()
                else:
                    if exists:
                        await ensure_async_(driver.execute)(update_sql)
                    else:
                        await ensure_async_(driver.execute)(insert_sql)
                    await ensure_async_(driver.commit)()

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
        """Internal method to delete session data."""
        delete_sql = sql.delete().from_(self._table_name).where(sql.column(self._session_id_column) == key)

        try:
            if isinstance(driver, AsyncDriverAdapterBase):
                await driver.execute(delete_sql)
                await driver.commit()
            else:
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
        try:
            async with with_ensure_async_(self._config.provide_session()) as driver:
                current_time = self._handler.get_current_time()

                select_sql = (
                    sql.select(sql.count().as_("count"))
                    .from_(self._table_name)
                    .where(
                        (sql.column(self._session_id_column) == key)
                        & (sql.column(self._expires_at_column) > current_time)
                    )
                )

                if isinstance(driver, AsyncDriverAdapterBase):
                    result = await driver.execute(select_sql)
                else:
                    result = await ensure_async_(driver.execute)(select_sql)

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

        # Select both the expires_at time and check if the session exists and is not expired
        select_sql = (
            sql.select(sql.column(self._expires_at_column))
            .from_(self._table_name)
            .where(
                (sql.column(self._session_id_column) == key) & (sql.column(self._expires_at_column) > current_time_db)
            )
        )

        try:
            async with with_ensure_async_(self._config.provide_session()) as driver:
                if isinstance(driver, AsyncDriverAdapterBase):
                    result = await driver.execute(select_sql)
                else:
                    result = await ensure_async_(driver.execute)(select_sql)

            if not result.data:
                # Session doesn't exist or has already expired
                return 0

            row = result.data[0]
            expires_at = self._handler.handle_column_casing(row, self._expires_at_column)

            # Handle different datetime formats from different databases
            if isinstance(expires_at, str):
                # Parse ISO string format
                try:
                    expires_at = datetime.fromisoformat(expires_at)
                except ValueError:
                    # Try parsing common datetime formats
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

            # Ensure timezone awareness
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
        async with with_ensure_async_(self._config.provide_session()) as driver:
            await self._delete_all_sessions(driver)

    async def _delete_all_sessions(self, driver: Union[SyncDriverAdapterBase, AsyncDriverAdapterBase]) -> None:
        """Internal method to delete all sessions."""
        delete_sql = sql.delete().from_(self._table_name)

        try:
            if isinstance(driver, AsyncDriverAdapterBase):
                await driver.execute(delete_sql)
                await driver.commit()
            else:
                await ensure_async_(driver.execute)(delete_sql)
                await ensure_async_(driver.commit)()

        except Exception as e:
            msg = f"Failed to delete all sessions: {e}"
            logger.exception("Failed to delete all sessions")
            raise SQLSpecSessionStoreError(msg) from e

    async def delete_expired(self) -> None:
        """Delete expired sessions."""
        async with with_ensure_async_(self._config.provide_session()) as driver:
            current_time = self._handler.get_current_time()
            await self._delete_expired_sessions(driver, current_time)

    async def _delete_expired_sessions(
        self, driver: Union[SyncDriverAdapterBase, AsyncDriverAdapterBase], current_time: Union[str, datetime]
    ) -> None:
        """Internal method to delete expired sessions."""
        delete_sql = sql.delete().from_(self._table_name).where(sql.column(self._expires_at_column) <= current_time)

        try:
            if isinstance(driver, AsyncDriverAdapterBase):
                await driver.execute(delete_sql)
                await driver.commit()
            else:
                await ensure_async_(driver.execute)(delete_sql)
                await ensure_async_(driver.commit)()

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
        async with with_ensure_async_(self._config.provide_session()) as driver:
            current_time = self._handler.get_current_time()
            async for item in self._get_all_sessions(driver, current_time):
                yield item

    async def _get_all_sessions(
        self, driver: Union[SyncDriverAdapterBase, AsyncDriverAdapterBase], current_time: Union[str, datetime]
    ) -> "AsyncIterator[tuple[str, Any]]":
        """Internal method to get all sessions."""
        select_sql = (
            sql.select(sql.column(self._session_id_column), sql.column(self._data_column))
            .from_(self._table_name)
            .where(sql.column(self._expires_at_column) > current_time)
        )

        try:
            if isinstance(driver, AsyncDriverAdapterBase):
                result = await driver.execute(select_sql)
            else:
                result = await ensure_async_(driver.execute)(select_sql)

            for row in result.data:
                session_id = self._handler.handle_column_casing(row, self._session_id_column)
                session_data = self._handler.handle_column_casing(row, self._data_column)

                # Handle Oracle AsyncLOB objects
                if hasattr(session_data, "read"):
                    import inspect

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
