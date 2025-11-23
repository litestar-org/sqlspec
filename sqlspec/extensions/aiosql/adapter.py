"""AioSQL adapter implementation for SQLSpec.

This module provides adapter classes that implement the aiosql adapter protocols
while using SQLSpec drivers under the hood. This enables users to load SQL queries
from files using aiosql while using SQLSpec's features for execution and type mapping.
"""

import logging
from collections.abc import Generator
from contextlib import AbstractAsyncContextManager, contextmanager
from typing import Any, ClassVar, Generic, TypeVar

from sqlspec.core import SQL, SQLResult, StatementConfig
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.typing import AiosqlAsyncProtocol, AiosqlParamType, AiosqlSQLOperationType, AiosqlSyncProtocol
from sqlspec.utils.module_loader import ensure_aiosql

logger = logging.getLogger("sqlspec.extensions.aiosql")

__all__ = ("AiosqlAsyncAdapter", "AiosqlSyncAdapter")

T = TypeVar("T")
DriverT = TypeVar("DriverT", bound="SyncDriverAdapterBase | AsyncDriverAdapterBase")


class AsyncCursorLike:
    def __init__(self, result: Any) -> None:
        self.result = result

    async def fetchall(self) -> list[Any]:
        if isinstance(self.result, SQLResult) and self.result.data is not None:
            return list(self.result.data)
        return []

    async def fetchone(self) -> Any | None:
        rows = await self.fetchall()
        return rows[0] if rows else None


class CursorLike:
    def __init__(self, result: Any) -> None:
        self.result = result

    def fetchall(self) -> list[Any]:
        if isinstance(self.result, SQLResult) and self.result.data is not None:
            return list(self.result.data)
        return []

    def fetchone(self) -> Any | None:
        rows = self.fetchall()
        return rows[0] if rows else None


def _normalize_dialect(dialect: "str | Any | None") -> str:
    """Normalize dialect name for SQLGlot compatibility.

    Args:
        dialect: Original dialect name (can be str, Dialect, type[Dialect], or None)

    Returns:
        Converted dialect name compatible with SQLGlot
    """
    if dialect is None:
        return "sql"

    if isinstance(dialect, str):
        dialect_str = dialect.lower()
    elif hasattr(dialect, "__name__"):
        dialect_str = str(dialect.__name__).lower()
    elif hasattr(dialect, "name"):
        dialect_str = str(dialect.name).lower()
    else:
        dialect_str = str(dialect).lower()

    dialect_mapping = {
        "postgresql": "postgres",
        "psycopg": "postgres",
        "asyncpg": "postgres",
        "psqlpy": "postgres",
        "sqlite3": "sqlite",
        "aiosqlite": "sqlite",
    }
    return dialect_mapping.get(dialect_str, dialect_str)


class _AiosqlAdapterBase(Generic[DriverT]):
    """Base adapter class providing common functionality for aiosql integration."""

    def __init__(self, driver: DriverT) -> None:
        """Initialize the base adapter.

        Args:
            driver: SQLSpec driver to use for execution.
        """
        ensure_aiosql()
        self.driver: DriverT = driver

    def process_sql(self, query_name: str, op_type: "AiosqlSQLOperationType", sql: str) -> str:
        """Process SQL string for aiosql compatibility.

        Args:
            query_name: Name of the query
            op_type: Operation type
            sql: SQL string to process

        Returns:
            Processed SQL string
        """
        return sql

    def _create_sql_object(self, sql: str, parameters: "AiosqlParamType" = None) -> SQL:
        """Create SQL object with proper configuration.

        Args:
            sql: SQL string
            parameters: Query parameters

        Returns:
            Configured SQL object
        """
        return SQL(
            sql,
            parameters,
            config=StatementConfig(enable_validation=False),
            dialect=_normalize_dialect(getattr(self.driver, "dialect", "sqlite")),
        )


class AiosqlSyncAdapter(_AiosqlAdapterBase[SyncDriverAdapterBase], AiosqlSyncProtocol):
    """Synchronous adapter that implements aiosql protocol using SQLSpec drivers.

    This adapter bridges aiosql's synchronous driver protocol with SQLSpec's sync drivers,
    enabling queries loaded by aiosql to be executed with SQLSpec drivers.
    """

    is_aio_driver: ClassVar[bool] = False

    def __init__(self, driver: "SyncDriverAdapterBase") -> None:
        """Initialize the sync adapter.

        Args:
            driver: SQLSpec sync driver to use for execution
        """
        super().__init__(driver)

    def select(
        self, conn: Any, query_name: str, sql: str, parameters: "AiosqlParamType", record_class: Any | None = None
    ) -> Generator[Any, None, None]:
        """Execute a SELECT query and return results as generator.

        Args:
            conn: Database connection (passed through to SQLSpec driver)
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters
            record_class: Deprecated - use schema_type in driver.execute instead

        Yields:
            Query result rows

        Note:
            The record_class parameter is ignored for compatibility. Use schema_type
            in driver.execute or _sqlspec_schema_type in parameters for type mapping.
        """
        if record_class is not None:
            logger.warning(
                "record_class parameter is deprecated and ignored. "
                "Use schema_type in driver.execute or _sqlspec_schema_type in parameters."
            )

        result = self.driver.execute(self._create_sql_object(sql, parameters), connection=conn)

        if isinstance(result, SQLResult) and result.data is not None:
            yield from result.data

    def select_one(
        self, conn: Any, query_name: str, sql: str, parameters: "AiosqlParamType", record_class: Any | None = None
    ) -> tuple[Any, ...] | None:
        """Execute a SELECT query and return first result.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters
            record_class: Deprecated - use schema_type in driver.execute instead

        Returns:
            First result row or None

        Note:
            The record_class parameter is ignored for compatibility. Use schema_type
            in driver.execute or _sqlspec_schema_type in parameters for type mapping.
        """
        if record_class is not None:
            logger.warning(
                "record_class parameter is deprecated and ignored. "
                "Use schema_type in driver.execute or _sqlspec_schema_type in parameters."
            )

        result = self.driver.execute(self._create_sql_object(sql, parameters), connection=conn)

        if hasattr(result, "data") and result.data and isinstance(result, SQLResult):
            row = result.data[0]
            return tuple(row.values()) if isinstance(row, dict) else row
        return None

    def select_value(self, conn: Any, query_name: str, sql: str, parameters: "AiosqlParamType") -> Any | None:
        """Execute a SELECT query and return first value of first row.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters

        Returns:
            First value of first row or None
        """
        row = self.select_one(conn, query_name, sql, parameters)
        if row is None:
            return None

        if isinstance(row, dict):
            return next(iter(row.values())) if row else None
        if hasattr(row, "__getitem__"):
            return row[0] if len(row) > 0 else None
        return row

    @contextmanager
    def select_cursor(
        self, conn: Any, query_name: str, sql: str, parameters: "AiosqlParamType"
    ) -> Generator[Any, None, None]:
        """Execute a SELECT query and return cursor context manager.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters

        Yields:
            Cursor-like object with results
        """
        result = self.driver.execute(self._create_sql_object(sql, parameters), connection=conn)

        yield CursorLike(result)

    def insert_update_delete(self, conn: Any, query_name: str, sql: str, parameters: "AiosqlParamType") -> int:
        """Execute INSERT/UPDATE/DELETE and return affected rows.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters

        Returns:
            Number of affected rows
        """
        result = self.driver.execute(self._create_sql_object(sql, parameters), connection=conn)

        return result.rows_affected if hasattr(result, "rows_affected") else 0

    def insert_update_delete_many(self, conn: Any, query_name: str, sql: str, parameters: "AiosqlParamType") -> int:
        """Execute INSERT/UPDATE/DELETE with many parameter sets.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Sequence of parameter sets

        Returns:
            Number of affected rows
        """
        result = self.driver.execute(self._create_sql_object(sql, parameters), connection=conn)

        return result.rows_affected if hasattr(result, "rows_affected") else 0

    def insert_returning(self, conn: Any, query_name: str, sql: str, parameters: "AiosqlParamType") -> Any | None:
        """Execute INSERT with RETURNING and return result.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters

        Returns:
            Returned value or None
        """
        return self.select_one(conn, query_name, sql, parameters)


class _AsyncCursorContextManager(Generic[T]):
    def __init__(self, cursor_like: T) -> None:
        self._cursor_like = cursor_like

    async def __aenter__(self) -> T:
        return self._cursor_like

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass


class AiosqlAsyncAdapter(_AiosqlAdapterBase[AsyncDriverAdapterBase], AiosqlAsyncProtocol):
    """Asynchronous adapter that implements aiosql protocol using SQLSpec drivers.

    This adapter bridges aiosql's async driver protocol with SQLSpec's async drivers,
    enabling queries loaded by aiosql to be executed with SQLSpec async drivers.
    """

    is_aio_driver: ClassVar[bool] = True

    def __init__(self, driver: "AsyncDriverAdapterBase") -> None:
        """Initialize the async adapter.

        Args:
            driver: SQLSpec async driver to use for execution
        """
        super().__init__(driver)

    async def select(
        self, conn: Any, query_name: str, sql: str, parameters: "AiosqlParamType", record_class: Any | None = None
    ) -> list[Any]:
        """Execute a SELECT query and return results as list (protocol-compatible).

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters
            record_class: Deprecated - use schema_type in driver.execute instead

        Returns:
            List of query result rows

        Note:
            The record_class parameter is ignored for compatibility. Use schema_type
            in driver.execute or _sqlspec_schema_type in parameters for type mapping.
        """
        if record_class is not None:
            logger.warning(
                "record_class parameter is deprecated and ignored. "
                "Use schema_type in driver.execute or _sqlspec_schema_type in parameters."
            )

        result = await self.driver.execute(self._create_sql_object(sql, parameters), connection=conn)

        if hasattr(result, "data") and result.data is not None and isinstance(result, SQLResult):
            return list(result.data)
        return []

    async def select_one(
        self, conn: Any, query_name: str, sql: str, parameters: "AiosqlParamType", record_class: Any | None = None
    ) -> Any | None:
        """Execute a SELECT query and return first result.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters
            record_class: Deprecated - use schema_type in driver.execute instead

        Returns:
            First result row or None

        Note:
            The record_class parameter is ignored for compatibility. Use schema_type
            in driver.execute or _sqlspec_schema_type in parameters for type mapping.
        """
        if record_class is not None:
            logger.warning(
                "record_class parameter is deprecated and ignored. "
                "Use schema_type in driver.execute or _sqlspec_schema_type in parameters."
            )

        result = await self.driver.execute(self._create_sql_object(sql, parameters), connection=conn)

        if hasattr(result, "data") and result.data and isinstance(result, SQLResult):
            row = result.data[0]
            return tuple(row.values()) if isinstance(row, dict) else row
        return None

    async def select_value(self, conn: Any, query_name: str, sql: str, parameters: "AiosqlParamType") -> Any | None:
        """Execute a SELECT query and return first value of first row.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters

        Returns:
            First value of first row or None
        """
        row = await self.select_one(conn, query_name, sql, parameters)
        if row is None:
            return None

        if isinstance(row, dict):
            return next(iter(row.values())) if row else None
        if hasattr(row, "__getitem__"):
            return row[0] if len(row) > 0 else None
        return row

    async def select_cursor(
        self, conn: Any, query_name: str, sql: str, parameters: "AiosqlParamType"
    ) -> AbstractAsyncContextManager[Any]:
        """Execute a SELECT query and return cursor context manager.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters

        Yields:
            Cursor-like object with results
        """
        result = await self.driver.execute(self._create_sql_object(sql, parameters), connection=conn)
        return _AsyncCursorContextManager(AsyncCursorLike(result))

    async def insert_update_delete(self, conn: Any, query_name: str, sql: str, parameters: "AiosqlParamType") -> None:
        """Execute INSERT/UPDATE/DELETE.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters

        Note:
            Returns None per aiosql async protocol
        """
        await self.driver.execute(self._create_sql_object(sql, parameters), connection=conn)

    async def insert_update_delete_many(
        self, conn: Any, query_name: str, sql: str, parameters: "AiosqlParamType"
    ) -> None:
        """Execute INSERT/UPDATE/DELETE with many parameter sets.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Sequence of parameter sets

        Note:
            Returns None per aiosql async protocol
        """
        await self.driver.execute(self._create_sql_object(sql, parameters), connection=conn)

    async def insert_returning(self, conn: Any, query_name: str, sql: str, parameters: "AiosqlParamType") -> Any | None:
        """Execute INSERT with RETURNING and return result.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters

        Returns:
            Returned value or None
        """
        return await self.select_one(conn, query_name, sql, parameters)
