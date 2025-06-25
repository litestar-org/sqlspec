"""AioSQL adapter implementation for SQLSpec.

This module provides adapter classes that implement the aiosql adapter protocols
while using SQLSpec drivers under the hood. This enables users to load SQL queries
from files using aiosql while leveraging all of SQLSpec's advanced features.
"""

import logging
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypeVar, Union, cast

from sqlspec.exceptions import MissingDependencyError
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import AIOSQL_INSTALLED, RowT

if TYPE_CHECKING:
    from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol

logger = logging.getLogger("sqlspec.extensions.aiosql")

__all__ = ("AiosqlAsyncAdapter", "AiosqlSyncAdapter")

T = TypeVar("T")


def _check_aiosql_available() -> None:
    if not AIOSQL_INSTALLED:
        msg = "aiosql"
        raise MissingDependencyError(msg, "aiosql")


def _normalize_dialect(dialect: "Union[str, Any, None]") -> str:
    """Normalize dialect name for SQLGlot compatibility.

    Args:
        dialect: Original dialect name (can be str, Dialect, type[Dialect], or None)

    Returns:
        Normalized dialect name
    """
    # Handle different dialect types
    if dialect is None:
        return "sql"

    # Extract string from dialect class or instance
    if hasattr(dialect, "__name__"):  # It's a class
        dialect_str = str(dialect.__name__).lower()  # pyright: ignore
    elif hasattr(dialect, "name"):  # It's an instance with name attribute
        dialect_str = str(dialect.name).lower()  # pyright: ignore
    elif isinstance(dialect, str):
        dialect_str = dialect.lower()
    else:
        dialect_str = str(dialect).lower()

    # Map common dialect aliases to SQLGlot names
    dialect_mapping = {
        "postgresql": "postgres",
        "psycopg": "postgres",
        "asyncpg": "postgres",
        "psqlpy": "postgres",
        "sqlite3": "sqlite",
        "aiosqlite": "sqlite",
    }
    return dialect_mapping.get(dialect_str, dialect_str)


class _AiosqlAdapterBase:
    """Base adapter for common logic."""

    def __init__(
        self, driver: "Union[SyncDriverAdapterProtocol[Any, Any], AsyncDriverAdapterProtocol[Any, Any]]"
    ) -> None:
        """Initialize the base adapter.

        Args:
            driver: SQLSpec driver to use for execution.
        """
        _check_aiosql_available()
        self.driver = driver

    def process_sql(self, query_name: str, op_type: "Any", sql: str) -> str:
        """Process SQL for aiosql compatibility."""
        return sql

    def _create_sql_object(self, sql: str, parameters: "Any" = None) -> SQL:
        """Create SQL object with proper configuration."""
        config = SQLConfig(strict_mode=False, enable_validation=False)
        normalized_dialect = _normalize_dialect(self.driver.dialect)
        return SQL(sql, parameters, config=config, dialect=normalized_dialect)


class AiosqlSyncAdapter(_AiosqlAdapterBase):
    """Sync adapter that implements aiosql protocol using SQLSpec drivers.

    This adapter bridges aiosql's sync driver protocol with SQLSpec's sync drivers,
    enabling all of SQLSpec's drivers to work with queries loaded by aiosql.

    """

    is_aio_driver: ClassVar[bool] = False

    def __init__(self, driver: "SyncDriverAdapterProtocol[Any, Any]") -> None:
        """Initialize the sync adapter.

        Args:
            driver: SQLSpec sync driver to use for execution
        """
        super().__init__(driver)

    def select(
        self, conn: Any, query_name: str, sql: str, parameters: "Any", record_class: Optional[Any] = None
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
            record_class parameter is ignored. Use schema_type in driver.execute
            or _sqlspec_schema_type in parameters for type mapping.
        """
        if record_class is not None:
            logger.warning(
                "record_class parameter is deprecated and ignored. "
                "Use schema_type in driver.execute or _sqlspec_schema_type in parameters."
            )

        # Create SQL object and apply filters
        sql_obj = self._create_sql_object(sql, parameters)
        # Execute using SQLSpec driver
        result = self.driver.execute(sql_obj, connection=conn)

        if isinstance(result, SQLResult) and result.data is not None:
            yield from result.data

    def select_one(
        self, conn: Any, query_name: str, sql: str, parameters: "Any", record_class: Optional[Any] = None
    ) -> Optional[RowT]:
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
            record_class parameter is ignored. Use schema_type in driver.execute
            or _sqlspec_schema_type in parameters for type mapping.
        """
        if record_class is not None:
            logger.warning(
                "record_class parameter is deprecated and ignored. "
                "Use schema_type in driver.execute or _sqlspec_schema_type in parameters."
            )

        sql_obj = self._create_sql_object(sql, parameters)

        result = cast("SQLResult[RowT]", self.driver.execute(sql_obj, connection=conn))

        if hasattr(result, "data") and result.data and isinstance(result, SQLResult):
            return cast("Optional[RowT]", result.data[0])
        return None

    def select_value(self, conn: Any, query_name: str, sql: str, parameters: "Any") -> Optional[Any]:
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
            # Return first value from dict
            return next(iter(row.values())) if row else None
        if hasattr(row, "__getitem__"):
            # Handle tuple/list-like objects
            return row[0] if len(row) > 0 else None
        # Handle scalar or object with attributes
        return row

    @contextmanager
    def select_cursor(self, conn: Any, query_name: str, sql: str, parameters: "Any") -> Generator[Any, None, None]:
        """Execute a SELECT query and return cursor context manager.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters

        Yields:
            Cursor-like object with results
        """
        sql_obj = self._create_sql_object(sql, parameters)
        result = self.driver.execute(sql_obj, connection=conn)

        # Create a cursor-like object
        class CursorLike:
            def __init__(self, result: Any) -> None:
                self.result = result

            def fetchall(self) -> list[Any]:
                if isinstance(result, SQLResult) and result.data is not None:
                    return list(result.data)
                return []

            def fetchone(self) -> Optional[Any]:
                rows = self.fetchall()
                return rows[0] if rows else None

        yield CursorLike(result)

    def insert_update_delete(self, conn: Any, query_name: str, sql: str, parameters: "Any") -> int:
        """Execute INSERT/UPDATE/DELETE and return affected rows.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters

        Returns:
            Number of affected rows
        """
        sql_obj = self._create_sql_object(sql, parameters)
        result = cast("SQLResult[Any]", self.driver.execute(sql_obj, connection=conn))

        # SQLResult has rows_affected attribute
        return result.rows_affected if hasattr(result, "rows_affected") else 0

    def insert_update_delete_many(self, conn: Any, query_name: str, sql: str, parameters: "Any") -> int:
        """Execute INSERT/UPDATE/DELETE with many parameter sets.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Sequence of parameter sets

        Returns:
            Number of affected rows
        """
        # For executemany, we don't extract sqlspec filters from individual parameter sets
        sql_obj = self._create_sql_object(sql)

        result = cast("SQLResult[Any]", self.driver.execute_many(sql_obj, parameters=parameters, connection=conn))

        # SQLResult has rows_affected attribute
        return result.rows_affected if hasattr(result, "rows_affected") else 0

    def insert_returning(self, conn: Any, query_name: str, sql: str, parameters: "Any") -> Optional[Any]:
        """Execute INSERT with RETURNING and return result.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters

        Returns:
            Returned value or None
        """
        # INSERT RETURNING is treated like a select that returns data
        return self.select_one(conn, query_name, sql, parameters)


class AiosqlAsyncAdapter(_AiosqlAdapterBase):
    """Async adapter that implements aiosql protocol using SQLSpec drivers.

    This adapter bridges aiosql's async driver protocol with SQLSpec's async drivers,
    enabling all of SQLSpec's features to work with queries loaded by aiosql.
    """

    is_aio_driver: ClassVar[bool] = True

    def __init__(self, driver: "AsyncDriverAdapterProtocol[Any, Any]") -> None:
        """Initialize the async adapter.

        Args:
            driver: SQLSpec async driver to use for execution
        """
        super().__init__(driver)

    async def select(
        self, conn: Any, query_name: str, sql: str, parameters: "Any", record_class: Optional[Any] = None
    ) -> list[Any]:
        """Execute a SELECT query and return results as list.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters
            record_class: Deprecated - use schema_type in driver.execute instead

        Returns:
            List of query result rows

        Note:
            record_class parameter is ignored. Use schema_type in driver.execute
            or _sqlspec_schema_type in parameters for type mapping.
        """
        if record_class is not None:
            logger.warning(
                "record_class parameter is deprecated and ignored. "
                "Use schema_type in driver.execute or _sqlspec_schema_type in parameters."
            )

        sql_obj = self._create_sql_object(sql, parameters)

        result = await self.driver.execute(sql_obj, connection=conn)  # type: ignore[misc]

        if hasattr(result, "data") and result.data is not None and isinstance(result, SQLResult):
            return list(result.data)
        return []

    async def select_one(
        self, conn: Any, query_name: str, sql: str, parameters: "Any", record_class: Optional[Any] = None
    ) -> Optional[Any]:
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
            record_class parameter is ignored. Use schema_type in driver.execute
            or _sqlspec_schema_type in parameters for type mapping.
        """
        if record_class is not None:
            logger.warning(
                "record_class parameter is deprecated and ignored. "
                "Use schema_type in driver.execute or _sqlspec_schema_type in parameters."
            )

        sql_obj = self._create_sql_object(sql, parameters)

        result = await self.driver.execute(sql_obj, connection=conn)  # type: ignore[misc]

        if hasattr(result, "data") and result.data and isinstance(result, SQLResult):
            return result.data[0]
        return None

    async def select_value(self, conn: Any, query_name: str, sql: str, parameters: "Any") -> Optional[Any]:
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
            # Return first value from dict
            return next(iter(row.values())) if row else None
        if hasattr(row, "__getitem__"):
            # Handle tuple/list-like objects
            return row[0] if len(row) > 0 else None
        # Handle scalar or object with attributes
        return row

    @asynccontextmanager
    async def select_cursor(self, conn: Any, query_name: str, sql: str, parameters: "Any") -> AsyncGenerator[Any, None]:
        """Execute a SELECT query and return cursor context manager.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters

        Yields:
            Cursor-like object with results
        """
        sql_obj = self._create_sql_object(sql, parameters)
        result = await self.driver.execute(sql_obj, connection=conn)  # type: ignore[misc]

        class AsyncCursorLike:
            def __init__(self, result: Any) -> None:
                self.result = result

            @staticmethod
            async def fetchall() -> list[Any]:
                if isinstance(result, SQLResult) and result.data is not None:
                    return list(result.data)
                return []

            async def fetchone(self) -> Optional[Any]:
                rows = await self.fetchall()
                return rows[0] if rows else None

        yield AsyncCursorLike(result)

    async def insert_update_delete(self, conn: Any, query_name: str, sql: str, parameters: "Any") -> None:
        """Execute INSERT/UPDATE/DELETE.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters

        Note:
            Async version returns None per aiosql protocol
        """
        sql_obj = self._create_sql_object(sql, parameters)

        await self.driver.execute(sql_obj, connection=conn)  # type: ignore[misc]

    async def insert_update_delete_many(self, conn: Any, query_name: str, sql: str, parameters: "Any") -> None:
        """Execute INSERT/UPDATE/DELETE with many parameter sets.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Sequence of parameter sets

        Note:
            Async version returns None per aiosql protocol
        """
        # For executemany, we don't extract sqlspec filters from individual parameter sets
        sql_obj = self._create_sql_object(sql)
        await self.driver.execute_many(sql_obj, parameters=parameters, connection=conn)  # type: ignore[misc]

    async def insert_returning(self, conn: Any, query_name: str, sql: str, parameters: "Any") -> Optional[Any]:
        """Execute INSERT with RETURNING and return result.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters

        Returns:
            Returned value or None
        """
        # INSERT RETURNING is treated like a select that returns data
        return await self.select_one(conn, query_name, sql, parameters)
