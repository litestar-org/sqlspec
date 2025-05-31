# ruff: noqa: PLR6301
"""AioSQL adapter implementation for SQLSpec.

This module provides adapter classes that implement the aiosql adapter protocols
while using SQLSpec drivers under the hood. This enables users to load SQL queries
from files using aiosql while leveraging all of SQLSpec's advanced features.
"""

import logging
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Optional,
    TypeVar,
    Union,
    cast,
)

from sqlspec.exceptions import MissingDependencyError
from sqlspec.service import SqlspecService
from sqlspec.statement.filters import StatementFilter
from sqlspec.statement.result import SelectResult
from sqlspec.statement.sql import SQL
from sqlspec.typing import AIOSQL_INSTALLED, DictRow, ModelDTOT, aiosql

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol

logger = logging.getLogger("sqlspec.extensions.aiosql")

__all__ = (
    "AiosqlAsyncAdapter",
    "AiosqlService",
    "AiosqlSyncAdapter",
)

T = TypeVar("T")


def _check_aiosql_available() -> None:
    if not AIOSQL_INSTALLED:
        msg = "aiosql"
        raise MissingDependencyError(msg, "aiosql")


class AiosqlSyncAdapter:
    """Sync adapter that implements aiosql protocol using SQLSpec drivers.

    This adapter bridges aiosql's sync driver protocol with SQLSpec's sync drivers,
    enabling all of SQLSpec's features (filters, instrumentation, validation) to work
    with queries loaded by aiosql.

    Example:
        >>> from sqlspec.adapters.psycopg import PsycopgSyncConfig
        >>> from sqlspec.extensions.aiosql import AiosqlSyncAdapter
        >>> import aiosql
        >>> # Create SQLSpec driver
        >>> config = PsycopgSyncConfig(...)
        >>> driver = config.create_driver()
        >>> # Create aiosql adapter
        >>> adapter = AiosqlSyncAdapter(driver)
        >>> # Load queries with aiosql
        >>> queries = aiosql.from_path("queries.sql", adapter)
        >>> # Use with SQLSpec filters
        >>> from sqlspec.statement.filters import LimitOffsetFilter
        >>> result = queries.get_users_with_sqlspec_filters(
        ...     conn,
        ...     name="John",
        ...     _sqlspec_filters=[LimitOffsetFilter(10, 0)],
        ... )
    """

    is_aio_driver: ClassVar[bool] = False

    def __init__(
        self,
        driver: "SyncDriverAdapterProtocol[Any, Any]",
        default_filters: "Optional[Sequence[StatementFilter]]" = None,
        allow_sqlspec_filters: bool = True,
    ) -> None:
        """Initialize the sync adapter.

        Args:
            driver: SQLSpec sync driver to use for execution
            default_filters: Default filters to apply to all queries
            allow_sqlspec_filters: Whether to allow _sqlspec_filters parameter
        """
        _check_aiosql_available()
        self.driver = driver
        self.default_filters = list(default_filters or [])
        self.allow_sqlspec_filters = allow_sqlspec_filters

    def process_sql(self, query_name: str, op_type: "Any", sql: str) -> str:
        """Process SQL for aiosql compatibility.

        Args:
            query_name: Name of the query
            op_type: Operation type from aiosql
            sql: Raw SQL string

        Returns:
            Processed SQL string
        """
        # For now, return SQL as-is. SQLSpec will handle processing during execution.
        return sql

    def _extract_sqlspec_filters(self, parameters: "Any") -> tuple["Any", list[StatementFilter]]:
        """Extract SQLSpec filters from parameters.

        Args:
            parameters: Original parameters from aiosql

        Returns:
            Tuple of (cleaned_parameters, extracted_filters)
        """
        if not self.allow_sqlspec_filters or not isinstance(parameters, dict):
            return parameters, self.default_filters.copy()

        # Extract _sqlspec_filters if present
        cleaned_params = parameters.copy()
        sqlspec_filters = cleaned_params.pop("_sqlspec_filters", [])

        # Combine default filters with provided filters
        all_filters = self.default_filters.copy()
        if sqlspec_filters:
            if isinstance(sqlspec_filters, list):
                all_filters.extend(sqlspec_filters)
            else:
                all_filters.append(sqlspec_filters)

        return cleaned_params, all_filters

    def select(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: "Any",
        record_class: Optional[Any] = None,
    ) -> Generator[Any, None, None]:
        """Execute a SELECT query and return results as generator.

        Args:
            conn: Database connection (passed through to SQLSpec driver)
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters
            record_class: Optional record class for result mapping

        Yields:
            Query result rows
        """
        cleaned_params, filters = self._extract_sqlspec_filters(parameters)

        # Create SQL object and apply filters
        sql_obj = SQL(sql, *filters, parameters=cleaned_params, dialect=self.driver.dialect)

        # Execute using SQLSpec driver
        result = self.driver.execute(sql_obj, connection=conn, schema_type=record_class)

        # Convert to generator as expected by aiosql
        # Check if it's a SelectResult (has rows) or ExecuteResult
        # Import here to avoid circular imports
        from sqlspec.statement.result import SelectResult

        if isinstance(result, SelectResult) and result.rows is not None:
            yield from result.rows

    def select_one(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: "Any",
        record_class: Optional[Any] = None,
    ) -> Optional[Any]:
        """Execute a SELECT query and return first result.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters
            record_class: Optional record class for result mapping

        Returns:
            First result row or None
        """
        cleaned_params, filters = self._extract_sqlspec_filters(parameters)

        sql_obj = SQL(sql, *filters, parameters=cleaned_params, dialect=self.driver.dialect)

        result = cast("SelectResult[DictRow]", self.driver.execute(sql_obj, connection=conn, schema_type=record_class))

        if hasattr(result, "rows") and result.rows and isinstance(result, SelectResult):
            return result.rows[0]
        return None

    def select_value(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: "Any",
    ) -> Optional[Any]:
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
    def select_cursor(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: "Any",
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
        cleaned_params, filters = self._extract_sqlspec_filters(parameters)

        sql_obj = SQL(sql, parameters=cleaned_params, dialect=self.driver.dialect)
        for filter_obj in filters:
            sql_obj = sql_obj.append_filter(filter_obj)

        result = self.driver.execute(sql_obj, connection=conn)

        # Create a cursor-like object
        class CursorLike:
            def __init__(self, result: Any) -> None:
                self.result = result

            def fetchall(self) -> list[Any]:
                if isinstance(result, SelectResult) and result.rows is not None:
                    return list(result.rows)
                return []

            def fetchone(self) -> Optional[Any]:
                rows = self.fetchall()
                return rows[0] if rows else None

        yield CursorLike(result)

    def insert_update_delete(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: "Any",
    ) -> int:
        """Execute INSERT/UPDATE/DELETE and return affected rows.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters

        Returns:
            Number of affected rows
        """
        cleaned_params, filters = self._extract_sqlspec_filters(parameters)

        sql_obj = SQL(sql, parameters=cleaned_params, dialect=self.driver.dialect)
        for filter_obj in filters:
            sql_obj = sql_obj.append_filter(filter_obj)

        result = self.driver.execute(sql_obj, connection=conn)

        return getattr(result, "rows_affected", 0)

    def insert_update_delete_many(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: "Any",
    ) -> int:
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
        sql_obj = SQL(sql, dialect=self.driver.dialect)
        for filter_obj in self.default_filters:
            sql_obj = sql_obj.append_filter(filter_obj)

        result = self.driver.execute_many(sql_obj, parameters=parameters, connection=conn)

        return getattr(result, "rows_affected", 0)

    def insert_returning(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: "Any",
    ) -> Optional[Any]:
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


class AiosqlAsyncAdapter:
    """Async adapter that implements aiosql protocol using SQLSpec drivers.

    This adapter bridges aiosql's async driver protocol with SQLSpec's async drivers,
    enabling all of SQLSpec's features to work with queries loaded by aiosql.
    """

    is_aio_driver: ClassVar[bool] = True

    def __init__(
        self,
        driver: "AsyncDriverAdapterProtocol[Any, Any]",
        default_filters: "Optional[Sequence[StatementFilter]]" = None,
        allow_sqlspec_filters: bool = True,
    ) -> None:
        """Initialize the async adapter.

        Args:
            driver: SQLSpec async driver to use for execution
            default_filters: Default filters to apply to all queries
            allow_sqlspec_filters: Whether to allow _sqlspec_filters parameter
        """
        _check_aiosql_available()
        self.driver = driver
        self.default_filters = list(default_filters or [])
        self.allow_sqlspec_filters = allow_sqlspec_filters

    def process_sql(self, query_name: str, op_type: "Any", sql: str) -> str:
        """Process SQL for aiosql compatibility.

        Args:
            query_name: Name of the query
            op_type: Operation type from aiosql
            sql: Raw SQL string

        Returns:
            Processed SQL string
        """
        return sql

    def _extract_sqlspec_filters(self, parameters: "Any") -> tuple["Any", list[StatementFilter]]:
        """Extract SQLSpec filters from parameters.

        Args:
            parameters: Original parameters from aiosql

        Returns:
            Tuple of (cleaned_parameters, extracted_filters)
        """
        if not self.allow_sqlspec_filters or not isinstance(parameters, dict):
            return parameters, self.default_filters.copy()

        cleaned_params = parameters.copy()
        sqlspec_filters = cleaned_params.pop("_sqlspec_filters", [])

        all_filters = self.default_filters.copy()
        if sqlspec_filters:
            if isinstance(sqlspec_filters, list):
                all_filters.extend(sqlspec_filters)
            else:
                all_filters.append(sqlspec_filters)

        return cleaned_params, all_filters

    async def select(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: "Any",
        record_class: Optional[Any] = None,
    ) -> list[Any]:
        """Execute a SELECT query and return results as list.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters
            record_class: Optional record class for result mapping

        Returns:
            List of query result rows
        """
        cleaned_params, filters = self._extract_sqlspec_filters(parameters)

        sql_obj = SQL(sql, parameters=cleaned_params, dialect=self.driver.dialect)
        for filter_obj in filters:
            sql_obj = sql_obj.append_filter(filter_obj)

        result = cast(
            "SelectResult[DictRow]", await self.driver.execute(sql_obj, connection=conn, schema_type=record_class)
        )

        if hasattr(result, "rows") and result.rows is not None and isinstance(result, SelectResult):
            return list(result.rows)
        return []

    async def select_one(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: "Any",
        record_class: Optional[Any] = None,
    ) -> Optional[Any]:
        """Execute a SELECT query and return first result.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters
            record_class: Optional record class for result mapping

        Returns:
            First result row or None
        """
        cleaned_params, filters = self._extract_sqlspec_filters(parameters)

        from sqlspec.statement.filters import LimitOffsetFilter

        filters.append(LimitOffsetFilter(limit=1, offset=0))

        sql_obj = SQL(sql, parameters=cleaned_params, dialect=self.driver.dialect)
        for filter_obj in filters:
            sql_obj = sql_obj.append_filter(filter_obj)

        result = cast(
            "SelectResult[DictRow]", await self.driver.execute(sql_obj, connection=conn, schema_type=record_class)
        )

        if hasattr(result, "rows") and result.rows and isinstance(result, SelectResult):
            return result.rows[0]
        return None

    async def select_value(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: "Any",
    ) -> Optional[Any]:
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

    @asynccontextmanager
    async def select_cursor(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: "Any",
    ) -> AsyncGenerator[Any, None]:
        """Execute a SELECT query and return async cursor context manager.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters

        Yields:
            Async cursor-like object with results
        """
        cleaned_params, filters = self._extract_sqlspec_filters(parameters)

        sql_obj = SQL(sql, parameters=cleaned_params, dialect=self.driver.dialect)
        for filter_obj in filters:
            sql_obj = sql_obj.append_filter(filter_obj)

        result = await self.driver.execute(sql_obj, connection=conn)

        class AsyncCursorLike:
            def __init__(self, result: Any) -> None:
                self.result = result

            async def fetchall(self) -> list[Any]:
                if isinstance(result, SelectResult) and result.rows is not None:
                    return list(result.rows)
                return []

            async def fetchone(self) -> Optional[Any]:
                rows = await self.fetchall()
                return rows[0] if rows else None

        yield AsyncCursorLike(result)

    async def insert_update_delete(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: "Any",
    ) -> None:
        """Execute INSERT/UPDATE/DELETE.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Query parameters
        """
        cleaned_params, filters = self._extract_sqlspec_filters(parameters)

        sql_obj = SQL(sql, parameters=cleaned_params, dialect=self.driver.dialect)
        for filter_obj in filters:
            sql_obj = sql_obj.append_filter(filter_obj)

        await self.driver.execute(sql_obj, connection=conn)

    async def insert_update_delete_many(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: "Any",
    ) -> None:
        """Execute INSERT/UPDATE/DELETE with many parameter sets.

        Args:
            conn: Database connection
            query_name: Name of the query
            sql: SQL string
            parameters: Sequence of parameter sets
        """
        sql_obj = SQL(sql, dialect=self.driver.dialect)
        for filter_obj in self.default_filters:
            sql_obj = sql_obj.append_filter(filter_obj)

        await self.driver.execute_many(sql_obj, parameters=parameters, connection=conn)

    async def insert_returning(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: "Any",
    ) -> Optional[Any]:
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


class AiosqlService(
    SqlspecService[Union["SyncDriverAdapterProtocol[Any, Any]", "AsyncDriverAdapterProtocol[Any, Any]"]]
):
    """Service layer that integrates aiosql query loading with SQLSpec drivers.

    This service combines aiosql's file-based query loading with SQLSpec's advanced
    features, providing a high-level interface for database operations.

    Example:
        >>> from sqlspec.adapters.psycopg import PsycopgSyncConfig
        >>> from sqlspec.extensions.aiosql import AiosqlService
        >>> # Create SQLSpec driver
        >>> config = PsycopgSyncConfig(...)
        >>> driver = config.create_driver()
        >>> # Create service with aiosql integration
        >>> service = AiosqlService(driver)
        >>> # Load queries from file
        >>> queries = service.load_queries("queries.sql")
        >>> # Use queries with SQLSpec features
        >>> from sqlspec.statement.filters import SearchFilter
        >>> result = service.execute_query_with_filters(
        ...     queries.get_users,
        ...     conn,
        ...     {"name": "John"},
        ...     [SearchFilter("email", "@example.com")],
        ... )
    """

    def __init__(
        self,
        driver: Union["SyncDriverAdapterProtocol[Any, Any]", "AsyncDriverAdapterProtocol[Any, Any]"],
        default_filters: "Optional[Sequence[StatementFilter]]" = None,
        allow_sqlspec_filters: bool = True,
    ) -> None:
        """Initialize the aiosql service.

        Args:
            driver: SQLSpec driver (sync or async)
            default_filters: Default filters to apply to all queries
            allow_sqlspec_filters: Whether to allow _sqlspec_filters parameter
        """
        super().__init__(driver)
        self.default_filters = default_filters
        self.allow_sqlspec_filters = allow_sqlspec_filters
        self._aiosql_adapter: Optional[Union[AiosqlSyncAdapter, AiosqlAsyncAdapter]] = None

    @property
    def aiosql_adapter(self) -> Union[AiosqlSyncAdapter, AiosqlAsyncAdapter]:
        """Get the appropriate aiosql adapter for the driver."""
        if self._aiosql_adapter is None:
            # Check if driver is async by looking for async methods
            if hasattr(self._driver, "execute") and callable(self._driver.execute):
                # Try to determine if it's async by checking the method signature
                import inspect

                if inspect.iscoroutinefunction(self._driver.execute):
                    self._aiosql_adapter = AiosqlAsyncAdapter(
                        cast("AsyncDriverAdapterProtocol[Any, Any]", self._driver),
                        self.default_filters,
                        self.allow_sqlspec_filters,
                    )
                else:
                    self._aiosql_adapter = AiosqlSyncAdapter(
                        cast("SyncDriverAdapterProtocol[Any, Any]", self._driver),
                        self.default_filters,
                        self.allow_sqlspec_filters,
                    )
            else:
                # Fallback to sync adapter
                self._aiosql_adapter = AiosqlSyncAdapter(
                    cast("SyncDriverAdapterProtocol[Any, Any]", self._driver),
                    self.default_filters,
                    self.allow_sqlspec_filters,
                )

        return self._aiosql_adapter

    def load_queries(self, sql_path: str, **aiosql_kwargs: Any) -> Any:
        """Load queries from a SQL file using aiosql.

        Args:
            sql_path: Path to SQL file
            **aiosql_kwargs: Additional arguments passed to aiosql.from_path

        Returns:
            aiosql Queries object configured with SQLSpec adapter
        """
        _check_aiosql_available()

        return aiosql.from_path(sql_path, self.aiosql_adapter, **aiosql_kwargs)  # pyright: ignore

    def load_queries_from_str(self, sql_str: str, **aiosql_kwargs: Any) -> Any:
        """Load queries from a SQL string using aiosql.

        Args:
            sql_str: SQL string containing queries
            **aiosql_kwargs: Additional arguments passed to aiosql.from_str

        Returns:
            aiosql Queries object configured with SQLSpec adapter
        """
        _check_aiosql_available()

        return aiosql.from_str(sql_str, self.aiosql_adapter, **aiosql_kwargs)  # pyright: ignore

    def execute_query_with_filters(
        self,
        query_method: Any,
        connection: Any,
        parameters: Optional[dict[str, Any]] = None,
        filters: "Optional[Sequence[StatementFilter]]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute an aiosql query with additional SQLSpec filters.

        Args:
            query_method: Query method from aiosql queries object
            connection: Database connection
            parameters: Query parameters
            filters: Additional SQLSpec filters to apply
            schema_type: Optional schema type for result conversion
            **kwargs: Additional arguments passed to query method

        Returns:
            Query results with applied filters and optional schema conversion
        """
        # Combine parameters with filters
        final_params = dict(parameters or {})
        if filters:
            existing_filters = final_params.get("_sqlspec_filters", [])
            if not isinstance(existing_filters, list):
                existing_filters = [existing_filters] if existing_filters else []
            final_params["_sqlspec_filters"] = existing_filters + list(filters)

        # Execute query with enhanced parameters
        result = query_method(connection, **final_params, **kwargs)

        # Apply schema conversion if requested
        if schema_type and hasattr(result, "__iter__"):
            try:
                if isinstance(result, (list, tuple)):
                    return self.to_schema(result, schema_type=schema_type)
                return self.to_schema(list(result), schema_type=schema_type)
            except Exception as e:  # noqa: BLE001
                logger.warning("Failed to convert result to schema type %s: %s", schema_type, e)
                return result

        return result
