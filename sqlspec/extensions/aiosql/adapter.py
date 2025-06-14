# ruff: noqa: PLR6301
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
from sqlspec.service import SqlspecService
from sqlspec.statement.filters import StatementFilter
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import AIOSQL_INSTALLED, DictRow, ModelDTOT, RowT, aiosql

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol

logger = logging.getLogger("sqlspec.extensions.aiosql")

__all__ = ("AiosqlAsyncAdapter", "AiosqlService", "AiosqlSyncAdapter")

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
        "psycopg2": "postgres",
        "asyncpg": "postgres",
        "psqlpy": "postgres",
        "sqlite3": "sqlite",
        "aiosqlite": "sqlite",
    }
    return dialect_mapping.get(dialect_str, dialect_str)


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

    def _create_sql_object(self, sql: str, parameters: "Any" = None) -> SQL:
        """Create SQL object with proper configuration.

        Args:
            sql: SQL string
            parameters: Query parameters

        Returns:
            SQL object with relaxed validation for aiosql compatibility
        """
        # Create relaxed config for aiosql compatibility
        config = SQLConfig(
            strict_mode=False,  # Allow DDL and other statements
            enable_validation=False,  # Skip validation for templates
        )

        # Normalize dialect for SQLGlot
        normalized_dialect = _normalize_dialect(self.driver.dialect)

        return SQL(sql, parameters=parameters, config=config, dialect=normalized_dialect)

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

        cleaned_params, filters = self._extract_sqlspec_filters(parameters)

        # Check for schema_type in parameters
        schema_type = None
        if isinstance(cleaned_params, dict):
            schema_type = cleaned_params.pop("_sqlspec_schema_type", None)

        # Create SQL object and apply filters
        sql_obj = self._create_sql_object(sql, cleaned_params)
        for filter_obj in filters:
            sql_obj = sql_obj.append_filter(filter_obj)

        # Execute using SQLSpec driver
        result = self.driver.execute(sql_obj, connection=conn, schema_type=schema_type)

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

        cleaned_params, filters = self._extract_sqlspec_filters(parameters)

        # Check for schema_type in parameters
        schema_type = None
        if isinstance(cleaned_params, dict):
            schema_type = cleaned_params.pop("_sqlspec_schema_type", None)

        sql_obj = self._create_sql_object(sql, cleaned_params)
        for filter_obj in filters:
            sql_obj = sql_obj.append_filter(filter_obj)

        result = cast("SQLResult[RowT]", self.driver.execute(sql_obj, connection=conn, schema_type=schema_type))

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
        cleaned_params, filters = self._extract_sqlspec_filters(parameters)

        sql_obj = self._create_sql_object(sql, cleaned_params)
        for filter_obj in filters:
            sql_obj = sql_obj.append_filter(filter_obj)

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
        cleaned_params, filters = self._extract_sqlspec_filters(parameters)

        sql_obj = self._create_sql_object(sql, cleaned_params)
        for filter_obj in filters:
            sql_obj = sql_obj.append_filter(filter_obj)

        result = self.driver.execute(sql_obj, connection=conn)

        return getattr(result, "rows_affected", 0)

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
        for filter_obj in self.default_filters:
            sql_obj = sql_obj.append_filter(filter_obj)

        result = self.driver.execute_many(sql_obj, parameters=parameters, connection=conn)

        return getattr(result, "rows_affected", 0)

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

    def _create_sql_object(self, sql: str, parameters: "Any" = None) -> SQL:
        """Create SQL object with proper configuration.

        Args:
            sql: SQL string
            parameters: Query parameters

        Returns:
            SQL object with relaxed validation for aiosql compatibility
        """
        # Create relaxed config for aiosql compatibility
        config = SQLConfig(
            strict_mode=False,  # Allow DDL and other statements
            enable_validation=False,  # Skip validation for templates
        )

        # Normalize dialect for SQLGlot
        normalized_dialect = _normalize_dialect(self.driver.dialect)

        return SQL(sql, parameters=parameters, config=config, dialect=normalized_dialect)

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

        cleaned_params, filters = self._extract_sqlspec_filters(parameters)

        # Check for schema_type in parameters
        schema_type = None
        if isinstance(cleaned_params, dict):
            schema_type = cleaned_params.pop("_sqlspec_schema_type", None)

        sql_obj = self._create_sql_object(sql, cleaned_params)
        for filter_obj in filters:
            sql_obj = sql_obj.append_filter(filter_obj)

        result = cast(
            "SQLResult[DictRow]", await self.driver.execute(sql_obj, connection=conn, schema_type=schema_type)
        )

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

        cleaned_params, filters = self._extract_sqlspec_filters(parameters)

        # Check for schema_type in parameters
        schema_type = None
        if isinstance(cleaned_params, dict):
            schema_type = cleaned_params.pop("_sqlspec_schema_type", None)

        sql_obj = self._create_sql_object(sql, cleaned_params)
        for filter_obj in filters:
            sql_obj = sql_obj.append_filter(filter_obj)

        result = cast(
            "SQLResult[DictRow]", await self.driver.execute(sql_obj, connection=conn, schema_type=schema_type)
        )

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
        cleaned_params, filters = self._extract_sqlspec_filters(parameters)

        sql_obj = self._create_sql_object(sql, cleaned_params)
        for filter_obj in filters:
            sql_obj = sql_obj.append_filter(filter_obj)

        result = await self.driver.execute(sql_obj, connection=conn)

        class AsyncCursorLike:
            def __init__(self, result: Any) -> None:
                self.result = result

            async def fetchall(self) -> list[Any]:
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
        cleaned_params, filters = self._extract_sqlspec_filters(parameters)

        sql_obj = self._create_sql_object(sql, cleaned_params)
        for filter_obj in filters:
            sql_obj = sql_obj.append_filter(filter_obj)

        await self.driver.execute(sql_obj, connection=conn)

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
        for filter_obj in self.default_filters:
            sql_obj = sql_obj.append_filter(filter_obj)

        await self.driver.execute_many(sql_obj, parameters=parameters, connection=conn)

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


class AiosqlService(
    SqlspecService[Union["SyncDriverAdapterProtocol[Any, Any]", "AsyncDriverAdapterProtocol[Any, Any]"]]
):
    """Enhanced service for aiosql integration with SQLSpec.

    Provides high-level abstractions for working with aiosql-loaded queries
    while leveraging all SQLSpec features like filters, validation, and instrumentation.

    Example:
        >>> from sqlspec.extensions.aiosql import AiosqlService
        >>> service = AiosqlService(
        ...     driver, default_filters=[LimitOffsetFilter(100)]
        ... )
        >>> queries = service.load_queries("user_queries.sql")
        >>> result = service.execute_query_with_filters(
        ...     queries.get_users,
        ...     connection=None,
        ...     parameters={"active": True},
        ...     filters=[SearchFilter("name", "John")],
        ...     schema_type=User,
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
        self.default_filters = list(default_filters or [])
        self.allow_sqlspec_filters = allow_sqlspec_filters

    @property
    def aiosql_adapter(self) -> Union[AiosqlSyncAdapter, AiosqlAsyncAdapter]:
        """Get the appropriate aiosql adapter for this service's driver.

        Returns:
            AiosqlSyncAdapter for sync drivers, AiosqlAsyncAdapter for async drivers
        """
        # Check if driver has async methods to determine type
        if hasattr(self.driver, "__aenter__") or any(
            hasattr(self.driver, method) and callable(getattr(self.driver, method))
            for method in ["aexecute", "aselect", "aexecute_many"]
        ):
            return AiosqlAsyncAdapter(
                cast("AsyncDriverAdapterProtocol[Any, Any]", self.driver),
                default_filters=self.default_filters,
                allow_sqlspec_filters=self.allow_sqlspec_filters,
            )
        return AiosqlSyncAdapter(
            cast("SyncDriverAdapterProtocol[Any, Any]", self.driver),
            default_filters=self.default_filters,
            allow_sqlspec_filters=self.allow_sqlspec_filters,
        )

    def load_queries(self, sql_path: str, **aiosql_kwargs: Any) -> Any:
        """Load queries from SQL file using aiosql with SQLSpec adapter.

        Args:
            sql_path: Path to SQL file
            **aiosql_kwargs: Additional arguments passed to aiosql.from_path

        Returns:
            aiosql queries object with SQLSpec power
        """
        if not aiosql:
            msg = "aiosql"
            raise MissingDependencyError(msg, "aiosql")

        # aiosql expects either a string name for registered adapters or a class/instance
        return aiosql.from_path(sql_path, self.aiosql_adapter, **aiosql_kwargs)  # type: ignore[arg-type]

    def load_queries_from_str(self, sql_str: str, **aiosql_kwargs: Any) -> Any:
        """Load queries from SQL string using aiosql with SQLSpec adapter.

        Args:
            sql_str: SQL string content
            **aiosql_kwargs: Additional arguments passed to aiosql.from_str

        Returns:
            aiosql queries object with SQLSpec power
        """
        if not aiosql:
            msg = "aiosql"
            raise MissingDependencyError(msg, "aiosql")

        # aiosql expects either a string name for registered adapters or a class/instance
        return aiosql.from_str(sql_str, self.aiosql_adapter, **aiosql_kwargs)  # type: ignore[arg-type]

    def execute_query_with_filters(
        self,
        query_method: Any,
        connection: Any,
        parameters: Optional[dict[str, Any]] = None,
        filters: "Optional[Sequence[StatementFilter]]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute aiosql query method with additional SQLSpec filters.

        Args:
            query_method: aiosql query method
            connection: Database connection
            parameters: Query parameters
            filters: Additional SQLSpec filters to apply
            schema_type: Schema type for result conversion
            **kwargs: Additional arguments

        Returns:
            Query result with applied filters and schema conversion
        """
        # Prepare parameters with SQLSpec enhancements
        enhanced_params = dict(parameters or {})

        # Add schema type if provided
        if schema_type:
            enhanced_params["_sqlspec_schema_type"] = schema_type

        # Add filters if provided
        if filters:
            enhanced_params["_sqlspec_filters"] = list(filters)

        # Execute query method
        return query_method(connection, **enhanced_params, **kwargs)
