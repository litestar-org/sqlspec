"""Database service implementation.

This module provides a high-level service layer that wraps database drivers
with instrumentation, convenience methods, and consistent error handling.
"""

from contextlib import contextmanager
from time import perf_counter
from typing import TYPE_CHECKING, Any, Generic, Optional, Union, overload

from sqlspec.config import DriverT
from sqlspec.exceptions import wrap_exceptions
from sqlspec.service.base import InstrumentedService
from sqlspec.typing import ConnectionT, ModelDTOT, RowT
from sqlspec.utils.correlation import CorrelationContext

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence

    from sqlspec.config import InstrumentationConfig
    from sqlspec.statement import SQL, SQLConfig, Statement
    from sqlspec.statement.builder import QueryBuilder
    from sqlspec.statement.filters import StatementFilter
    from sqlspec.statement.parameters import SQLParameterType
    from sqlspec.statement.result import SQLResult


__all__ = ("AsyncDatabaseService", "DatabaseService")


class DatabaseService(InstrumentedService, Generic[DriverT, ConnectionT, RowT]):
    """Synchronous database service providing high-level operations.

    This service wraps a database driver to provide:
    - Instrumented operations with logging and telemetry
    - Convenience methods for common patterns
    - Consistent error handling
    - Correlation tracking across operations
    """

    def __init__(
        self,
        driver: DriverT,
        instrumentation_config: Optional["InstrumentationConfig"] = None,
        service_name: Optional[str] = None,
    ) -> None:
        """Initialize the database service.

        Args:
            driver: The database driver to wrap
            instrumentation_config: Instrumentation configuration
            service_name: Name of the service for logging
        """
        super().__init__(instrumentation_config, service_name or "DatabaseService")
        self.driver = driver

    @contextmanager
    def _track_operation(self, operation: str, **context: Any) -> "Generator[dict[str, Any], None, None]":
        """Track a service operation with timing and logging.

        Args:
            operation: Name of the operation
            **context: Additional context to log

        Yields:
            Operation context dict that can be updated
        """
        start_time = perf_counter()
        op_context = {"correlation_id": CorrelationContext.get(), **context}

        self._log_operation_start(operation, **op_context)

        try:
            yield op_context
            duration_ms = (perf_counter() - start_time) * 1000
            # Extract result_count from context to avoid duplicate keyword argument
            result_count = op_context.pop("result_count", None)
            self._log_operation_complete(
                operation, duration_ms, result_count=result_count, **op_context
            )
        except Exception as e:
            self._log_operation_error(operation, e, **op_context)
            raise

    # Core execute methods that mirror driver interface

    @overload
    def execute(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]": ...

    @overload
    def execute(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "SQLResult[ModelDTOT]": ...

    def execute(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[SQLResult[ModelDTOT], SQLResult[RowT]]":
        """Execute a SQL statement.

        Args:
            statement: SQL statement to execute
            parameters: Query parameters
            *filters: Statement filters to apply
            connection: Optional connection to use
            config: Optional SQL configuration
            schema_type: Optional schema type for result deserialization
            **kwargs: Additional driver-specific arguments

        Returns:
            Query results
        """
        with (
            self._instrument("execute", statement_type=type(statement).__name__),
            self._track_operation("execute", statement_type=type(statement).__name__) as ctx,
            wrap_exceptions(),
        ):
            result = self.driver.execute(
                statement, parameters, *filters, connection=connection, config=config, schema_type=schema_type, **kwargs
            )
            ctx["result_count"] = getattr(result, "rowcount", None)
            return result

    def execute_many(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[Sequence[SQLParameterType]]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]":
        """Execute a statement multiple times with different parameters.

        Args:
            statement: SQL statement to execute
            parameters: Sequence of parameter sets
            *filters: Statement filters to apply
            connection: Optional connection to use
            config: Optional SQL configuration
            **kwargs: Additional driver-specific arguments

        Returns:
            Combined results
        """
        with (
            self._instrument("execute_many", batch_size=len(parameters) if parameters else 0),
            self._track_operation("execute_many", batch_size=len(parameters) if parameters else 0) as ctx,
            wrap_exceptions(),
        ):
            result = self.driver.execute_many(
                statement, parameters, *filters, connection=connection, config=config, **kwargs
            )
            ctx["result_count"] = getattr(result, "rowcount", None)
            return result

    # Convenience methods for common query patterns

    @overload
    def select(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "list[RowT]": ...

    @overload
    def select(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: "type[ModelDTOT]" = ...,
        **kwargs: Any,
    ) -> "list[ModelDTOT]": ...

    def select(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[list[ModelDTOT], list[RowT]]":
        """Execute a SELECT query and return all results.

        Args:
            statement: SELECT statement
            parameters: Query parameters
            *filters: Statement filters to apply
            connection: Optional connection to use
            config: Optional SQL configuration
            schema_type: Optional schema type for result deserialization
            **kwargs: Additional driver-specific arguments

        Returns:
            List of all results
        """
        with self._instrument("select"), self._track_operation("select") as ctx:
            result = self.execute(
                statement, parameters, *filters, connection=connection, config=config, schema_type=schema_type, **kwargs
            )
            data = list(result.all())
            ctx["result_count"] = len(data)
            return data

    @overload
    def select_one(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> RowT: ...

    @overload
    def select_one(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: "type[ModelDTOT]" = ...,
        **kwargs: Any,
    ) -> ModelDTOT: ...

    def select_one(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, RowT]":
        """Execute a SELECT query and return exactly one result.

        Args:
            statement: SELECT statement
            parameters: Query parameters
            *filters: Statement filters to apply
            connection: Optional connection to use
            config: Optional SQL configuration
            schema_type: Optional schema type for result deserialization
            **kwargs: Additional driver-specific arguments

        Returns:
            Single result

        Raises:
            NoResultFoundError: If no results found
            TooManyResultsError: If more than one result found
        """
        with self._instrument("select_one"), self._track_operation("select_one"):
            result = self.execute(
                statement, parameters, *filters, connection=connection, config=config, schema_type=schema_type, **kwargs
            )
            return result.one()

    @overload
    def select_one_or_none(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[RowT]": ...

    @overload
    def select_one_or_none(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: "type[ModelDTOT]" = ...,
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...

    def select_one_or_none(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[Optional[ModelDTOT], Optional[RowT]]":
        """Execute a SELECT query and return at most one result.

        Args:
            statement: SELECT statement
            parameters: Query parameters
            *filters: Statement filters to apply
            connection: Optional connection to use
            config: Optional SQL configuration
            schema_type: Optional schema type for result deserialization
            **kwargs: Additional driver-specific arguments

        Returns:
            Single result or None

        Raises:
            TooManyResultsError: If more than one result found
        """
        with self._instrument("select_one_or_none"), self._track_operation("select_one_or_none"):
            result = self.execute(
                statement, parameters, *filters, connection=connection, config=config, schema_type=schema_type, **kwargs
            )
            return result.one_or_none()

    def select_value(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a SELECT query and return a single scalar value.

        Args:
            statement: SELECT statement (should return single column)
            parameters: Query parameters
            *filters: Statement filters to apply
            connection: Optional connection to use
            config: Optional SQL configuration
            **kwargs: Additional driver-specific arguments

        Returns:
            Scalar value from first column of first row

        Raises:
            NoResultFoundError: If no results found
            TooManyResultsError: If more than one result found
        """
        with self._instrument("select_value"), self._track_operation("select_value"):
            result = self.execute(statement, parameters, *filters, connection=connection, config=config, **kwargs)
            return result.scalar()

    def select_value_or_none(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a SELECT query and return a single scalar value or None.

        Args:
            statement: SELECT statement (should return single column)
            parameters: Query parameters
            *filters: Statement filters to apply
            connection: Optional connection to use
            config: Optional SQL configuration
            **kwargs: Additional driver-specific arguments

        Returns:
            Scalar value from first column of first row or None

        Raises:
            TooManyResultsError: If more than one result found
        """
        with self._instrument("select_value_or_none"), self._track_operation("select_value_or_none"):
            result = self.execute(statement, parameters, *filters, connection=connection, config=config, **kwargs)
            return result.scalar_or_none()

    # DML convenience methods

    def insert(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]":
        """Execute an INSERT statement.

        Args:
            statement: INSERT statement
            parameters: Query parameters
            *filters: Statement filters to apply
            connection: Optional connection to use
            config: Optional SQL configuration
            **kwargs: Additional driver-specific arguments

        Returns:
            Execution result
        """
        with self._instrument("insert"), self._track_operation("insert") as ctx:
            result = self.execute(statement, parameters, *filters, connection=connection, config=config, **kwargs)
            ctx["rows_affected"] = getattr(result, "rowcount", None)
            return result

    def update(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]":
        """Execute an UPDATE statement.

        Args:
            statement: UPDATE statement
            parameters: Query parameters
            *filters: Statement filters to apply
            connection: Optional connection to use
            config: Optional SQL configuration
            **kwargs: Additional driver-specific arguments

        Returns:
            Execution result
        """
        with self._instrument("update"), self._track_operation("update") as ctx:
            result = self.execute(statement, parameters, *filters, connection=connection, config=config, **kwargs)
            ctx["rows_affected"] = getattr(result, "rowcount", None)
            return result

    def delete(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]":
        """Execute a DELETE statement.

        Args:
            statement: DELETE statement
            parameters: Query parameters
            *filters: Statement filters to apply
            connection: Optional connection to use
            config: Optional SQL configuration
            **kwargs: Additional driver-specific arguments

        Returns:
            Execution result
        """
        with self._instrument("delete"), self._track_operation("delete") as ctx:
            result = self.execute(statement, parameters, *filters, connection=connection, config=config, **kwargs)
            ctx["rows_affected"] = getattr(result, "rowcount", None)
            return result


class AsyncDatabaseService(InstrumentedService, Generic[DriverT, ConnectionT, RowT]):
    """Asynchronous database service providing high-level operations.

    This service wraps an async database driver to provide:
    - Instrumented operations with logging and telemetry
    - Convenience methods for common patterns
    - Consistent error handling
    - Correlation tracking across operations
    """

    def __init__(
        self,
        driver: DriverT,
        instrumentation_config: Optional["InstrumentationConfig"] = None,
        service_name: Optional[str] = None,
    ) -> None:
        """Initialize the async database service.

        Args:
            driver: The async database driver to wrap
            instrumentation_config: Instrumentation configuration
            service_name: Name of the service for logging
        """
        super().__init__(instrumentation_config, service_name or "AsyncDatabaseService")
        self.driver = driver

    @contextmanager
    def _track_operation(self, operation: str, **context: Any) -> "Generator[dict[str, Any], None, None]":
        """Track a service operation with timing and logging.

        Args:
            operation: Name of the operation
            **context: Additional context to log

        Yields:
            Operation context dict that can be updated
        """
        start_time = perf_counter()
        op_context = {"correlation_id": CorrelationContext.get(), **context}

        self._log_operation_start(operation, **op_context)

        try:
            yield op_context
            duration_ms = (perf_counter() - start_time) * 1000
            # Extract result_count from context to avoid duplicate keyword argument
            result_count = op_context.pop("result_count", None)
            self._log_operation_complete(
                operation, duration_ms, result_count=result_count, **op_context
            )
        except Exception as e:
            self._log_operation_error(operation, e, **op_context)
            raise

    # Core execute methods that mirror async driver interface

    @overload
    async def execute(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]": ...

    @overload
    async def execute(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: "type[ModelDTOT]" = ...,
        **kwargs: Any,
    ) -> "SQLResult[ModelDTOT]": ...

    async def execute(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[SQLResult[ModelDTOT], SQLResult[RowT]]":
        """Execute a SQL statement asynchronously.

        Args:
            statement: SQL statement to execute
            parameters: Query parameters
            *filters: Statement filters to apply
            connection: Optional connection to use
            config: Optional SQL configuration
            schema_type: Optional schema type for result deserialization
            **kwargs: Additional driver-specific arguments

        Returns:
            Query results
        """
        with (
            self._instrument("execute", statement_type=type(statement).__name__),
            self._track_operation("execute", statement_type=type(statement).__name__) as ctx,
            wrap_exceptions(),
        ):
            result = await self.driver.execute(
                statement, parameters, *filters, connection=connection, config=config, schema_type=schema_type, **kwargs
            )
            ctx["result_count"] = getattr(result, "rowcount", None)
            return result

    async def execute_many(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[Sequence[SQLParameterType]]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]":
        """Execute a statement multiple times with different parameters asynchronously.

        Args:
            statement: SQL statement to execute
            parameters: Sequence of parameter sets
            *filters: Statement filters to apply
            connection: Optional connection to use
            config: Optional SQL configuration
            **kwargs: Additional driver-specific arguments

        Returns:
            Combined results
        """
        with (
            self._instrument("execute_many", batch_size=len(parameters) if parameters else 0),
            self._track_operation("execute_many", batch_size=len(parameters) if parameters else 0) as ctx,
            wrap_exceptions(),
        ):
            result = await self.driver.execute_many(
                statement, parameters, *filters, connection=connection, config=config, **kwargs
            )
            ctx["result_count"] = getattr(result, "rowcount", None)
            return result

    # Async convenience methods for common query patterns

    @overload
    async def select(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "list[RowT]": ...

    @overload
    async def select(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: "type[ModelDTOT]" = ...,
        **kwargs: Any,
    ) -> "list[ModelDTOT]": ...

    async def select(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[list[ModelDTOT], list[RowT]]":
        """Execute a SELECT query and return all results asynchronously.

        Args:
            statement: SELECT statement
            parameters: Query parameters
            *filters: Statement filters to apply
            connection: Optional connection to use
            config: Optional SQL configuration
            schema_type: Optional schema type for result deserialization
            **kwargs: Additional driver-specific arguments

        Returns:
            List of all results
        """
        with self._instrument("select"), self._track_operation("select") as ctx:
            result = await self.execute(
                statement, parameters, *filters, connection=connection, config=config, schema_type=schema_type, **kwargs
            )
            data = await result.all()
            ctx["result_count"] = len(data)
            return data

    @overload
    async def select_one(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> RowT: ...

    @overload
    async def select_one(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: "type[ModelDTOT]" = ...,
        **kwargs: Any,
    ) -> ModelDTOT: ...

    async def select_one(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, RowT]":
        """Execute a SELECT query and return exactly one result asynchronously.

        Args:
            statement: SELECT statement
            parameters: Query parameters
            *filters: Statement filters to apply
            connection: Optional connection to use
            config: Optional SQL configuration
            schema_type: Optional schema type for result deserialization
            **kwargs: Additional driver-specific arguments

        Returns:
            Single result

        Raises:
            NoResultFoundError: If no results found
            TooManyResultsError: If more than one result found
        """
        with self._instrument("select_one"), self._track_operation("select_one"):
            result = await self.execute(
                statement, parameters, *filters, connection=connection, config=config, schema_type=schema_type, **kwargs
            )
            return await result.one()

    @overload
    async def select_one_or_none(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[RowT]": ...

    @overload
    async def select_one_or_none(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: "type[ModelDTOT]" = ...,
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...

    async def select_one_or_none(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[Optional[ModelDTOT], Optional[RowT]]":
        """Execute a SELECT query and return at most one result asynchronously.

        Args:
            statement: SELECT statement
            parameters: Query parameters
            *filters: Statement filters to apply
            connection: Optional connection to use
            config: Optional SQL configuration
            schema_type: Optional schema type for result deserialization
            **kwargs: Additional driver-specific arguments

        Returns:
            Single result or None

        Raises:
            TooManyResultsError: If more than one result found
        """
        with self._instrument("select_one_or_none"), self._track_operation("select_one_or_none"):
            result = await self.execute(
                statement, parameters, *filters, connection=connection, config=config, schema_type=schema_type, **kwargs
            )
            return await result.one_or_none()

    async def select_value(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a SELECT query and return a single scalar value asynchronously.

        Args:
            statement: SELECT statement (should return single column)
            parameters: Query parameters
            *filters: Statement filters to apply
            connection: Optional connection to use
            config: Optional SQL configuration
            **kwargs: Additional driver-specific arguments

        Returns:
            Scalar value from first column of first row

        Raises:
            NoResultFoundError: If no results found
            TooManyResultsError: If more than one result found
        """
        with self._instrument("select_value"), self._track_operation("select_value"):
            result = await self.execute(statement, parameters, *filters, connection=connection, config=config, **kwargs)
            return await result.scalar()

    async def select_value_or_none(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a SELECT query and return a single scalar value or None asynchronously.

        Args:
            statement: SELECT statement (should return single column)
            parameters: Query parameters
            *filters: Statement filters to apply
            connection: Optional connection to use
            config: Optional SQL configuration
            **kwargs: Additional driver-specific arguments

        Returns:
            Scalar value from first column of first row or None

        Raises:
            TooManyResultsError: If more than one result found
        """
        with self._instrument("select_value_or_none"), self._track_operation("select_value_or_none"):
            result = await self.execute(statement, parameters, *filters, connection=connection, config=config, **kwargs)
            return await result.scalar_or_none()

    # Async DML convenience methods

    async def insert(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]":
        """Execute an INSERT statement asynchronously.

        Args:
            statement: INSERT statement
            parameters: Query parameters
            *filters: Statement filters to apply
            connection: Optional connection to use
            config: Optional SQL configuration
            **kwargs: Additional driver-specific arguments

        Returns:
            Execution result
        """
        with self._instrument("insert"), self._track_operation("insert") as ctx:
            result = await self.execute(statement, parameters, *filters, connection=connection, config=config, **kwargs)
            ctx["rows_affected"] = getattr(result, "rowcount", None)
            return result

    async def update(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]":
        """Execute an UPDATE statement asynchronously.

        Args:
            statement: UPDATE statement
            parameters: Query parameters
            *filters: Statement filters to apply
            connection: Optional connection to use
            config: Optional SQL configuration
            **kwargs: Additional driver-specific arguments

        Returns:
            Execution result
        """
        with self._instrument("update"), self._track_operation("update") as ctx:
            result = await self.execute(statement, parameters, *filters, connection=connection, config=config, **kwargs)
            ctx["rows_affected"] = getattr(result, "rowcount", None)
            return result

    async def delete(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]":
        """Execute a DELETE statement asynchronously.

        Args:
            statement: DELETE statement
            parameters: Query parameters
            *filters: Statement filters to apply
            connection: Optional connection to use
            config: Optional SQL configuration
            **kwargs: Additional driver-specific arguments

        Returns:
            Execution result
        """
        with self._instrument("delete"), self._track_operation("delete") as ctx:
            result = await self.execute(statement, parameters, *filters, connection=connection, config=config, **kwargs)
            ctx["rows_affected"] = getattr(result, "rowcount", None)
            return result
