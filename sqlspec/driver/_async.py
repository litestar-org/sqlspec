"""Asynchronous driver protocol implementation."""

from abc import abstractmethod
from time import perf_counter
from typing import TYPE_CHECKING, Any, Final, TypeVar, overload

from sqlspec.core import SQL, StackResult, Statement, create_arrow_result
from sqlspec.core.stack import StackOperation, StatementStack
from sqlspec.driver._common import (
    CommonDriverAttributesMixin,
    DataDictionaryMixin,
    ExecutionResult,
    StackExecutionObserver,
    VersionInfo,
    describe_stack_statement,
    handle_single_row_error,
)
from sqlspec.driver.mixins import SQLTranslatorMixin, StorageDriverMixin
from sqlspec.exceptions import ImproperConfigurationError, StackExecutionError
from sqlspec.utils.arrow_helpers import convert_dict_to_arrow
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_pyarrow

if TYPE_CHECKING:
    from collections.abc import Sequence
    from contextlib import AbstractAsyncContextManager

    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, SQLResult, StatementConfig, StatementFilter
    from sqlspec.typing import ArrowReturnFormat, SchemaT, StatementParameters


__all__ = ("AsyncDataDictionaryBase", "AsyncDriverAdapterBase", "AsyncDriverT")


EMPTY_FILTERS: Final["list[StatementFilter]"] = []
_LOGGER_NAME: Final[str] = "sqlspec"
logger = get_logger(_LOGGER_NAME)

AsyncDriverT = TypeVar("AsyncDriverT", bound="AsyncDriverAdapterBase")


class AsyncDriverAdapterBase(CommonDriverAttributesMixin, SQLTranslatorMixin, StorageDriverMixin):
    """Base class for asynchronous database drivers."""

    __slots__ = ()
    is_async: bool = True

    @property
    @abstractmethod
    def data_dictionary(self) -> "AsyncDataDictionaryBase":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """

    async def dispatch_statement_execution(self, statement: "SQL", connection: "Any") -> "SQLResult":
        """Central execution dispatcher using the Template Method Pattern.

        Args:
            statement: The SQL statement to execute
            connection: The database connection to use

        Returns:
            The result of the SQL execution
        """
        runtime = self.observability
        compiled_sql, execution_parameters = statement.compile()
        processed_state = statement.get_processed_state()
        operation = getattr(processed_state, "operation_type", statement.operation_type)
        query_context = {
            "sql": compiled_sql,
            "parameters": execution_parameters,
            "driver": type(self).__name__,
            "operation": operation,
            "is_many": statement.is_many,
            "is_script": statement.is_script,
        }
        runtime.emit_query_start(**query_context)
        span = runtime.start_query_span(compiled_sql, operation, type(self).__name__)
        started = perf_counter()

        try:
            async with self.handle_database_exceptions(), self.with_cursor(connection) as cursor:
                special_result = await self._try_special_handling(cursor, statement)
                if special_result is not None:
                    result = special_result
                elif statement.is_script:
                    execution_result = await self._execute_script(cursor, statement)
                    result = self.build_statement_result(statement, execution_result)
                elif statement.is_many:
                    execution_result = await self._execute_many(cursor, statement)
                    result = self.build_statement_result(statement, execution_result)
                else:
                    execution_result = await self._execute_statement(cursor, statement)
                    result = self.build_statement_result(statement, execution_result)
        except Exception as exc:  # pragma: no cover
            runtime.span_manager.end_span(span, error=exc)
            runtime.emit_error(exc, **query_context)
            raise

        runtime.span_manager.end_span(span)
        duration = perf_counter() - started
        runtime.emit_query_complete(**{**query_context, "rows_affected": result.rows_affected})
        runtime.emit_statement_event(
            sql=compiled_sql,
            parameters=execution_parameters,
            driver=type(self).__name__,
            operation=operation,
            execution_mode=self.statement_config.execution_mode,
            is_many=statement.is_many,
            is_script=statement.is_script,
            rows_affected=result.rows_affected,
            duration_s=duration,
            storage_backend=(result.metadata or {}).get("storage_backend") if hasattr(result, "metadata") else None,
            started_at=started,
        )
        return result

    @abstractmethod
    def with_cursor(self, connection: Any) -> Any:
        """Create and return an async context manager for cursor acquisition and cleanup.

        Returns an async context manager that yields a cursor for database operations.
        Concrete implementations handle database-specific cursor creation and cleanup.
        """

    @abstractmethod
    def handle_database_exceptions(self) -> "AbstractAsyncContextManager[None]":
        """Handle database-specific exceptions and wrap them appropriately.

        Returns:
            AsyncContextManager that can be used in async with statements
        """

    @abstractmethod
    async def begin(self) -> None:
        """Begin a database transaction on the current connection."""

    @abstractmethod
    async def rollback(self) -> None:
        """Rollback the current transaction on the current connection."""

    @abstractmethod
    async def commit(self) -> None:
        """Commit the current transaction on the current connection."""

    @abstractmethod
    async def _try_special_handling(self, cursor: Any, statement: "SQL") -> "SQLResult | None":
        """Hook for database-specific special operations (e.g., PostgreSQL COPY, bulk operations).

        This method is called first in dispatch_statement_execution() to allow drivers to handle
        special operations that don't follow the standard SQL execution pattern.

        Args:
            cursor: Database cursor/connection object
            statement: SQL statement to analyze

        Returns:
            SQLResult if the special operation was handled and completed,
            None if standard execution should proceed
        """

    async def _execute_script(self, cursor: Any, statement: "SQL") -> ExecutionResult:
        """Execute a SQL script containing multiple statements.

        Default implementation splits the script and executes statements individually.
        Drivers can override for database-specific script execution methods.

        Args:
            cursor: Database cursor/connection object
            statement: SQL statement object with all necessary data and configuration

        Returns:
            ExecutionResult with script execution data including statement counts
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, self.statement_config, strip_trailing_semicolon=True)

        statement_count: int = len(statements)
        successful_count: int = 0

        for stmt in statements:
            single_stmt = statement.copy(statement=stmt, parameters=prepared_parameters)
            await self._execute_statement(cursor, single_stmt)
            successful_count += 1

        return self.create_execution_result(
            cursor, statement_count=statement_count, successful_statements=successful_count, is_script_result=True
        )

    async def execute_stack(
        self, stack: "StatementStack", *, continue_on_error: bool = False
    ) -> "tuple[StackResult, ...]":
        """Execute a StatementStack sequentially using the adapter's primitives."""

        if not isinstance(stack, StatementStack):
            msg = "execute_stack expects a StatementStack instance"
            raise TypeError(msg)
        if not stack:
            msg = "Cannot execute an empty StatementStack"
            raise ValueError(msg)

        results: list[StackResult] = []
        single_transaction = not continue_on_error

        with StackExecutionObserver(self, stack, continue_on_error, native_pipeline=False) as observer:
            started_transaction = False

            try:
                if single_transaction and not self._connection_in_transaction():
                    await self.begin()
                    started_transaction = True

                for index, operation in enumerate(stack.operations):
                    try:
                        result = await self._execute_stack_operation(operation)
                    except Exception as exc:  # pragma: no cover - exercised via tests
                        stack_error = StackExecutionError(
                            index,
                            describe_stack_statement(operation.statement),
                            exc,
                            adapter=type(self).__name__,
                            mode="continue-on-error" if continue_on_error else "fail-fast",
                        )

                        if started_transaction and not continue_on_error:
                            try:
                                await self.rollback()
                            except Exception as rollback_error:  # pragma: no cover - diagnostics only
                                logger.debug("Rollback after stack failure failed: %s", rollback_error)
                            started_transaction = False

                        if continue_on_error:
                            await self._rollback_after_stack_error_async()
                            observer.record_operation_error(stack_error)
                            results.append(StackResult.from_error(stack_error))
                            continue

                        raise stack_error from exc

                    results.append(StackResult(result=result))

                    if continue_on_error:
                        await self._commit_after_stack_operation_async()

                if started_transaction:
                    await self.commit()
            except Exception:
                if started_transaction:
                    try:
                        await self.rollback()
                    except Exception as rollback_error:  # pragma: no cover - diagnostics only
                        logger.debug("Rollback after stack failure failed: %s", rollback_error)
                raise

        return tuple(results)

    async def _rollback_after_stack_error_async(self) -> None:
        """Attempt to rollback after a stack operation error (async)."""

        try:
            await self.rollback()
        except Exception as rollback_error:  # pragma: no cover - driver-specific cleanup
            logger.debug("Rollback after stack error failed: %s", rollback_error)

    async def _commit_after_stack_operation_async(self) -> None:
        """Attempt to commit after a successful stack operation when not batching (async)."""

        try:
            await self.commit()
        except Exception as commit_error:  # pragma: no cover - driver-specific cleanup
            logger.debug("Commit after stack operation failed: %s", commit_error)

    @abstractmethod
    async def _execute_many(self, cursor: Any, statement: "SQL") -> ExecutionResult:
        """Execute SQL with multiple parameter sets (executemany).

        Must be implemented by each driver for database-specific executemany logic.

        Args:
            cursor: Database cursor/connection object
            statement: SQL statement object with all necessary data and configuration

        Returns:
            ExecutionResult with execution data for the many operation
        """

    @abstractmethod
    async def _execute_statement(self, cursor: Any, statement: "SQL") -> ExecutionResult:
        """Execute a single SQL statement.

        Must be implemented by each driver for database-specific execution logic.

        Args:
            cursor: Database cursor/connection object
            statement: SQL statement object with all necessary data and configuration

        Returns:
            ExecutionResult with execution data
        """

    async def execute(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute a statement with parameter handling."""
        sql_statement = self.prepare_statement(
            statement, parameters, statement_config=statement_config or self.statement_config, kwargs=kwargs
        )
        return await self.dispatch_statement_execution(statement=sql_statement, connection=self.connection)

    async def execute_many(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        parameters: "Sequence[StatementParameters]",
        *filters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute statement multiple times with different parameters.

        Parameters passed will be used as the batch execution sequence.
        """
        config = statement_config or self.statement_config

        if isinstance(statement, SQL):
            sql_statement = SQL(statement.raw_sql, parameters, statement_config=config, is_many=True, **kwargs)
        else:
            base_statement = self.prepare_statement(statement, filters, statement_config=config, kwargs=kwargs)
            sql_statement = SQL(base_statement.raw_sql, parameters, statement_config=config, is_many=True, **kwargs)

        return await self.dispatch_statement_execution(statement=sql_statement, connection=self.connection)

    async def execute_script(
        self,
        statement: "str | SQL",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute a multi-statement script.

        By default, validates each statement and logs warnings for dangerous
        operations. Use suppress_warnings=True for migrations and admin scripts.
        """
        config = statement_config or self.statement_config
        sql_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)

        return await self.dispatch_statement_execution(statement=sql_statement.as_script(), connection=self.connection)

    @overload
    async def select_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SchemaT": ...

    @overload
    async def select_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...

    async def select_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SchemaT | dict[str, Any]":
        """Execute a select statement and return exactly one row.

        Raises an exception if no rows or more than one row is returned.
        """
        result = await self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        try:
            return result.one(schema_type=schema_type)
        except ValueError as error:
            handle_single_row_error(error)

    @overload
    async def select_one_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SchemaT | None": ...

    @overload
    async def select_one_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "dict[str, Any] | None": ...

    async def select_one_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SchemaT | dict[str, Any] | None":
        """Execute a select statement and return at most one row.

        Returns None if no rows are found.
        Raises an exception if more than one row is returned.
        """
        result = await self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        return result.one_or_none(schema_type=schema_type)

    @overload
    async def select(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "list[SchemaT]": ...

    @overload
    async def select(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "list[dict[str, Any]]": ...

    async def select(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "list[SchemaT] | list[dict[str, Any]]":
        """Execute a select statement and return all rows."""
        result = await self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        return result.get_data(schema_type=schema_type)

    async def select_to_arrow(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        return_format: "ArrowReturnFormat" = "table",
        native_only: bool = False,
        batch_size: int | None = None,
        arrow_schema: Any = None,
        **kwargs: Any,
    ) -> "ArrowResult":
        """Execute query and return results as Apache Arrow format (async).

        This base implementation uses the conversion path: execute() → dict → Arrow.
        Adapters with native Arrow support (ADBC, DuckDB, BigQuery) override this
        method to use zero-copy native paths for 5-10x performance improvement.

        Args:
            statement: SQL query string, Statement, or QueryBuilder
            *parameters: Query parameters (same format as execute()/select())
            statement_config: Optional statement configuration override
            return_format: "table" for pyarrow.Table (default), "batch" for single RecordBatch,
                         "batches" for iterator of RecordBatches, "reader" for RecordBatchReader
            native_only: If True, raise error if native Arrow unavailable (default: False)
            batch_size: Rows per batch for "batch"/"batches" format (default: None = all rows)
            arrow_schema: Optional pyarrow.Schema for type casting
            **kwargs: Additional keyword arguments

        Returns:
            ArrowResult containing pyarrow.Table, RecordBatchReader, or RecordBatches

        Raises:
            ImproperConfigurationError: If native_only=True and adapter doesn't support native Arrow

        Examples:
            >>> result = await driver.select_to_arrow(
            ...     "SELECT * FROM users WHERE age > ?", 18
            ... )
            >>> df = result.to_pandas()
            >>> print(df.head())

            >>> # Force native Arrow path (raises error if unavailable)
            >>> result = await driver.select_to_arrow(
            ...     "SELECT * FROM users", native_only=True
            ... )
        """
        ensure_pyarrow()

        if native_only:
            msg = (
                f"Adapter '{self.__class__.__name__}' does not support native Arrow results. "
                f"Use native_only=False to allow conversion path, or switch to an adapter "
                f"with native Arrow support (ADBC, DuckDB, BigQuery)."
            )
            raise ImproperConfigurationError(msg)

        result = await self.execute(statement, *parameters, statement_config=statement_config, **kwargs)

        arrow_data = convert_dict_to_arrow(result.data, return_format=return_format, batch_size=batch_size)
        if arrow_schema is not None:
            import pyarrow as pa

            if not isinstance(arrow_schema, pa.Schema):
                msg = f"arrow_schema must be a pyarrow.Schema, got {type(arrow_schema).__name__}"
                raise TypeError(msg)

            arrow_data = arrow_data.cast(arrow_schema)  # type: ignore[union-attr]
        return create_arrow_result(
            statement=result.statement,
            data=arrow_data,
            rows_affected=result.rows_affected,
            last_inserted_id=result.last_inserted_id,
            execution_time=result.execution_time,
            metadata=result.metadata,
        )

    async def select_value(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a select statement and return a single scalar value.

        Expects exactly one row with one column.
        Raises an exception if no rows or more than one row/column is returned.
        """
        result = await self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        try:
            return result.scalar()
        except ValueError as error:
            handle_single_row_error(error)

    async def select_value_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a select statement and return a single scalar value or None.

        Returns None if no rows are found.
        Expects at most one row with one column.
        Raises an exception if more than one row is returned.
        """
        result = await self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        return result.scalar_or_none()

    @overload
    async def select_with_total(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "tuple[list[SchemaT], int]": ...

    @overload
    async def select_with_total(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "tuple[list[dict[str, Any]], int]": ...

    async def select_with_total(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "tuple[list[SchemaT] | list[dict[str, Any]], int]":
        """Execute a select statement and return both the data and total count.

        This method is designed for pagination scenarios where you need both
        the current page of data and the total number of rows that match the query.

        Args:
            statement: The SQL statement, QueryBuilder, or raw SQL string
            *parameters: Parameters for the SQL statement
            schema_type: Optional schema type for data transformation
            statement_config: Optional SQL configuration
            **kwargs: Additional keyword arguments

        Returns:
            A tuple containing:
            - List of data rows (transformed by schema_type if provided)
            - Total count of rows matching the query (ignoring LIMIT/OFFSET)
        """
        sql_statement = self.prepare_statement(
            statement, parameters, statement_config=statement_config or self.statement_config, kwargs=kwargs
        )
        count_result = await self.dispatch_statement_execution(self._create_count_query(sql_statement), self.connection)
        select_result = await self.execute(sql_statement)

        return (select_result.get_data(schema_type=schema_type), count_result.scalar())

    async def _execute_stack_operation(self, operation: "StackOperation") -> "SQLResult | ArrowResult | None":
        kwargs = dict(operation.keyword_arguments) if operation.keyword_arguments else {}

        if operation.method == "execute":
            return await self.execute(operation.statement, *operation.arguments, **kwargs)

        if operation.method == "execute_many":
            if not operation.arguments:
                msg = "execute_many stack operation requires parameter sets"
                raise ValueError(msg)
            parameter_sets = operation.arguments[0]
            filters = operation.arguments[1:]
            return await self.execute_many(operation.statement, parameter_sets, *filters, **kwargs)

        if operation.method == "execute_script":
            return await self.execute_script(operation.statement, *operation.arguments, **kwargs)

        if operation.method == "execute_arrow":
            return await self.select_to_arrow(operation.statement, *operation.arguments, **kwargs)

        msg = f"Unsupported stack operation method: {operation.method}"
        raise ValueError(msg)


class AsyncDataDictionaryBase(DataDictionaryMixin):
    """Base class for asynchronous data dictionary implementations."""

    @abstractmethod
    async def get_version(self, driver: "AsyncDriverAdapterBase") -> "VersionInfo | None":
        """Get database version information.

        Args:
            driver: Async database driver instance

        Returns:
            Version information or None if detection fails
        """

    @abstractmethod
    async def get_feature_flag(self, driver: "AsyncDriverAdapterBase", feature: str) -> bool:
        """Check if database supports a specific feature.

        Args:
            driver: Async database driver instance
            feature: Feature name to check

        Returns:
            True if feature is supported, False otherwise
        """

    @abstractmethod
    async def get_optimal_type(self, driver: "AsyncDriverAdapterBase", type_category: str) -> str:
        """Get optimal database type for a category.

        Args:
            driver: Async database driver instance
            type_category: Type category (e.g., 'json', 'uuid', 'boolean')

        Returns:
            Database-specific type name
        """

    async def get_tables(self, driver: "AsyncDriverAdapterBase", schema: "str | None" = None) -> "list[str]":
        """Get list of tables in schema.

        Args:
            driver: Async database driver instance
            schema: Schema name (None for default)

        Returns:
            List of table names
        """
        _ = driver, schema
        return []

    async def get_columns(
        self, driver: "AsyncDriverAdapterBase", table: str, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        """Get column information for a table.

        Args:
            driver: Async database driver instance
            table: Table name
            schema: Schema name (None for default)

        Returns:
            List of column metadata dictionaries
        """
        _ = driver, table, schema
        return []

    async def get_indexes(
        self, driver: "AsyncDriverAdapterBase", table: str, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        """Get index information for a table.

        Args:
            driver: Async database driver instance
            table: Table name
            schema: Schema name (None for default)

        Returns:
            List of index metadata dictionaries
        """
        _ = driver, table, schema
        return []

    def list_available_features(self) -> "list[str]":
        """List all features that can be checked via get_feature_flag.

        Returns:
            List of feature names this data dictionary supports
        """
        return self.get_default_features()
