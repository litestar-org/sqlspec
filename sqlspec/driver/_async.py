"""Asynchronous driver protocol implementation."""

from abc import abstractmethod
from time import perf_counter
from typing import TYPE_CHECKING, Any, Final, TypeVar, cast, overload

from mypy_extensions import mypyc_attr

from sqlspec.core import SQL, ProcessedState, StackResult, Statement, create_arrow_result
from sqlspec.core.stack import StackOperation, StatementStack
from sqlspec.driver._common import (
    AsyncExceptionHandler,
    CommonDriverAttributesMixin,
    DataDictionaryMixin,
    ExecutionResult,
    StackExecutionObserver,
    VersionInfo,
    describe_stack_statement,
    handle_single_row_error,
)
from sqlspec.driver._sql_helpers import DEFAULT_PRETTY
from sqlspec.driver._sql_helpers import convert_to_dialect as _convert_to_dialect_impl
from sqlspec.driver._storage_helpers import (
    arrow_table_to_rows,
    attach_partition_telemetry,
    build_ingest_telemetry,
    coerce_arrow_table,
    create_storage_job,
    stringify_storage_target,
)
from sqlspec.exceptions import ImproperConfigurationError, StackExecutionError
from sqlspec.storage import AsyncStoragePipeline, StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
from sqlspec.utils.arrow_impl import convert_dict_to_arrow_with_schema
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlglot.dialects.dialect import DialectType

    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, SQLResult, StatementConfig, StatementFilter
    from sqlspec.driver._common import ForeignKeyMetadata
    from sqlspec.typing import ArrowReturnFormat, ArrowTable, SchemaT, StatementParameters


__all__ = ("AsyncDataDictionaryBase", "AsyncDriverAdapterBase", "AsyncDriverT")


EMPTY_FILTERS: Final["list[StatementFilter]"] = []
_LOGGER_NAME: Final[str] = "sqlspec"
logger = get_logger(_LOGGER_NAME)

AsyncDriverT = TypeVar("AsyncDriverT", bound="AsyncDriverAdapterBase")


@mypyc_attr(allow_interpreted_subclasses=True)
class AsyncDriverAdapterBase(CommonDriverAttributesMixin):
    """Base class for asynchronous database drivers.

    This class includes flattened storage and SQL translation methods that were
    previously in StorageDriverMixin and SQLTranslatorMixin. The flattening
    eliminates cross-trait attribute access that caused mypyc segmentation faults.
    """

    __slots__ = ()

    dialect: "DialectType | None" = None

    @property
    def is_async(self) -> bool:
        """Return whether the driver executes asynchronously.

        Returns:
            True for async drivers.

        """
        return True

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
        _ = cast("ProcessedState", statement.get_processed_state())
        operation = statement.operation_type
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

        result: SQLResult | None = None
        exc_handler = self.handle_database_exceptions()
        cursor_manager = self.with_cursor(connection)
        cursor: Any | None = None
        exc: Exception | None = None
        exc_handler_entered = False
        cursor_entered = False

        try:
            await exc_handler.__aenter__()
            exc_handler_entered = True
            cursor = await cursor_manager.__aenter__()
            cursor_entered = True
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
        except Exception as err:
            exc = err
        finally:
            if cursor_entered:
                if exc is None:
                    await cursor_manager.__aexit__(None, None, None)
                else:
                    await cursor_manager.__aexit__(type(exc), exc, exc.__traceback__)
            if exc_handler_entered:
                if exc is None:
                    await exc_handler.__aexit__(None, None, None)
                else:
                    await exc_handler.__aexit__(type(exc), exc, exc.__traceback__)

        if exc is not None:
            mapped_exc = exc_handler.pending_exception or exc
            runtime.span_manager.end_span(span, error=mapped_exc)
            runtime.emit_error(mapped_exc, **query_context)
            if exc_handler.pending_exception is not None:
                raise mapped_exc from exc
            raise exc

        if exc_handler.pending_exception is not None:
            mapped_exc = exc_handler.pending_exception
            runtime.span_manager.end_span(span, error=mapped_exc)
            runtime.emit_error(mapped_exc, **query_context)
            raise mapped_exc from None

        assert result is not None  # Guaranteed: no exception means result was assigned

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
            storage_backend=(result.metadata or {}).get("storage_backend"),
            started_at=started,
        )
        return result

    def _connection_in_transaction(self) -> bool:
        """Check if the connection is inside a transaction.

        Each adapter MUST override this method with direct attribute access
        for optimal mypyc performance. Do not use getattr chains.

        Raises:
            NotImplementedError: Always - subclasses must override.

        """
        msg = "Adapters must override _connection_in_transaction()"
        raise NotImplementedError(msg)

    @abstractmethod
    def with_cursor(self, connection: Any) -> Any:
        """Create and return an async context manager for cursor acquisition and cleanup.

        Returns an async context manager that yields a cursor for database operations.
        Concrete implementations handle database-specific cursor creation and cleanup.
        """

    @abstractmethod
    def handle_database_exceptions(self) -> "AsyncExceptionHandler":
        """Handle database-specific exceptions and wrap them appropriately.

        Returns:
            Exception handler with deferred exception pattern for mypyc compatibility.
            The handler stores mapped exceptions in pending_exception rather than
            raising from __aexit__ to avoid ABI boundary violations.

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
        _ = (cursor, statement)
        return None

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
    async def fetch_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SchemaT": ...

    @overload
    async def fetch_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...

    async def fetch_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SchemaT | dict[str, Any]":
        """Execute a select statement and return exactly one row.

        This is an alias for :meth:`select_one` provided for users familiar
        with asyncpg's fetch_one() naming convention.

        Raises an exception if no rows or more than one row is returned.

        See Also:
            select_one(): Primary method with identical behavior

        """
        return await self.select_one(
            statement, *parameters, schema_type=schema_type, statement_config=statement_config, **kwargs
        )

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
    async def fetch_one_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SchemaT | None": ...

    @overload
    async def fetch_one_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "dict[str, Any] | None": ...

    async def fetch_one_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SchemaT | dict[str, Any] | None":
        """Execute a select statement and return at most one row.

        This is an alias for :meth:`select_one_or_none` provided for users familiar
        with asyncpg's fetch_one_or_none() naming convention.

        Returns None if no rows are found.
        Raises an exception if more than one row is returned.

        See Also:
            select_one_or_none(): Primary method with identical behavior

        """
        return await self.select_one_or_none(
            statement, *parameters, schema_type=schema_type, statement_config=statement_config, **kwargs
        )

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

    @overload
    async def fetch(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "list[SchemaT]": ...

    @overload
    async def fetch(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "list[dict[str, Any]]": ...

    async def fetch(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "list[SchemaT] | list[dict[str, Any]]":
        """Execute a select statement and return all rows.

        This is an alias for :meth:`select` provided for users familiar
        with asyncpg's fetch() naming convention.

        See Also:
            select(): Primary method with identical behavior

        """
        return await self.select(
            statement, *parameters, schema_type=schema_type, statement_config=statement_config, **kwargs
        )

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
        if native_only:
            msg = (
                f"Adapter '{self.__class__.__name__}' does not support native Arrow results. "
                f"Use native_only=False to allow conversion path, or switch to an adapter "
                f"with native Arrow support (ADBC, DuckDB, BigQuery)."
            )
            raise ImproperConfigurationError(msg)

        result = await self.execute(statement, *parameters, statement_config=statement_config, **kwargs)

        arrow_data = convert_dict_to_arrow_with_schema(
            result.data, return_format=return_format, batch_size=batch_size, arrow_schema=arrow_schema
        )
        return create_arrow_result(
            statement=result.statement,
            data=arrow_data,
            rows_affected=result.rows_affected,
            last_inserted_id=result.last_inserted_id,
            execution_time=result.execution_time,
            metadata=result.metadata,
        )

    async def fetch_to_arrow(
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

        This is an alias for :meth:`select_to_arrow` provided for users familiar
        with asyncpg's fetch() naming convention.

        See Also:
            select_to_arrow(): Primary method with identical behavior and full documentation

        """
        return await self.select_to_arrow(
            statement,
            *parameters,
            statement_config=statement_config,
            return_format=return_format,
            native_only=native_only,
            batch_size=batch_size,
            arrow_schema=arrow_schema,
            **kwargs,
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

    async def fetch_value(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a select statement and return a single scalar value.

        This is an alias for :meth:`select_value` provided for users familiar
        with asyncpg's fetch_value() naming convention.

        Expects exactly one row with one column.
        Raises an exception if no rows or more than one row/column is returned.

        See Also:
            select_value(): Primary method with identical behavior

        """
        return await self.select_value(statement, *parameters, statement_config=statement_config, **kwargs)

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

    async def fetch_value_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a select statement and return a single scalar value or None.

        This is an alias for :meth:`select_value_or_none` provided for users familiar
        with asyncpg's fetch_value_or_none() naming convention.

        Returns None if no rows are found.
        Expects at most one row with one column.
        Raises an exception if more than one row is returned.

        See Also:
            select_value_or_none(): Primary method with identical behavior

        """
        return await self.select_value_or_none(statement, *parameters, statement_config=statement_config, **kwargs)

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

    @overload
    async def fetch_with_total(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "tuple[list[SchemaT], int]": ...

    @overload
    async def fetch_with_total(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "tuple[list[dict[str, Any]], int]": ...

    async def fetch_with_total(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "tuple[list[SchemaT] | list[dict[str, Any]], int]":
        """Execute a select statement and return both the data and total count.

        This is an alias for :meth:`select_with_total` provided for users familiar
        with asyncpg's fetch() naming convention.

        This method is designed for pagination scenarios where you need both
        the current page of data and the total number of rows that match the query.

        See Also:
            select_with_total(): Primary method with identical behavior and full documentation

        """
        return await self.select_with_total(
            statement, *parameters, schema_type=schema_type, statement_config=statement_config, **kwargs
        )

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

    def convert_to_dialect(
        self, statement: "Statement", to_dialect: "DialectType | None" = None, pretty: bool = DEFAULT_PRETTY
    ) -> str:
        """Convert a statement to a target SQL dialect.

        Args:
            statement: SQL statement to convert.
            to_dialect: Target dialect (defaults to current dialect).
            pretty: Whether to format the output SQL.

        Returns:
            SQL string in target dialect.

        """
        return _convert_to_dialect_impl(statement, self.dialect, to_dialect, pretty)

    def _storage_pipeline(self) -> "AsyncStoragePipeline":
        """Get or create an async storage pipeline.

        Returns:
            AsyncStoragePipeline instance.

        """
        factory = self.storage_pipeline_factory
        if factory is None:
            return AsyncStoragePipeline()
        return cast("AsyncStoragePipeline", factory())

    async def select_to_storage(
        self,
        statement: "SQL | str",
        destination: "StorageDestination",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        partitioner: "dict[str, object] | None" = None,
        format_hint: "StorageFormat | None" = None,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        """Stream a SELECT statement directly into storage.

        Args:
            statement: SQL statement to execute.
            destination: Storage destination path.
            parameters: Query parameters.
            statement_config: Optional statement configuration.
            partitioner: Optional partitioner configuration.
            format_hint: Optional format hint for storage.
            telemetry: Optional telemetry dict to merge.

        Returns:
            StorageBridgeJob with execution telemetry.

        """
        self._raise_storage_not_implemented("select_to_storage")
        raise NotImplementedError

    async def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        """Load Arrow data into the target table.

        Args:
            table: Target table name.
            source: Arrow data source.
            partitioner: Optional partitioner configuration.
            overwrite: Whether to overwrite existing data.

        Returns:
            StorageBridgeJob with execution telemetry.

        Raises:
            NotImplementedError: If not implemented.

        """
        self._raise_storage_not_implemented("load_from_arrow")
        raise NotImplementedError

    async def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        """Load artifacts from storage into the target table.

        Args:
            table: Target table name.
            source: Storage source path.
            file_format: File format of source.
            partitioner: Optional partitioner configuration.
            overwrite: Whether to overwrite existing data.

        Returns:
            StorageBridgeJob with execution telemetry.

        """
        self._raise_storage_not_implemented("load_from_storage")
        raise NotImplementedError

    def stage_artifact(self, request: "dict[str, Any]") -> "dict[str, Any]":
        """Provision staging metadata for adapters that require remote URIs.

        Args:
            request: Staging request configuration.

        Returns:
            Staging metadata dict.

        """
        self._raise_storage_not_implemented("stage_artifact")
        raise NotImplementedError

    def flush_staging_artifacts(self, artifacts: "list[dict[str, Any]]", *, error: Exception | None = None) -> None:
        """Clean up staged artifacts after a job completes.

        Args:
            artifacts: List of staging artifacts to clean up.
            error: Optional error that triggered cleanup.

        """
        if artifacts:
            self._raise_storage_not_implemented("flush_staging_artifacts")

    def get_storage_job(self, job_id: str) -> "StorageBridgeJob | None":
        """Fetch a previously created job handle.

        Args:
            job_id: Job identifier.

        Returns:
            StorageBridgeJob if found, None otherwise.

        """
        return None

    async def _write_result_to_storage_async(
        self,
        result: "ArrowResult",
        destination: "StorageDestination",
        *,
        format_hint: "StorageFormat | None" = None,
        storage_options: "dict[str, Any] | None" = None,
        pipeline: "AsyncStoragePipeline | None" = None,
    ) -> "StorageTelemetry":
        """Write Arrow result to storage with telemetry.

        Args:
            result: Arrow result to write.
            destination: Storage destination.
            format_hint: Optional format hint.
            storage_options: Optional storage options.
            pipeline: Optional storage pipeline.

        Returns:
            StorageTelemetry with write metrics.

        """
        runtime = self.observability
        span = runtime.start_storage_span(
            "write", destination=stringify_storage_target(destination), format_label=format_hint
        )
        try:
            telemetry = await result.write_to_storage_async(
                destination, format_hint=format_hint, storage_options=storage_options, pipeline=pipeline
            )
        except Exception as exc:
            runtime.end_storage_span(span, error=exc)
            raise
        telemetry = runtime.annotate_storage_telemetry(telemetry)
        runtime.end_storage_span(span, telemetry=telemetry)
        return telemetry

    async def _read_arrow_from_storage_async(
        self,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        storage_options: "dict[str, Any] | None" = None,
    ) -> "tuple[ArrowTable, StorageTelemetry]":
        """Read Arrow table from storage with telemetry.

        Args:
            source: Storage source path.
            file_format: File format to read.
            storage_options: Optional storage options.

        Returns:
            Tuple of (ArrowTable, StorageTelemetry).

        """
        runtime = self.observability
        span = runtime.start_storage_span(
            "read", destination=stringify_storage_target(source), format_label=file_format
        )
        pipeline = self._storage_pipeline()
        try:
            table, telemetry = await pipeline.read_arrow_async(
                source, file_format=file_format, storage_options=storage_options
            )
        except Exception as exc:
            runtime.end_storage_span(span, error=exc)
            raise
        telemetry = runtime.annotate_storage_telemetry(telemetry)
        runtime.end_storage_span(span, telemetry=telemetry)
        return table, telemetry

    def _coerce_arrow_table(self, source: "ArrowResult | Any") -> "ArrowTable":
        """Coerce various sources to a PyArrow Table.

        Args:
            source: ArrowResult, PyArrow Table, RecordBatch, or iterable of dicts.

        Returns:
            PyArrow Table.

        """
        return coerce_arrow_table(source)

    @staticmethod
    def _arrow_table_to_rows(
        table: "ArrowTable", columns: "list[str] | None" = None
    ) -> "tuple[list[str], list[tuple[Any, ...]]]":
        """Convert Arrow table to column names and row tuples.

        Args:
            table: Arrow table to convert.
            columns: Optional list of columns to extract.

        Returns:
            Tuple of (column_names, list of row tuples).

        """
        return arrow_table_to_rows(table, columns)

    @staticmethod
    def _build_ingest_telemetry(table: "ArrowTable", *, format_label: str = "arrow") -> "StorageTelemetry":
        """Build telemetry dict from Arrow table statistics.

        Args:
            table: Arrow table to extract statistics from.
            format_label: Format label for telemetry.

        Returns:
            StorageTelemetry dict with row/byte counts.

        """
        return build_ingest_telemetry(table, format_label=format_label)

    def _attach_partition_telemetry(
        self, telemetry: "StorageTelemetry", partitioner: "dict[str, object] | None"
    ) -> None:
        """Attach partitioner info to telemetry dict.

        Args:
            telemetry: Telemetry dict to update.
            partitioner: Partitioner configuration or None.

        """
        attach_partition_telemetry(telemetry, partitioner)

    def _create_storage_job(
        self, produced: "StorageTelemetry", provided: "StorageTelemetry | None" = None, *, status: str = "completed"
    ) -> "StorageBridgeJob":
        """Create a StorageBridgeJob from telemetry data.

        Args:
            produced: Telemetry from the production side of the operation.
            provided: Optional telemetry from the source side.
            status: Job status string.

        Returns:
            StorageBridgeJob instance.

        """
        return create_storage_job(produced, provided, status=status)


@mypyc_attr(allow_interpreted_subclasses=True)
class AsyncDataDictionaryBase(DataDictionaryMixin):
    """Base class for asynchronous data dictionary implementations."""

    __slots__ = ()

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

    async def get_foreign_keys(
        self, driver: "AsyncDriverAdapterBase", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata.

        Args:
            driver: Async database driver instance
            table: Optional table name filter
            schema: Optional schema name filter

        Returns:
            List of foreign key metadata

        """
        _ = driver, table, schema
        return []

    def list_available_features(self) -> "list[str]":
        """List all features that can be checked via get_feature_flag.

        Returns:
            List of feature names this data dictionary supports

        """
        return self.get_default_features()
