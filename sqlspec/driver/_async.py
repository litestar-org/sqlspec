"""Asynchronous driver protocol implementation.

Storage and SQL translation methods are defined directly on ``AsyncDriverAdapterBase``
rather than separate mixins to avoid cross-trait mypyc attribute dispatch issues,
and ``AsyncExceptionHandler`` stores mapped exceptions on ``pending_exception``
rather than raising inside ``__aexit__`` across C-extension ABI boundaries.
"""

import logging
import sys
from abc import abstractmethod
from inspect import isawaitable
from time import perf_counter
from typing import TYPE_CHECKING, Any, ClassVar, Final, Generic, Literal, Self, TypeVar, cast, final, overload

from mypy_extensions import mypyc_attr

from sqlspec.core import SQL, CachedQuery, StackResult, create_arrow_result
from sqlspec.core.result import DMLResult
from sqlspec.core.stack import StackOperation, StatementStack
from sqlspec.data_dictionary import (
    DEFAULT_SYSTEM_METADATA_DOMAINS,
    ColumnProfileMetadata,
    DDLResult,
    MetadataCapabilityProfile,
    MetadataResult,
    MetadataSupport,
    ObjectIdentity,
    ensure_system_metadata_request,
    system_metadata_capabilities_from_domains,
    system_metadata_gated_result,
    unsupported_system_metadata_capability,
)
from sqlspec.driver._common import (
    AsyncExceptionHandler,
    CommonDriverAttributesMixin,
    DataDictionaryDialectMixin,
    DataDictionaryMixin,
    ExecutionResult,
    StackExecutionObserver,
    _raise_database_exception,
    _resolve_fast_params,
    _sqlglot_dialect,
    describe_stack_statement,
    handle_single_row_error,
    validate_savepoint_name,
)
from sqlspec.driver._exception_handler import _run_with_async_exception_handler
from sqlspec.driver._sql_helpers import DEFAULT_PRETTY
from sqlspec.driver._sql_helpers import convert_to_dialect as _convert_to_dialect_impl
from sqlspec.driver._storage_helpers import (
    AsyncWithoutIndexesContext,
    execute_load_from_storage_async,
    execute_staged_load_async,
    stringify_storage_target,
)
from sqlspec.driver._stream import AsyncRowStream, _LazyEagerAsyncRowSource
from sqlspec.exceptions import ImproperConfigurationError, StackExecutionError
from sqlspec.observability import _runtime as observability_runtime
from sqlspec.storage import AsyncStoragePipeline, StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
from sqlspec.utils.arrow_helpers import convert_dict_to_arrow_with_schema
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.schema import ValueT, to_value_type
from sqlspec.utils.type_guards import resolve_row_format

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence
    from types import TracebackType

    from sqlglot.dialects.dialect import DialectType

    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, OperationType, SQLResult, Statement, StatementConfig, StatementFilter
    from sqlspec.data_dictionary import (
        ColumnMetadata,
        ForeignKeyMetadata,
        IndexMetadata,
        SystemMetadataCapability,
        SystemMetadataRequest,
        SystemMetadataResult,
        TableMetadata,
        VersionInfo,
    )
    from sqlspec.typing import ArrowReturnFormat, ArrowTable, SchemaT, StatementParameters


__all__ = ("AsyncDataDictionaryBase", "AsyncDriverAdapterBase", "AsyncPoolConnectionContext", "AsyncPoolSessionFactory")


_LOGGER_NAME: Final[str] = "sqlspec.driver"
logger = get_logger(_LOGGER_NAME)
_AsyncResultT = TypeVar("_AsyncResultT")
_AsyncDriverT = TypeVar("_AsyncDriverT", bound="AsyncDriverAdapterBase")


@mypyc_attr(allow_interpreted_subclasses=True)
class AsyncPoolConnectionContext:
    """Base async connection context using pool acquire/release pattern.

    Subclass per adapter and override ``__aenter__``/``__aexit__`` for
    adapter-specific pool acquisition and release logic.
    """

    __slots__ = ("_config", "_connection", "_pool")

    def __init__(self, config: Any) -> None:
        self._config = config
        self._connection: Any = None
        self._pool: Any = None

    async def __aenter__(self) -> Any:
        if hasattr(self._config, "provide_pool"):
            pool = await self._config.provide_pool()
        else:
            pool = self._config.connection_instance
            if pool is None:
                pool = await self._config.create_pool()
                self._config.connection_instance = pool
        self._pool = pool
        self._connection = await pool.acquire()
        return self._connection

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> "bool | None":
        if self._connection is not None:
            pool = self._pool or getattr(self._config, "connection_instance", None)
            if pool is not None:
                await pool.release(self._connection)
            self._connection = None
            self._pool = None
        return None


@mypyc_attr(allow_interpreted_subclasses=True)
class AsyncPoolSessionFactory:
    """Base async session factory using pool acquire/release pattern.

    Subclass per adapter and override ``acquire_connection``/``release_connection``
    for adapter-specific pool acquisition and release logic.
    """

    __slots__ = ("_config", "_connection", "_pool")

    def __init__(self, config: Any) -> None:
        self._config = config
        self._connection: Any = None
        self._pool: Any = None

    async def acquire_connection(self) -> Any:
        if hasattr(self._config, "provide_pool"):
            pool = await self._config.provide_pool()
        else:
            pool = self._config.connection_instance
            if pool is None:
                pool = await self._config.create_pool()
                self._config.connection_instance = pool
        self._pool = pool
        self._connection = await pool.acquire()
        return self._connection

    async def release_connection(self, _conn: Any, **kwargs: Any) -> None:
        if self._connection is not None:
            pool = self._pool or getattr(self._config, "connection_instance", None)
            if pool is not None:
                await pool.release(self._connection)
            self._connection = None
            self._pool = None


@mypyc_attr(allow_interpreted_subclasses=True)
class AsyncDriverAdapterBase(CommonDriverAttributesMixin):
    """Base execution, transaction, Arrow, and storage adapter for asynchronous database drivers.

    Provides uniform SQL compilation, parameter binding, statement caching, result mapping,
    Apache Arrow fetching, and storage pipeline execution over asynchronous driver connections.
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

    async def set_migration_session_schema(self, schema: str) -> None:
        """Set the default schema for migration SQL when supported.

        Args:
            schema: Schema requested for the current migration session.
        """
        log_with_context(logger, logging.DEBUG, "migration.schema.noop", schema=schema, driver=type(self).__name__)

    async def set_migration_non_transactional_schema(self, schema: str) -> None:
        """Set the default schema for non-transactional migration SQL when supported.

        Args:
            schema: Schema requested for the current migration session.
        """
        await self.set_migration_session_schema(schema)

    async def reset_migration_session_schema(self) -> None:
        """Reset migration schema state after a non-transactional migration."""
        log_with_context(logger, logging.DEBUG, "migration.schema.reset.noop", driver=type(self).__name__)

    async def has_schema(self, schema: str) -> bool:
        """Return whether the schema exists for migration validation.

        Args:
            schema: Schema name to validate.

        Returns:
            True when the adapter does not provide schema validation.
        """
        log_with_context(
            logger, logging.DEBUG, "migration.schema.validation.noop", schema=schema, driver=type(self).__name__
        )
        return True

    @final
    async def dispatch_statement_execution(self, statement: "SQL", connection: "Any") -> "SQLResult":
        """Central execution dispatcher using the Template Method Pattern.

        Args:
            statement: The SQL statement to execute
            connection: The database connection to use

        Returns:
            The result of the SQL execution
        """
        try:
            runtime = self._observability
            compiled_sql, execution_parameters = statement.compile()
            result: SQLResult | None = None

            if runtime is None or runtime.is_idle:
                exc_handler = self.handle_database_exceptions()
                if sys.exc_info()[1] is None:
                    async with exc_handler:
                        result = await self._dispatch_statement_with_cursor(
                            connection, statement, has_execution_parameters=bool(execution_parameters)
                        )
                else:
                    result = await self._run_with_exception_handler(
                        exc_handler,
                        self._dispatch_statement_with_cursor,
                        connection,
                        statement,
                        has_execution_parameters=bool(execution_parameters),
                    )
                self._check_pending_exception(exc_handler)
                assert result is not None
                return result

            return await self._execute_with_observability_async(
                compiled_sql=compiled_sql,
                execution_parameters=execution_parameters,
                operation="SCRIPT" if statement.is_script else statement.operation_type,
                is_many=statement.is_many,
                is_script=statement.is_script,
                runner=self._dispatch_statement_observed_async,
                connection=connection,
                statement=statement,
                has_execution_parameters=bool(execution_parameters),
            )
        finally:
            self._release_pooled_statement(statement)

    @abstractmethod
    async def dispatch_execute(self, cursor: Any, statement: "SQL") -> ExecutionResult:
        """Execute a single SQL statement.

        Must be implemented by each driver for database-specific execution logic.

        Args:
            cursor: Database cursor/connection object
            statement: SQL statement object with all necessary data and configuration

        Returns:
            ExecutionResult with execution data
        """

    @abstractmethod
    async def dispatch_execute_many(self, cursor: Any, statement: "SQL") -> ExecutionResult:
        """Execute SQL with multiple parameter sets (executemany).

        Must be implemented by each driver for database-specific executemany logic.

        Args:
            cursor: Database cursor/connection object
            statement: SQL statement object with all necessary data and configuration

        Returns:
            ExecutionResult with execution data for the many operation
        """

    async def dispatch_execute_script(self, cursor: Any, statement: "SQL") -> ExecutionResult:
        """Execute a SQL script containing multiple statements.

        Default implementation splits the script and executes statements individually.
        Drivers can override for database-specific script execution methods.

        Args:
            cursor: Database cursor/connection object
            statement: SQL statement object with all necessary data and configuration

        Returns:
            ExecutionResult with script execution data including statement counts
        """
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, self.statement_config, strip_trailing_semicolon=True)

        statement_count: int = len(statements)
        successful_count: int = 0
        runtime = self._observability
        has_observers = runtime is not None and runtime.has_statement_observers

        for idx, stmt in enumerate(statements):
            single_stmt = self._sub_statement(stmt, prepared_parameters)
            stmt_started = perf_counter() if has_observers else 0.0
            self._pending_statement_profile = None
            try:
                sub_result = await self.dispatch_execute(cursor, single_stmt)
            finally:
                self._release_pooled_statement(single_stmt)
            if has_observers:
                stmt_duration = perf_counter() - stmt_started
                stmt_profile: dict[str, Any] | None = self._pending_statement_profile
                self._pending_statement_profile = None
                self._emit_script_statement_event(
                    sql=stmt,
                    parameters=prepared_parameters,
                    operation=single_stmt.operation_type,
                    rows_affected=sub_result.rowcount_override,
                    duration_s=stmt_duration,
                    started_at=stmt_started,
                    profile=stmt_profile,
                    script_ordinal=idx,
                    script_statement_count=statement_count,
                )
            successful_count += 1

        self._pending_statement_profile = None
        return self.create_execution_result(
            cursor, statement_count=statement_count, successful_statements=successful_count, is_script_result=True
        )

    async def dispatch_special_handling(self, cursor: Any, statement: "SQL") -> "SQLResult | None":
        """Hook for database-specific special operations.

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

    def collect_rows(self, cursor: Any, fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        """Collect rows from cursor after fetchall for the direct execution path.

        Adapters should override this method to provide optimized row collection
        that bypasses full dispatch_execute overhead.

        Args:
            cursor: Database cursor with description metadata.
            fetched: Rows returned from cursor.fetchall().

        Returns:
            Tuple of (data, column_names, row_count).

        Raises:
            NotImplementedError: If the adapter does not implement this method.
        """
        msg = "Adapter must implement collect_rows() for direct execution path"
        raise NotImplementedError(msg)

    def resolve_rowcount(self, cursor: Any) -> int:
        """Resolve the number of affected rows from cursor for the direct execution path.

        Adapters should override this method to provide optimized rowcount resolution
        that bypasses full dispatch_execute overhead.

        Args:
            cursor: Database cursor with rowcount metadata.

        Returns:
            Number of affected rows, or 0 when unknown.

        Raises:
            NotImplementedError: If the adapter does not implement this method.
        """
        msg = "Adapter must implement resolve_rowcount() for direct execution path"
        raise NotImplementedError(msg)

    @abstractmethod
    async def begin(self) -> None:
        """Begin a database transaction on the current connection."""

    @abstractmethod
    async def commit(self) -> None:
        """Commit the current transaction on the current connection."""

    @abstractmethod
    async def rollback(self) -> None:
        """Rollback the current transaction on the current connection."""

    async def create_savepoint(self, name: str) -> None:
        """Create a savepoint within the current transaction."""
        await self.execute_script(f"SAVEPOINT {validate_savepoint_name(name)}")

    async def release_savepoint(self, name: str) -> None:
        """Release a previously created savepoint."""
        await self.execute_script(f"RELEASE SAVEPOINT {validate_savepoint_name(name)}")

    async def rollback_to_savepoint(self, name: str) -> None:
        """Roll back the current transaction to a previously created savepoint."""
        await self.execute_script(f"ROLLBACK TO SAVEPOINT {validate_savepoint_name(name)}")

    def transaction(self) -> "_AsyncDriverTransaction[Self]":
        """Return a context manager that wraps a block in a transaction.

        Entering the block calls ``begin()`` and yields this driver. A normal exit
        calls ``commit()``; an exception calls ``rollback()`` and propagates. A
        failed commit is followed by a rollback attempt before the commit error
        propagates. When the connection already has an open transaction, the block
        joins it instead of calling ``begin()`` and commits it on exit. Inside
        another ``transaction()`` or service ``begin_transaction()`` block on this
        driver, the block runs in a savepoint instead and leaves the outer
        transaction open.
        Isolation settings are applied with ``execute_script`` inside the block.

        Example:
            .. code-block:: python

                async with session.transaction():
                    await session.execute(
                        "INSERT INTO items (id) VALUES (:id)", id=1
                    )

        Returns:
            A context manager yielding this driver.
        """
        return _AsyncDriverTransaction(self)

    @abstractmethod
    def with_cursor(self, connection: Any) -> Any:
        """Create and return an async context manager for cursor acquisition and cleanup.

        Returns an async context manager that yields a cursor for database operations.
        Concrete implementations handle database-specific cursor creation and cleanup.
        """

    @abstractmethod
    def handle_database_exceptions(self) -> "AsyncExceptionHandler":
        """Return an async context manager that maps driver errors to SQLSpec exceptions.

        Returns:
            Asynchronous exception handler exposing mapped errors via ``pending_exception``.
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
        return await self._execute(statement, parameters, statement_config, kwargs)

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
        return await self._execute_many(statement, parameters, filters, statement_config, kwargs)

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
        return await self._execute_script(statement, parameters, statement_config, kwargs)

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

        Returns None if no rows are found. Raises
        :class:`~sqlspec.exceptions.MultipleResultsFoundError` if more than one row
        is returned. Any database or SQL execution errors raised by the driver are
        propagated unchanged.
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
    async def select_value(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: "type[ValueT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "ValueT": ...
    @overload
    async def select_value(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> Any: ...
    async def select_value(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: "type[ValueT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "ValueT | Any":
        """Execute a select statement and return a single scalar value.

        Expects exactly one row with one column.
        Raises an exception if no rows or more than one row/column is returned.

        Args:
            statement: SQL statement or query builder to execute.
            *parameters: Positional parameters for the statement.
            value_type: Optional type to convert the result to. When provided,
                the return value is converted to this type and the return type
                is narrowed for type checkers. Supports int, float, str, bool,
                datetime, date, time, Decimal, UUID, Path, dict, and list.
            statement_config: Optional statement configuration.
            **kwargs: Additional keyword arguments.

        Returns:
            The scalar value, optionally converted to the specified type
        """
        result = await self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        try:
            value = result.scalar()
        except ValueError as error:
            handle_single_row_error(error)

        if value_type is not None:
            return to_value_type(value, value_type)
        return value

    @overload
    async def fetch_value(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: "type[ValueT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "ValueT": ...
    @overload
    async def fetch_value(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> Any: ...
    async def fetch_value(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: "type[ValueT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "ValueT | Any":
        """Execute a select statement and return a single scalar value.

        This is an alias for :meth:`select_value` provided for users familiar
        with asyncpg's fetch_value() naming convention.

        Expects exactly one row with one column.
        Raises an exception if no rows or more than one row/column is returned.

        See Also:
            select_value(): Primary method with identical behavior
        """
        return await self.select_value(
            statement, *parameters, value_type=value_type, statement_config=statement_config, **kwargs
        )

    @overload
    async def select_value_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: "type[ValueT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "ValueT | None": ...
    @overload
    async def select_value_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> Any: ...
    async def select_value_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: "type[ValueT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "ValueT | Any | None":
        """Execute a select statement and return a single scalar value or None.

        Returns None if no rows are found.
        Expects at most one row with one column.
        Raises an exception if more than one row is returned.

        Args:
            statement: SQL statement or query builder to execute.
            *parameters: Positional parameters for the statement.
            value_type: Optional type to convert the result to. When provided,
                the return value is converted to this type and the return type
                is narrowed to ``T | None`` for type checkers. Supports int, float,
                str, bool, datetime, date, time, Decimal, UUID, Path, dict, and list.
            statement_config: Optional statement configuration.
            **kwargs: Additional keyword arguments.

        Returns:
            The scalar value (optionally converted), or None if no rows found.
        """
        result = await self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        value = result.scalar_or_none()

        if value is None:
            return None
        if value_type is not None:
            return to_value_type(value, value_type)
        return value

    @overload
    async def fetch_value_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: "type[ValueT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "ValueT | None": ...
    @overload
    async def fetch_value_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> Any: ...
    async def fetch_value_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: "type[ValueT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "ValueT | Any | None":
        """Execute a select statement and return a single scalar value or None.

        This is an alias for :meth:`select_value_or_none` provided for users familiar
        with asyncpg's fetch_value_or_none() naming convention.

        Returns None if no rows are found.
        Expects at most one row with one column.
        Raises an exception if more than one row is returned.

        See Also:
            select_value_or_none(): Primary method with identical behavior
        """
        return await self.select_value_or_none(
            statement, *parameters, value_type=value_type, statement_config=statement_config, **kwargs
        )

    @overload
    async def select_with_total(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        count_with_window: bool = False,
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
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> "tuple[list[dict[str, Any]], int]": ...
    async def select_with_total(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        count_with_window: bool = False,
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
            count_with_window: If True, use a single query with COUNT(*) OVER() window
                function instead of two separate queries. This can be more efficient
                for some databases but adds a column to each row. Default False.
            **kwargs: Additional keyword arguments

        Returns:
        A tuple containing:
            - List of data rows (transformed by schema_type if provided)
            - Total count of rows matching the query (ignoring LIMIT/OFFSET)
        """
        sql_statement = self.prepare_statement(
            statement, parameters, statement_config=statement_config or self.statement_config, kwargs=kwargs
        )

        if count_with_window:
            modified_sql = self._with_total_count(sql_statement)
            result = await self.execute(modified_sql)
            rows = result.all()
            data, total = self._extract_total_from_rows(rows)

            if schema_type is not None:
                return (cast("list[SchemaT]", self.to_schema(data, schema_type=schema_type)), total)
            return (data, total)

        count_result = await self.execute(self._count_query(sql_statement))
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
        count_with_window: bool = False,
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
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> "tuple[list[dict[str, Any]], int]": ...
    async def fetch_with_total(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        count_with_window: bool = False,
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
            statement,
            *parameters,
            schema_type=schema_type,
            statement_config=statement_config,
            count_with_window=count_with_window,
            **kwargs,
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
            result.get_data(),
            return_format=return_format,
            batch_size=batch_size,
            arrow_schema=arrow_schema,
            column_types=result.get_metadata("column_types"),
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

    @overload
    def select_stream(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> "AsyncRowStream[SchemaT]": ...
    @overload
    def select_stream(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> "AsyncRowStream[dict[str, Any]]": ...
    def select_stream(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> "AsyncRowStream[SchemaT] | AsyncRowStream[dict[str, Any]]":
        """Execute a query and stream rows in chunks."""
        if chunk_size < 1:
            msg = "chunk_size must be greater than or equal to 1"
            raise ValueError(msg)
        sql_statement = self.prepare_statement(
            statement, parameters, statement_config=statement_config or self.statement_config, kwargs=kwargs
        )
        stream = self.dispatch_select_stream(sql_statement, chunk_size)
        if stream is not None:
            return stream._with_schema_type(schema_type)
        if native_only:
            msg = (
                f"Adapter '{type(self).__name__}' does not support native row streaming. "
                "Use native_only=False to allow eager fallback (not bounded-memory), "
                "or switch to an adapter with native row streaming support."
            )
            raise ImproperConfigurationError(msg)
        return AsyncRowStream(_LazyEagerAsyncRowSource(self, sql_statement, chunk_size), schema_type=schema_type)

    @overload
    def fetch_stream(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> "AsyncRowStream[SchemaT]": ...
    @overload
    def fetch_stream(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> "AsyncRowStream[dict[str, Any]]": ...
    def fetch_stream(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> "AsyncRowStream[SchemaT] | AsyncRowStream[dict[str, Any]]":
        """Execute a query and stream rows in chunks.

        This is an alias for :meth:`select_stream` provided for users familiar
        with asyncpg's fetch() naming convention.

        See Also:
            select_stream(): Primary method with identical behavior and full documentation
        """
        return self.select_stream(
            statement,
            *parameters,
            schema_type=schema_type,
            statement_config=statement_config,
            chunk_size=chunk_size,
            native_only=native_only,
            **kwargs,
        )

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "AsyncRowStream[dict[str, Any]] | None":
        """Adapter hook returning a native row stream, or None when unsupported."""
        _ = (statement, chunk_size)
        return None

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
                caller_in_transaction = self._connection_in_transaction()
            except NotImplementedError:
                caller_in_transaction = False

            try:
                if single_transaction and not caller_in_transaction:
                    await self.begin()
                    started_transaction = True

                for index, operation in enumerate(stack.operations):
                    try:
                        result = await self._execute_stack_operation(operation)
                    except Exception as exc:  # pragma: no cover
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
                            except Exception as rollback_error:  # pragma: no cover
                                logger.debug("Rollback after stack failure failed: %s", rollback_error)
                            started_transaction = False

                        if continue_on_error:
                            if not caller_in_transaction:
                                await self._rollback_failed_stack()
                            observer.record_operation_error(stack_error)
                            results.append(StackResult.from_error(stack_error))
                            continue

                        raise stack_error from exc

                    results.append(StackResult(result=result))

                    if continue_on_error and not caller_in_transaction:
                        await self._commit_stack_success()

                if started_transaction:
                    await self.commit()
            except Exception:
                if started_transaction:
                    try:
                        await self.rollback()
                    except Exception as rollback_error:  # pragma: no cover
                        logger.debug("Rollback after stack failure failed: %s", rollback_error)
                raise

        return tuple(results)

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

    def without_indexes(
        self, table: str, *, schema: str | None = None, concurrent: bool = False
    ) -> AsyncWithoutIndexesContext:
        """Drop secondary indexes on ``table`` before bulk loading and rebuild them on exit."""
        return AsyncWithoutIndexesContext(self, table, schema=schema, concurrent=concurrent)

    async def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
        stage_table: "bool | str | None" = None,
        conflict_keys: "Sequence[str] | None" = None,
        on_conflict: Literal["update", "nothing"] = "update",
        exclude_update_columns: "Sequence[str] | None" = None,
        drop_stage_table: bool = True,
        drop_indexes: bool = False,
        concurrent_indexes: bool = False,
        type_casts: "Mapping[str, str] | None" = None,
    ) -> "StorageBridgeJob":
        """Load Arrow data into the target table.

        Args:
            table: Target table name.
            source: Arrow data source.
            partitioner: Optional partitioner configuration.
            overwrite: Whether to overwrite existing data.
            telemetry: Optional inbound storage telemetry to merge.
            stage_table: Optional staging table flag or explicit staging table name.
            conflict_keys: Optional conflict key columns for server-side merge/upsert.
            on_conflict: Conflict action (``"update"`` or ``"nothing"``).
            exclude_update_columns: Optional columns excluded from ``SET`` updates.
            drop_stage_table: Whether to drop the staging table after merge.
            drop_indexes: Whether to drop and rebuild secondary indexes around the load.
            concurrent_indexes: Whether to rebuild PostgreSQL indexes with ``CONCURRENTLY``.
            type_casts: Optional column-to-SQL-type mapping for stage-to-target casts.

        Returns:
            StorageBridgeJob with execution telemetry.

        Raises:
            NotImplementedError: If not implemented.
        """
        _ = (
            table,
            source,
            partitioner,
            overwrite,
            telemetry,
            stage_table,
            conflict_keys,
            on_conflict,
            exclude_update_columns,
            drop_stage_table,
            drop_indexes,
            concurrent_indexes,
            type_casts,
        )
        self._raise_storage_not_implemented("load_from_arrow")

    async def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        storage_options: "dict[str, Any] | None" = None,
        stage_table: "bool | str | None" = None,
        conflict_keys: "Sequence[str] | None" = None,
        on_conflict: Literal["update", "nothing"] = "update",
        exclude_update_columns: "Sequence[str] | None" = None,
        drop_stage_table: bool = True,
        drop_indexes: bool = False,
        concurrent_indexes: bool = False,
        checkpoint_table: str | None = None,
        checkpoint_phase: str = "default",
        max_workers: int = 1,
        executor: Literal["thread", "process"] = "thread",
        arrow_schema: Any = None,
        type_casts: "Mapping[str, str] | None" = None,
    ) -> "StorageBridgeJob":
        """Load artifacts from storage into the target table.

        Args:
            table: Target table name.
            source: Storage source path, prefix, or glob pattern.
            file_format: File format of source.
            partitioner: Optional partitioner configuration.
            overwrite: Whether to overwrite existing data.
            storage_options: Optional storage backend configuration.
            stage_table: Optional staging table flag or explicit staging table name.
            conflict_keys: Optional conflict key columns for server-side merge/upsert.
            on_conflict: Conflict action (``"update"`` or ``"nothing"``).
            exclude_update_columns: Optional columns excluded from ``SET`` updates.
            drop_stage_table: Whether to drop the staging table after merge.
            drop_indexes: Whether to drop and rebuild secondary indexes around the load.
            concurrent_indexes: Whether to rebuild PostgreSQL indexes with ``CONCURRENTLY``.
            checkpoint_table: Optional database table name for shard-level checkpoint tracking.
            checkpoint_phase: Checkpoint phase identifier (default ``"default"``).
            max_workers: Number of parallel workers for multi-shard storage fan-out.
            executor: Worker pool type (``"thread"`` or ``"process"``).
            arrow_schema: Optional PyArrow schema for explicit type pinning on decode.
            type_casts: Optional column-to-SQL-type mapping for stage-to-target casts.

        Returns:
            StorageBridgeJob with execution telemetry.
        """
        if type(self).load_from_arrow is AsyncDriverAdapterBase.load_from_arrow:
            self._raise_storage_not_implemented("load_from_storage")
        return await execute_load_from_storage_async(
            self,
            table,
            source,
            file_format=file_format,
            partitioner=partitioner,
            overwrite=overwrite,
            storage_options=storage_options,
            stage_table=stage_table,
            conflict_keys=conflict_keys,
            on_conflict=on_conflict,
            exclude_update_columns=exclude_update_columns,
            drop_stage_table=drop_stage_table,
            drop_indexes=drop_indexes,
            concurrent_indexes=concurrent_indexes,
            checkpoint_table=checkpoint_table,
            checkpoint_phase=checkpoint_phase,
            max_workers=max_workers,
            executor=executor,
            arrow_schema=arrow_schema,
            type_casts=type_casts,
        )

    async def load_from_records(
        self,
        table: str,
        records: "Sequence[Mapping[str, Any]] | Sequence[Sequence[Any]]",
        *,
        columns: "list[str] | None" = None,
        overwrite: bool = False,
        stage_table: "bool | str | None" = None,
        conflict_keys: "Sequence[str] | None" = None,
        on_conflict: Literal["update", "nothing"] = "update",
        exclude_update_columns: "Sequence[str] | None" = None,
        drop_stage_table: bool = True,
        drop_indexes: bool = False,
        concurrent_indexes: bool = False,
        type_casts: "Mapping[str, str] | None" = None,
    ) -> "StorageBridgeJob":
        """Load in-memory dict or positional records into the target table.

        Records are normalized into an Arrow table and routed through the
        adapter's native ``load_from_arrow`` path (COPY, executemany, mutations,
        etc.). Dict records derive their columns from the keys; positional
        records require ``columns``.

        Args:
            table: Target table name.
            records: Mapping records, or positional sequences with ``columns``.
            columns: Column names (required for positional records).
            overwrite: Whether to overwrite existing data.
            stage_table: Optional staging table flag or explicit staging table name.
            conflict_keys: Optional conflict key columns for server-side merge/upsert.
            on_conflict: Conflict action (``"update"`` or ``"nothing"``).
            exclude_update_columns: Optional columns excluded from ``SET`` updates.
            drop_stage_table: Whether to drop the staging table after merge.
            drop_indexes: Whether to drop and rebuild secondary indexes around the load.
            concurrent_indexes: Whether to rebuild PostgreSQL indexes with ``CONCURRENTLY``.
            type_casts: Optional column-to-SQL-type mapping for stage-to-target casts.

        Returns:
            StorageBridgeJob with execution telemetry.
        """
        prepared_records = self._prepare_records_for_arrow(records)
        arrow_table = self._records_to_arrow_table(prepared_records, columns)
        if stage_table or conflict_keys or drop_indexes:
            return await execute_staged_load_async(
                self,
                table,
                arrow_table,
                raw_loader=self.load_from_arrow,
                overwrite=overwrite,
                stage_table=stage_table,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                drop_stage_table=drop_stage_table,
                drop_indexes=drop_indexes,
                concurrent_indexes=concurrent_indexes,
                type_casts=type_casts,
            )
        return await self.load_from_arrow(table, arrow_table, overwrite=overwrite)

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

    @staticmethod
    def _check_pending_exception(exc_handler: AsyncExceptionHandler) -> None:
        """Raise any pending mapped exception after context manager exit."""
        pending = exc_handler.pending_exception
        if pending is not None:
            if pending.__cause__ is not None:
                raise pending
            raise pending from None

    async def _run_with_exception_handler(
        self,
        exc_handler: AsyncExceptionHandler,
        operation: "Callable[..., Awaitable[_AsyncResultT]]",
        *args: Any,
        **kwargs: Any,
    ) -> "_AsyncResultT | None":
        """Run an async operation without inheriting the caller's active exception state."""
        return await _run_with_async_exception_handler(exc_handler, operation, *args, **kwargs)

    async def _execute_with_observability_async(
        self,
        *,
        compiled_sql: str,
        execution_parameters: Any,
        operation: "OperationType",
        is_many: bool,
        is_script: bool,
        runner: "Callable[..., Awaitable[SQLResult]]",
        **runner_kwargs: Any,
    ) -> "SQLResult":
        """Execute an async runner while emitting spans, lifecycle hooks, and statement events."""
        runtime = self._observability
        if runtime is None or runtime.is_idle:
            return await runner(**runner_kwargs)
        driver_name = type(self).__name__
        query_context = {
            "sql": compiled_sql,
            "parameters": execution_parameters,
            "driver": driver_name,
            "operation": operation,
            "is_many": is_many,
            "is_script": is_script,
        }
        await runtime.emit_query_start_async(**query_context)
        sql_hash = None
        if runtime.span_manager.is_enabled or (
            runtime.has_statement_observers and runtime.config.logging and runtime.config.logging.include_sql_hash
        ):
            sql_hash = observability_runtime.compute_sql_hash(compiled_sql)
        span = runtime.start_query_span(compiled_sql, operation, driver_name, sql_hash=sql_hash)
        started = perf_counter()
        self._pending_statement_profile = None
        try:
            result = await runner(**runner_kwargs)
        except Exception as exc:
            self._pending_statement_profile = None
            runtime.span_manager.end_span(span, error=exc)
            await runtime.emit_error_async(exc, **query_context)
            raise
        profile: dict[str, Any] | None = self._pending_statement_profile
        self._pending_statement_profile = None
        runtime.span_manager.end_span(span)
        duration = perf_counter() - started
        await runtime.emit_query_complete_async(**{**query_context, "rows_affected": result.rows_affected})
        runtime.emit_statement_event(
            sql=compiled_sql,
            parameters=execution_parameters,
            driver=driver_name,
            operation=operation,
            execution_mode=self.statement_config.execution_mode,
            is_many=is_many,
            is_script=is_script,
            rows_affected=result.rows_affected,
            duration_s=duration,
            storage_backend=(result.metadata or {}).get("storage_backend"),
            started_at=started,
            precomputed_sql_hash=sql_hash,
            profile=profile,
        )
        return result

    async def _dispatch_statement_observed_async(
        self, connection: Any, statement: "SQL", *, has_execution_parameters: bool
    ) -> "SQLResult":
        """Execute an async statement inside an exception handler and raise any mapped error."""
        exc_handler = self.handle_database_exceptions()
        result: SQLResult | None = None
        try:
            if sys.exc_info()[1] is None:
                async with exc_handler:
                    result = await self._dispatch_statement_with_cursor(
                        connection, statement, has_execution_parameters=has_execution_parameters
                    )
            else:
                result = await self._run_with_exception_handler(
                    exc_handler,
                    self._dispatch_statement_with_cursor,
                    connection,
                    statement,
                    has_execution_parameters=has_execution_parameters,
                )
        except Exception as exc:  # pragma: no cover
            _raise_database_exception(exc_handler, exc)
        if exc_handler.pending_exception is not None:
            _raise_database_exception(exc_handler, None)
        assert result is not None
        return result

    async def _execute_cache_hit(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: CachedQuery
    ) -> "SQLResult":
        """Execute pre-compiled query via ultra-fast path (async).

        Uses DBAPI-style async cursor methods when available. Async adapters with
        connection-level execution APIs fall back to dispatch_execute so they can
        preserve adapter-specific parameter binding semantics. For DML operations,
        returns DMLResult to bypass full SQLResult construction.

        Args:
            sql: Raw SQL string (original, not compiled).
            params: Query parameters.
            cached: Cached query metadata.

        Returns:
            SQLResult or DMLResult.
        """
        exc_handler = self.handle_database_exceptions()
        result: SQLResult | None = None
        if sys.exc_info()[1] is None:
            async with exc_handler:
                result = await self._execute_cache_hit_with_cursor(sql, params, cached)
        else:
            result = await self._run_with_exception_handler(
                exc_handler, self._execute_cache_hit_with_cursor, sql, params, cached
            )
        self._check_pending_exception(exc_handler)
        assert result is not None
        return result

    async def _execute_cache_hit_observed(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: CachedQuery
    ) -> "SQLResult":
        """Execute an async fast-path cache hit wrapped in observability instrumentation."""
        obs_params = list(params) if isinstance(params, tuple) else params
        return await self._execute_with_observability_async(
            compiled_sql=self._format_wire_sql(cached.compiled_sql),
            execution_parameters=obs_params,
            operation=cached.operation_type,
            is_many=False,
            is_script=False,
            runner=self._execute_cache_hit,
            sql=sql,
            params=params,
            cached=cached,
        )

    async def _cached_execution(
        self, statement: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]"
    ) -> "SQLResult | None":
        """Attempt fast-path execution for cached query (async).

        Args:
            statement: Raw SQL string.
            params: Query parameters.

        Returns:
            SQLResult if cache hit and execution succeeds, None otherwise.
        """
        result = super()._cached_execution(statement, params)
        if result is None:
            return None
        return await cast("Awaitable[SQLResult | None]", result)

    async def _execute_cached_statement(self, statement: "SQL") -> "SQLResult":
        """Execute pre-compiled query via fast path (async).

        The statement is already compiled by _cached_execution, so dispatch_execute
        will hit the fast path in _compiled_statement (is_processed check).
        """
        exc_handler = self.handle_database_exceptions()
        result: SQLResult | None = None
        try:
            if sys.exc_info()[1] is None:
                async with exc_handler:
                    result = await self._execute_cached_statement_with_cursor(statement)
            else:
                result = await self._run_with_exception_handler(
                    exc_handler, self._execute_cached_statement_with_cursor, statement
                )

            self._check_pending_exception(exc_handler)
            assert result is not None
            return result
        finally:
            self._release_pooled_statement(statement)

    async def _execute_cached_statement_observed(
        self, statement: "SQL", compiled_sql: str, execution_parameters: Any, operation: "OperationType"
    ) -> "SQLResult":
        """Execute a rebound cached async statement wrapped in observability instrumentation."""
        obs_params = list(execution_parameters) if isinstance(execution_parameters, tuple) else execution_parameters
        return await self._execute_with_observability_async(
            compiled_sql=compiled_sql,
            execution_parameters=obs_params,
            operation=operation,
            is_many=False,
            is_script=False,
            runner=self._execute_cached_statement,
            statement=statement,
        )

    async def _execute(
        self,
        statement: "SQL | Statement | QueryBuilder",
        parameters: "tuple[StatementParameters | StatementFilter, ...]",
        statement_config: "StatementConfig | None",
        kwargs: "dict[str, Any]",
    ) -> "SQLResult":
        fast_params = _resolve_fast_params(parameters, kwargs)
        if (
            self._stmt_cache_enabled
            and (statement_config is None or statement_config is self.statement_config)
            and isinstance(statement, str)
            and fast_params is not None
        ):
            fast_result = await self._cached_execution(statement, fast_params)
            if fast_result is not None:
                return fast_result
        sql_statement = self.prepare_statement(
            statement, parameters, statement_config=statement_config or self.statement_config, kwargs=kwargs
        )
        return await self.dispatch_statement_execution(statement=sql_statement, connection=self.connection)

    async def _execute_many(
        self,
        statement: "SQL | Statement | QueryBuilder",
        parameters: "Sequence[StatementParameters]",
        filters: "tuple[StatementParameters | StatementFilter, ...]",
        statement_config: "StatementConfig | None",
        kwargs: "dict[str, Any]",
    ) -> "SQLResult":
        config = statement_config or self.statement_config
        if isinstance(statement, str) and not filters and not kwargs:
            sql_statement = SQL(statement, parameters, statement_config=config, is_many=True)
        elif isinstance(statement, SQL):
            statement_seed = statement.raw_expression or statement.raw_sql
            sql_statement = SQL(statement_seed, parameters, statement_config=config, is_many=True, **kwargs)
        else:
            base_statement = self.prepare_statement(statement, filters, statement_config=config, kwargs=kwargs)
            statement_seed = base_statement.raw_expression or base_statement.raw_sql
            sql_statement = SQL(statement_seed, parameters, statement_config=config, is_many=True, **kwargs)
        return await self.dispatch_statement_execution(statement=sql_statement, connection=self.connection)

    async def _execute_script(
        self,
        statement: "str | SQL",
        parameters: "tuple[StatementParameters | StatementFilter, ...]",
        statement_config: "StatementConfig | None",
        kwargs: "dict[str, Any]",
    ) -> "SQLResult":
        config = statement_config or self.statement_config
        sql_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
        return await self.dispatch_statement_execution(statement=sql_statement.as_script(), connection=self.connection)

    def _connection_in_transaction(self) -> bool:
        """Check if the connection is inside a transaction.

        Each adapter MUST override this method with direct attribute access
        for optimal mypyc performance. Do not use getattr chains.

        Raises:
            NotImplementedError: Always - subclasses must override.
        """
        msg = "Adapters must override _connection_in_transaction()"
        raise NotImplementedError(msg)

    async def _dispatch_statement_with_cursor(
        self, connection: Any, statement: "SQL", *, has_execution_parameters: bool
    ) -> "SQLResult":
        """Execute a statement while owning only the cursor context."""
        cursor_manager = self.with_cursor(connection)
        cursor_entered = False
        exit_suppressed = False
        error: Exception | None = None
        execution_result: ExecutionResult | None = None
        result: SQLResult | None = None
        try:
            cursor = await cursor_manager.__aenter__()
            cursor_entered = True
            special_result = await self.dispatch_special_handling(cursor, statement)
            if special_result is not None:
                result = special_result
            elif statement.is_script:
                execution_result = await self.dispatch_execute_script(cursor, statement)
            elif statement.is_many:
                if has_execution_parameters:
                    execution_result = await self.dispatch_execute_many(cursor, statement)
                else:
                    execution_result = self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
            else:
                execution_result = await self.dispatch_execute(cursor, statement)
            if special_result is None:
                assert execution_result is not None
                result = self.build_statement_result(statement, execution_result)
        except Exception as exc:
            error = exc
        finally:
            if cursor_entered:
                if error is None:
                    await cursor_manager.__aexit__(None, None, None)
                else:
                    exit_suppressed = bool(await cursor_manager.__aexit__(type(error), error, error.__traceback__))
        if error is not None and not exit_suppressed:
            raise error
        assert result is not None
        return result

    async def _execute_cache_hit_with_cursor(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: CachedQuery
    ) -> "SQLResult":
        """Execute a cached query while owning only the cursor context."""
        direct_statement: SQL | None = None
        result: SQLResult | None = None
        try:
            async with self.with_cursor(self.connection) as cursor:
                wire_sql = self._format_wire_sql(cached.compiled_sql)
                returns_rows = cached.operation_profile.returns_rows
                execute = getattr(cursor, "execute", None) if cursor is not self.connection else None
                fetchall = getattr(cursor, "fetchall", None) if execute is not None and returns_rows else None
                can_use_cursor_fast_path = execute is not None and (
                    (returns_rows and fetchall is not None) or (not returns_rows and hasattr(cursor, "rowcount"))
                )
                if can_use_cursor_fast_path:
                    assert execute is not None
                    execute_result = execute(wire_sql, params) if params else execute(wire_sql)
                    if isawaitable(execute_result):
                        await execute_result
                    if returns_rows:
                        assert fetchall is not None
                        fetched_data = fetchall()
                        if isawaitable(fetched_data):
                            fetched_data = await fetched_data
                        data, column_names, row_count = self.collect_rows(cursor, fetched_data)
                        result = self.build_cached_select_result(
                            sql,
                            params,
                            cached,
                            selected_data=data,
                            column_names=column_names,
                            data_row_count=row_count,
                            row_format=resolve_row_format(data),
                            compiled_sql=wire_sql,
                        )
                    else:
                        affected_rows = self.resolve_rowcount(cursor)
                        result = DMLResult(cached.operation_type, affected_rows)
                else:
                    direct_statement = self._cached_statement(
                        sql, params, cached, params, params_are_simple=True, compiled_sql=wire_sql
                    )
                    execution_result = await self.dispatch_execute(cursor, direct_statement)
                    if returns_rows:
                        result = self.build_statement_result(direct_statement, execution_result)
                    else:
                        affected_rows = (
                            execution_result.rowcount_override
                            if execution_result.rowcount_override is not None
                            and execution_result.rowcount_override >= 0
                            else 0
                        )
                        result = DMLResult(cached.operation_type, affected_rows)
            assert result is not None
            return result
        finally:
            if direct_statement is not None:
                self._release_pooled_statement(direct_statement)

    async def _execute_cached_statement_with_cursor(self, statement: "SQL") -> "SQLResult":
        """Execute a prepared statement while owning only the cursor context."""
        async with self.with_cursor(self.connection) as cursor:
            execution_result = await self.dispatch_execute(cursor, statement)
            return self.build_statement_result(statement, execution_result)

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

    async def _rollback_failed_stack(self) -> None:
        """Attempt to rollback after a stack operation error (async)."""
        try:
            await self.rollback()
        except Exception as rollback_error:  # pragma: no cover
            logger.debug("Rollback after stack error failed: %s", rollback_error)

    async def _commit_stack_success(self) -> None:
        """Attempt to commit after a successful stack operation when not batching (async)."""
        try:
            await self.commit()
        except Exception as commit_error:  # pragma: no cover
            logger.debug("Commit after stack operation failed: %s", commit_error)

    def _storage_pipeline(self) -> "AsyncStoragePipeline":
        """Get or create an async storage pipeline.

        Returns:
            AsyncStoragePipeline instance.
        """
        factory = self.storage_pipeline_factory
        if factory is None:
            return AsyncStoragePipeline()
        return cast("AsyncStoragePipeline", factory())

    async def _write_storage_result(
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

    async def _read_storage_arrow(
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


@mypyc_attr(allow_interpreted_subclasses=True)
class AsyncDataDictionaryBase(DataDictionaryDialectMixin, DataDictionaryMixin):
    """Base class for asynchronous data dictionary implementations.

    Uses Python-compatible class layouts for cross-module inheritance.
    Child classes define dialect as a class attribute.
    """

    dialect: "ClassVar[str]"
    """Dialect identifier. Must be defined by subclasses as a class attribute."""

    def __init__(self) -> None:
        self._version_cache: dict[int, VersionInfo | None] = {}
        self._version_fetch_attempted: set[int] = set()

    async def get_metadata_capabilities(
        self, driver: Any, domains: "Sequence[str] | None" = None
    ) -> "MetadataCapabilityProfile":
        """Get data-dictionary capability profile.

        Args:
            driver: Async database driver instance.
            domains: Optional metadata domains to report.

        Returns:
            Capability profile. Base implementations report requested domains as unsupported.
        """
        requested_domains: tuple[str, ...] = (
            "schemas",
            "objects",
            "tables",
            "columns",
            "constraints",
            "indexes",
            "views",
            "routines",
            "privileges",
            "dependencies",
            "ddl",
        )
        if domains is not None:
            requested_domains = tuple(domains)
        return MetadataCapabilityProfile.from_domains(self.dialect, type(self).__name__, requested_domains)

    async def get_system_metadata_capabilities(
        self, driver: Any, domains: "Sequence[str] | None" = None
    ) -> "tuple[SystemMetadataCapability, ...]":
        """Get opt-in system metadata capability disclosures.

        Args:
            driver: Async database driver instance.
            domains: Optional system metadata domains to report.

        Returns:
            System metadata capabilities. Base implementations report requested domains as unsupported.
        """
        requested_domains = DEFAULT_SYSTEM_METADATA_DOMAINS if domains is None else tuple(domains)
        return system_metadata_capabilities_from_domains(requested_domains)

    async def get_schemas(self, driver: Any) -> "MetadataResult":
        """Get schema metadata or an unsupported-domain result."""
        return self._unsupported_metadata("schemas")

    async def get_objects(self, driver: Any, schema: "str | None" = None) -> "MetadataResult":
        """Get database object metadata or an unsupported-domain result."""
        return self._unsupported_metadata("objects")

    async def get_table_details(self, driver: Any, table: str, schema: "str | None" = None) -> "MetadataResult":
        """Get rich table metadata or an unsupported-domain result."""
        return self._unsupported_metadata("tables")

    async def get_constraints(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "MetadataResult":
        """Get constraint metadata or an unsupported-domain result."""
        return self._unsupported_metadata("constraints")

    async def get_views(self, driver: Any, schema: "str | None" = None) -> "MetadataResult":
        """Get view metadata or an unsupported-domain result."""
        return self._unsupported_metadata("views")

    async def get_routines(self, driver: Any, schema: "str | None" = None) -> "MetadataResult":
        """Get routine metadata or an unsupported-domain result."""
        return self._unsupported_metadata("routines")

    async def get_privileges(
        self, driver: Any, object_name: "str | None" = None, schema: "str | None" = None
    ) -> "MetadataResult":
        """Get privilege metadata or an unsupported-domain result."""
        return self._unsupported_metadata("privileges")

    async def get_dependencies(
        self, driver: Any, object_name: "str | None" = None, schema: "str | None" = None
    ) -> "MetadataResult":
        """Get dependency metadata or an unsupported-domain result."""
        return self._unsupported_metadata("dependencies")

    async def get_sequences(self, driver: Any, schema: "str | None" = None) -> "MetadataResult":
        """Get sequence metadata or an unsupported-domain result."""
        return self._unsupported_metadata("sequences")

    async def get_types(self, driver: Any, schema: "str | None" = None) -> "MetadataResult":
        """Get user-defined type metadata or an unsupported-domain result."""
        return self._unsupported_metadata("types")

    async def get_ddl(
        self,
        driver: Any,
        object_name: str,
        schema: "str | None" = None,
        *,
        object_type: str = "table",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
        target_dialect: "str | None" = None,
    ) -> "DDLResult":
        """Get object DDL or an explicit unsupported DDL result.

        Public alias of :meth:`get_object_ddl` that accepts ``schema`` positionally.
        """
        return await self.get_object_ddl(
            driver,
            object_name,
            schema=schema,
            object_type=object_type,
            include_dependencies=include_dependencies,
            prefer_native=prefer_native,
            redact=redact,
            target_dialect=target_dialect,
        )

    async def get_object_ddl(
        self,
        driver: Any,
        object_name: str,
        *,
        schema: "str | None" = None,
        object_type: str = "table",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
        target_dialect: "str | None" = None,
    ) -> "DDLResult":
        """Get one object's DDL or an explicit unsupported DDL result.

        ``object_name`` and ``schema`` are caller input and follow the dialect's identifier
        normalization (quote a name to match it exactly). Native catalog DDL is preferred; table
        DDL is synthesized from catalog metadata when ``prefer_native`` is False, when a
        ``target_dialect`` is requested, or when native DDL is unavailable.

        Args:
            driver: Database driver instance.
            object_name: Object name.
            schema: Schema name, or None for the dialect default.
            object_type: Object kind such as ``table``, ``view``, or ``sequence``.
            include_dependencies: Whether native helpers should report dependencies.
            prefer_native: Prefer native catalog DDL over synthesized DDL.
            redact: Whether native helpers should redact sensitive values.
            target_dialect: Dialect to render or transpile the DDL into.

        Returns:
            DDL result for the object.
        """
        resolved_name, resolved_schema = self.resolve_ddl_object_name(driver, object_name, schema)
        return await self._get_exact_object_ddl(
            driver,
            resolved_name,
            schema=resolved_schema,
            object_type=object_type,
            include_dependencies=include_dependencies,
            prefer_native=prefer_native,
            redact=redact,
            target_dialect=target_dialect,
        )

    async def get_schema_ddl(
        self,
        driver: Any,
        schema: "str | None" = None,
        *,
        include_domains: "Sequence[str] | None" = None,
        exclude_domains: "Sequence[str] | None" = None,
        exclude_tables: "Sequence[str] | None" = (),
        direction: "Literal['create', 'drop']" = "create",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
        target_dialect: "str | None" = None,
    ) -> "MetadataResult":
        """Get schema DDL items in topological dependency order.

        When ``prefer_native`` is set and no ``target_dialect`` is requested, adapters with a
        native schema DDL source supply the objects; tables whose native DDL is not faithful are
        synthesized from catalog metadata. Otherwise types, sequences, tables, and views are
        collected, and tables are synthesized from schema-wide catalog queries. ``direction="drop"``
        only collects object identities and dependency edges; it never synthesizes table DDL.
        Domains that cannot be collected are reported in the result warnings.

        Args:
            driver: Database driver instance.
            schema: Schema name, or None for the dialect default.
            include_domains: Domains to include (``tables``, ``views``, ``sequences``, ``types``, ...).
            exclude_domains: Domains to exclude.
            exclude_tables: Table names to omit, matched as bare, unquoted, case-insensitive names
                with any schema qualification stripped.
            direction: ``create`` for CREATE statements in dependency order, ``drop`` for
                ``DROP ... IF EXISTS`` statements in reverse dependency order.
            include_dependencies: Collect foreign keys and dependencies to order the output.
            prefer_native: Prefer native catalog DDL over synthesized DDL.
            redact: Whether native helpers should redact sensitive values.
            target_dialect: Dialect to render or transpile the DDL into.

        Returns:
            Schema DDL result.
        """
        warnings: list[str] = []
        exact_schema, getter_schema = self._resolve_schema_ddl_input(driver, schema)
        native_items: list[DDLResult] | None = None
        loaded_foreign_keys: list[Any] | None = None
        if prefer_native and target_dialect is None:
            native_items = await self._get_native_schema_ddl(
                driver, exact_schema, include_dependencies=include_dependencies, redact=redact
            )
        if native_items is not None:
            ddl_results = (
                native_items
                if direction == "drop"
                else await self._replace_unfaithful_native_tables(driver, getter_schema, native_items, warnings)
            )
        else:
            ddl_results, loaded_foreign_keys = await self._collect_synthesized_schema_ddl(
                driver,
                getter_schema,
                default_schema=exact_schema,
                domains=self._schema_ddl_collect_domains(include_domains, exclude_domains),
                exclude_tables=exclude_tables,
                direction=direction,
                include_dependencies=include_dependencies,
                prefer_native=prefer_native,
                redact=redact,
                target_dialect=target_dialect,
                warnings=warnings,
            )
        foreign_keys: list[Any] = []
        dependency_items: tuple[Any, ...] = ()
        if include_dependencies:
            if loaded_foreign_keys is None:
                loaded_foreign_keys = list(await self.get_foreign_keys(driver, table=None, schema=getter_schema))
            foreign_keys = loaded_foreign_keys
            dependencies = await self.get_dependencies(driver, schema=getter_schema)
            if dependencies.capability.support == MetadataSupport.SUPPORTED:
                dependency_items = dependencies.items
        return self._finalize_schema_ddl_results(
            ddl_results,
            foreign_keys=foreign_keys,
            dependency_items=dependency_items,
            include_domains=include_domains,
            exclude_domains=exclude_domains,
            exclude_tables=exclude_tables,
            direction=direction,
            target_dialect=target_dialect,
            warnings=tuple(warnings),
        )

    async def get_table_profile(
        self,
        driver: Any,
        table: str,
        schema: "str | None" = None,
        *,
        sample_size: int = 1000,
        target_dialect: "str | None" = None,
    ) -> "list[ColumnProfileMetadata]":
        """Profile live table columns and infer refined SQL types from existing data.

        Query failures (for example missing privileges) propagate to the caller.
        """
        resolved_table, resolved_schema = self.resolve_ddl_object_name(driver, table, schema)
        columns = await self.get_columns(
            driver,
            table=self.exact_catalog_identifier(driver, resolved_table),
            schema=self.exact_catalog_identifier(driver, resolved_schema) if resolved_schema is not None else None,
        )
        if not columns:
            return []
        stats_rows = await driver.select(self._build_table_profile_sql(resolved_table, columns, schema=resolved_schema))
        stats_row: Mapping[str, Any] | None = stats_rows[0] if stats_rows else None
        sample_rows: list[Mapping[str, Any]] = list(
            await driver.select(
                self._build_table_sample_sql(resolved_table, schema=resolved_schema, sample_size=sample_size)
            )
        )
        return self._compile_column_profiles(columns, stats_row, sample_rows, target_dialect=target_dialect)

    async def infer_table_ddl_from_data(
        self,
        driver: Any,
        table: str,
        schema: "str | None" = None,
        *,
        target_table: "str | None" = None,
        target_dialect: "str | None" = None,
        sample_size: int = 1000,
        primary_key: "str | Sequence[str] | None" = None,
    ) -> "DDLResult":
        """Synthesize a refined table DDLResult by profiling live table data."""
        profiles = await self.get_table_profile(
            driver, table, schema=schema, sample_size=sample_size, target_dialect=target_dialect
        )
        resolved_table, resolved_schema = self.resolve_ddl_object_name(driver, table, schema)
        if not profiles:
            return self._unsupported_ddl(target_table or resolved_table, schema=resolved_schema, object_type="table")
        return self._compile_profiled_table_ddl(
            resolved_table,
            profiles,
            schema=resolved_schema,
            target_table=target_table,
            target_dialect=target_dialect,
            primary_key=primary_key,
        )

    async def get_system_metadata(
        self, driver: Any, request: "SystemMetadataRequest | str | None" = None, **kwargs: Any
    ) -> "SystemMetadataResult":
        """Get opt-in system metadata or a gated/unsupported result."""
        metadata_request = ensure_system_metadata_request(request, **kwargs)
        capability = unsupported_system_metadata_capability(metadata_request.domain)
        return system_metadata_gated_result(metadata_request, capability)

    @abstractmethod
    async def get_version(self, driver: Any) -> "VersionInfo | None":
        """Get database version information.

        Args:
            driver: Async database driver instance

        Returns:
            Version information or None if detection fails
        """

    @abstractmethod
    async def get_feature_flag(self, driver: Any, feature: str) -> bool:
        """Check if database supports a specific feature.

        Args:
            driver: Async database driver instance
            feature: Feature name to check

        Returns:
            True if feature is supported, False otherwise
        """

    @abstractmethod
    async def get_optimal_type(self, driver: Any, type_category: str) -> str:
        """Get optimal database type for a category.

        Args:
            driver: Async database driver instance
            type_category: Type category

        Returns:
            Database-specific type name
        """

    @abstractmethod
    async def get_tables(self, driver: Any, schema: "str | None" = None) -> "list[TableMetadata]":
        """Get list of tables in schema.

        Args:
            driver: Async database driver instance
            schema: Schema name (None for default)

        Returns:
            List of table metadata dictionaries
        """

    @abstractmethod
    async def get_columns(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema.

        Args:
            driver: Async database driver instance
            table: Table name (None to fetch columns for all tables in schema)
            schema: Schema name (None for default)

        Returns:
            List of column metadata dictionaries
        """

    @abstractmethod
    async def get_indexes(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index information for a table or schema.

        Args:
            driver: Async database driver instance
            table: Table name (None to fetch indexes for all tables in schema)
            schema: Schema name (None for default)

        Returns:
            List of index metadata dictionaries
        """

    @abstractmethod
    async def get_foreign_keys(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata.

        Args:
            driver: Async database driver instance
            table: Optional table name filter
            schema: Optional schema name filter

        Returns:
            List of foreign key metadata
        """

    async def _get_native_object_ddl(
        self,
        driver: Any,
        object_name: str,
        *,
        schema: "str | None",
        object_type: str,
        include_dependencies: bool,
        redact: bool,
    ) -> "DDLResult | None":
        """Return native catalog DDL for one object, or None when the adapter has no native source.

        Adapters with native DDL override this hook. ``object_name`` and ``schema`` are exact
        catalog names and must not be normalized again. Return an unsupported DDLResult with
        warnings when native DDL exists in principle but is unavailable or not faithful for this
        object; tables then fall back to synthesized DDL carrying those warnings.
        """
        return None

    async def _get_native_schema_ddl(
        self, driver: Any, schema: "str | None", *, include_dependencies: bool, redact: bool
    ) -> "list[DDLResult] | None":
        """Return native CREATE DDL for every schema object, or None when the adapter has no native source.

        Adapters with a schema-wide native DDL source override this hook. Returned items may carry
        dependency edges and a ``DDL_PARENT_TABLE_CONTEXT_KEY`` context entry for indexes and
        triggers. Tables returned as unsupported are replaced by synthesized DDL. Filtering,
        exclusion, ordering, transpiling, and DROP generation are handled by the base class.
        """
        return None

    async def _get_exact_object_ddl(
        self,
        driver: Any,
        object_name: str,
        *,
        schema: "str | None",
        object_type: str,
        include_dependencies: bool,
        prefer_native: bool,
        redact: bool,
        target_dialect: "str | None",
    ) -> "DDLResult":
        """Resolve one object's DDL from exact catalog names, choosing native or synthesized DDL."""
        is_table = self._is_table_object_type(object_type)
        if is_table and (not prefer_native or target_dialect is not None):
            return await self._synthesize_table_ddl(driver, object_name, schema=schema, target_dialect=target_dialect)
        native = await self._get_native_object_ddl(
            driver,
            object_name,
            schema=schema,
            object_type=object_type,
            include_dependencies=include_dependencies,
            redact=redact,
        )
        if native is not None and native.status == MetadataSupport.SUPPORTED and native.ddl:
            return self._transpile_ddl_result(native, target_dialect=target_dialect)
        if is_table:
            return await self._synthesize_table_ddl(
                driver,
                object_name,
                schema=schema,
                target_dialect=target_dialect,
                warnings=native.warnings if native is not None else (),
            )
        if native is not None:
            return native
        return self._unsupported_ddl(object_name, schema=schema, object_type=object_type)

    async def _replace_unfaithful_native_tables(
        self, driver: Any, schema: "str | None", items: "list[DDLResult]", warnings: "list[str]"
    ) -> "list[DDLResult]":
        """Replace unsupported native table items with DDL synthesized from schema-wide catalog queries."""
        replace_names = [
            item.identity.name
            for item in items
            if self._is_table_object_type(item.identity.object_type) and item.status != MetadataSupport.SUPPORTED
        ]
        if not replace_names:
            return items
        catalog = await self._load_table_catalog(driver, schema, replace_names, warnings)
        source_dialect = self.resolve_ddl_dialect(driver)
        resolved: list[DDLResult] = []
        replaced_tables: set[tuple[str, str]] = set()
        for item in items:
            if self._is_table_object_type(item.identity.object_type) and item.status != MetadataSupport.SUPPORTED:
                table_schema = (
                    None
                    if schema is None
                    and item.identity.schema is not None
                    and item.identity.schema.lower() == "main"
                    and _sqlglot_dialect(source_dialect) in {"sqlite", "duckdb"}
                    else item.identity.schema
                )
                synthesized = self._synthesize_table_from_catalog(
                    item.identity.name,
                    table_schema,
                    catalog,
                    source_dialect=source_dialect,
                    target_dialect=None,
                    warnings=item.warnings,
                )
                resolved.append(self._with_dependencies(synthesized, item.dependencies))
                replaced_tables.add(((item.identity.schema or "").lower(), item.identity.name.lower()))
                continue
            resolved.append(item)
        return self._replace_synthesized_table_dependents(resolved, replaced_tables)

    async def _collect_synthesized_schema_ddl(
        self,
        driver: Any,
        schema: "str | None",
        *,
        default_schema: "str | None" = None,
        domains: "set[str]",
        exclude_tables: "Sequence[str] | None",
        direction: "Literal['create', 'drop']",
        include_dependencies: bool,
        prefer_native: bool,
        redact: bool,
        target_dialect: "str | None",
        warnings: "list[str]",
    ) -> "tuple[list[DDLResult], list[Any] | None]":
        """Collect types, sequences, tables, and views for schema DDL.

        Tables are synthesized from one schema-wide query per catalog domain. For ``drop`` only
        table identities are collected. Returns the DDL results plus the schema foreign keys when
        they were already loaded for every table, so callers can reuse them for ordering.
        """
        resolved_default_schema = default_schema if default_schema is not None else schema
        source_dialect = self.resolve_ddl_dialect(driver)
        excluded_names = self._normalize_excluded_table_names(exclude_tables)
        results: list[DDLResult] = []
        loaded_foreign_keys: list[Any] | None = None
        grouped_columns: dict[str, list[Any]] = {}
        if "types" in domains:
            types = await self.get_types(driver, schema=schema)
            if types.capability.support == MetadataSupport.SUPPORTED:
                for row in types.items:
                    type_ddl = self._build_type_ddl_from_row(
                        row, default_schema=resolved_default_schema, dialect=source_dialect
                    )
                    if type_ddl is not None:
                        results.append(type_ddl)
            else:
                warnings.append(self._uncollected_domain_warning("types", source_dialect))
        if "sequences" in domains:
            sequences = await self.get_sequences(driver, schema=schema)
            if sequences.capability.support == MetadataSupport.SUPPORTED:
                for row in sequences.items:
                    sequence_ddl = self._build_sequence_ddl_from_row(
                        row, default_schema=resolved_default_schema, dialect=source_dialect
                    )
                    if sequence_ddl is None:
                        continue
                    if direction == "create" and prefer_native and target_dialect is None:
                        native_sequence = await self._get_native_object_ddl(
                            driver,
                            sequence_ddl.identity.name,
                            schema=sequence_ddl.identity.schema,
                            object_type="sequence",
                            include_dependencies=include_dependencies,
                            redact=redact,
                        )
                        if native_sequence is not None and native_sequence.status == MetadataSupport.SUPPORTED:
                            sequence_ddl = native_sequence
                    results.append(sequence_ddl)
            else:
                warnings.append(self._uncollected_domain_warning("sequences", source_dialect))
        if "tables" in domains:
            table_results, grouped_columns, loaded_foreign_keys = await self._collect_schema_table_ddl(
                driver,
                schema,
                default_schema=resolved_default_schema,
                excluded_names=excluded_names,
                direction=direction,
                include_dependencies=include_dependencies,
                prefer_native=prefer_native,
                redact=redact,
                target_dialect=target_dialect,
                source_dialect=source_dialect,
                warnings=warnings,
            )
            results.extend(table_results)
        if "views" in domains:
            views = await self.get_views(driver, schema=schema)
            if views.capability.support == MetadataSupport.SUPPORTED:
                for view_row in views.items:
                    view_ddl = self._build_view_ddl_from_row(
                        view_row, default_schema=resolved_default_schema, dialect=source_dialect
                    )
                    if view_ddl is None:
                        warnings.append(
                            f"Omitted a view from schema DDL because its definition is unavailable: {view_row!r}"
                        )
                        continue
                    results.append(view_ddl)
            else:
                warnings.append(self._uncollected_domain_warning("views", source_dialect))
        return (
            self._link_schema_references(results, self._column_reference_text(grouped_columns)),
            loaded_foreign_keys,
        )

    async def _collect_schema_table_ddl(
        self,
        driver: Any,
        schema: "str | None",
        *,
        default_schema: "str | None" = None,
        excluded_names: "set[str]",
        direction: "Literal['create', 'drop']",
        include_dependencies: bool,
        prefer_native: bool,
        redact: bool,
        target_dialect: "str | None",
        source_dialect: "str | None",
        warnings: "list[str]",
    ) -> "tuple[list[DDLResult], dict[str, list[Any]], list[Any] | None]":
        """Collect table DDL (or identities for drop) with schema-wide catalog queries.

        Returns the table results, catalog columns grouped by table, and the schema foreign keys
        when they were loaded for every table.
        """
        resolved_default_schema = default_schema if default_schema is not None else schema
        results: list[DDLResult] = []
        loaded_foreign_keys: list[Any] | None = None
        grouped_columns: dict[str, list[Any]] = {}
        table_refs: list[tuple[str, str | None]] = []
        for table_item in await self.get_tables(driver, schema=schema):
            table_name, table_schema = self._extract_table_name_and_schema(
                table_item, default_schema=resolved_default_schema
            )
            if table_name and table_name.lower() not in excluded_names:
                table_refs.append((table_name, table_schema))
        table_names = [name for name, _ in table_refs]
        if direction == "drop":
            columns = await self.get_columns(driver, table=None, schema=schema)
            grouped_columns = self._group_rows_by_table(columns, table_names) or {}
            results.extend(
                self._catalog_object_identity_result(name, "table", table_schema, source_dialect)
                for name, table_schema in table_refs
            )
        else:
            native_tables: dict[str, DDLResult] = {}
            native_warnings: dict[str, tuple[str, ...]] = {}
            if prefer_native and target_dialect is None:
                for name, table_schema in table_refs:
                    native = await self._get_native_object_ddl(
                        driver,
                        name,
                        schema=table_schema,
                        object_type="table",
                        include_dependencies=include_dependencies,
                        redact=redact,
                    )
                    if native is not None and native.status == MetadataSupport.SUPPORTED and native.ddl:
                        native_tables[name] = native
                    elif native is not None:
                        native_warnings[name] = native.warnings
            pending = [name for name in table_names if name not in native_tables]
            catalog: dict[str, dict[str, list[Any]]] = {}
            if pending:
                catalog = await self._load_table_catalog(driver, schema, pending, warnings)
                grouped_columns = catalog["columns"]
                if len(pending) == len(table_names):
                    loaded_foreign_keys = [fk for rows in catalog["foreign_keys"].values() for fk in rows]
            for name, table_schema in table_refs:
                table_result = native_tables.get(name)
                if table_result is None:
                    table_result = self._synthesize_table_from_catalog(
                        name,
                        table_schema,
                        catalog,
                        source_dialect=source_dialect,
                        target_dialect=target_dialect,
                        warnings=native_warnings.get(name, ()),
                    )
                results.append(table_result)
        return (results, grouped_columns, loaded_foreign_keys)

    async def _load_table_catalog(
        self, driver: Any, schema: "str | None", table_names: "list[str]", warnings: "list[str]"
    ) -> "dict[str, dict[str, list[Any]]]":
        """Load columns, constraints, foreign keys, and indexes for ``table_names`` grouped by table.

        Each domain is fetched with one schema-wide call; a domain whose rows do not name their
        table falls back to one call per table.
        """
        columns = await self.get_columns(driver, table=None, schema=schema)
        constraints_result = await self.get_constraints(driver, table=None, schema=schema)
        warnings.extend(constraints_result.warnings)
        constraints = (
            constraints_result.items if constraints_result.capability.support == MetadataSupport.SUPPORTED else ()
        )
        foreign_keys = await self.get_foreign_keys(driver, table=None, schema=schema)
        indexes = await self.get_indexes(driver, table=None, schema=schema)
        catalog: dict[str, dict[str, list[Any]]] = {}
        for domain, rows in (
            ("columns", columns),
            ("constraints", constraints),
            ("foreign_keys", foreign_keys),
            ("indexes", indexes),
        ):
            grouped = self._group_rows_by_table(rows, table_names)
            if grouped is None:
                grouped = {}
                for table_name in table_names:
                    grouped[table_name] = await self._load_table_rows(driver, domain, table_name, schema)
            catalog[domain] = grouped
        return catalog

    async def _load_table_rows(self, driver: Any, domain: str, table_name: str, schema: "str | None") -> "list[Any]":
        """Load one catalog domain for one table using its exact catalog name."""
        lookup_table = self.exact_catalog_identifier(driver, table_name)
        if domain == "columns":
            return list(await self.get_columns(driver, table=lookup_table, schema=schema))
        if domain == "foreign_keys":
            return list(await self.get_foreign_keys(driver, table=lookup_table, schema=schema))
        if domain == "indexes":
            return list(await self.get_indexes(driver, table=lookup_table, schema=schema))
        constraints_result = await self.get_constraints(driver, table=lookup_table, schema=schema)
        if constraints_result.capability.support != MetadataSupport.SUPPORTED:
            return []
        return list(constraints_result.items)

    async def _synthesize_table_ddl(
        self,
        driver: Any,
        table_name: str,
        *,
        schema: "str | None" = None,
        target_dialect: "str | None" = None,
        warnings: tuple[str, ...] = (),
    ) -> "DDLResult":
        """Synthesize table DDL from exact catalog names using columns, constraints, foreign keys, and indexes."""
        lookup_table = self.exact_catalog_identifier(driver, table_name)
        lookup_schema = self.exact_catalog_identifier(driver, schema) if schema is not None else None
        columns = await self.get_columns(driver, table=lookup_table, schema=lookup_schema)
        if not columns:
            return self._unsupported_ddl(table_name, schema=schema, object_type="table", warnings=warnings)
        constraints_result = await self.get_constraints(driver, table=lookup_table, schema=lookup_schema)
        constraints = (
            constraints_result.items if constraints_result.capability.support == MetadataSupport.SUPPORTED else ()
        )
        warnings = (*warnings, *constraints_result.warnings)
        foreign_keys = await self.get_foreign_keys(driver, table=lookup_table, schema=lookup_schema)
        indexes = await self.get_indexes(driver, table=lookup_table, schema=lookup_schema)
        return self.build_synthesized_table_ddl(
            table_name,
            columns,
            schema=schema,
            constraints=constraints,
            foreign_keys=foreign_keys,
            indexes=indexes,
            target_dialect=target_dialect,
            source_dialect=self.resolve_ddl_dialect(driver),
            warnings=warnings,
        )

    def _unsupported_metadata(self, domain: str) -> "MetadataResult":
        return MetadataResult.unsupported(domain)

    def _unsupported_ddl(
        self, object_name: str, *, schema: "str | None", object_type: str, warnings: tuple[str, ...] = ()
    ) -> "DDLResult":
        return DDLResult.unsupported(
            ObjectIdentity(object_name, object_type, schema=schema, dialect=self.dialect),
            warnings=(*warnings, f"{self.dialect} DDL extraction is not implemented"),
        )


@mypyc_attr(allow_interpreted_subclasses=False)
class _AsyncDriverTransaction(Generic[_AsyncDriverT]):
    """Context manager that commits on success and rolls back on error.

    Inside another SQLSpec transaction block on the same driver, the block runs in
    a savepoint that is released on success and rolled back to on error.
    """

    __slots__ = ("_driver", "_entered", "_savepoint")

    def __init__(self, driver: "_AsyncDriverT") -> None:
        self._driver = driver
        self._savepoint: str | None = None
        self._entered = False

    async def __aenter__(self) -> "_AsyncDriverT":
        driver = self._driver
        depth = driver._transaction_depth
        if depth > 0:
            name = f"sqlspec_sp_{depth}"
            try:
                await driver.create_savepoint(name)
            except NotImplementedError as exc:
                msg = f"{type(driver).__name__} does not support the savepoints used by nested transaction blocks."
                error = ImproperConfigurationError(msg)
                error.__cause__ = exc
                raise error from exc
            self._savepoint = name
        else:
            try:
                in_transaction = driver._connection_in_transaction()
            except NotImplementedError:
                in_transaction = False
            if not in_transaction:
                await driver.begin()
        driver._transaction_depth = depth + 1
        self._entered = True
        return driver

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc: "BaseException | None", traceback: "TracebackType | None"
    ) -> "Literal[False]":
        driver = self._driver
        savepoint = self._savepoint
        entered = self._entered
        self._savepoint = None
        self._entered = False
        try:
            if savepoint is not None:
                if exc_type is None:
                    await driver.release_savepoint(savepoint)
                else:
                    await driver.rollback_to_savepoint(savepoint)
                return False
            if exc_type is not None:
                await driver.rollback()
                return False
            try:
                await driver.commit()
            except BaseException:
                try:
                    await driver.rollback()
                except Exception:
                    logger.debug("Rollback after a failed commit also failed", exc_info=True)
                raise
            return False
        finally:
            if entered and driver._transaction_depth > 0:
                driver._transaction_depth -= 1
