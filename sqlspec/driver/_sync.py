"""Synchronous driver protocol implementation.

Storage and SQL translation methods are defined directly on ``SyncDriverAdapterBase``
rather than separate mixins to avoid cross-trait mypyc attribute dispatch issues,
and ``SyncExceptionHandler`` stores mapped exceptions on ``pending_exception``
rather than raising inside ``__exit__`` across C-extension ABI boundaries.
"""

import contextlib
import logging
import sys
from abc import abstractmethod
from collections.abc import Mapping
from time import perf_counter
from typing import TYPE_CHECKING, Any, ClassVar, Final, Generic, Literal, Self, TypeVar, cast, final, overload

from mypy_extensions import mypyc_attr

from sqlspec.core import SQL, StackResult, create_arrow_result
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
    CommonDriverAttributesMixin,
    DataDictionaryDialectMixin,
    DataDictionaryMixin,
    ExecutionResult,
    StackExecutionObserver,
    SyncExceptionHandler,
    _raise_database_exception,
    _resolve_fast_params,
    describe_stack_statement,
    handle_single_row_error,
    validate_savepoint_name,
)
from sqlspec.driver._exception_handler import _run_with_sync_exception_handler
from sqlspec.driver._query_cache import CachedQuery
from sqlspec.driver._sql_helpers import DEFAULT_PRETTY
from sqlspec.driver._sql_helpers import convert_to_dialect as _convert_to_dialect_impl
from sqlspec.driver._storage_helpers import stringify_storage_target
from sqlspec.driver._stream import SyncRowStream, _LazyEagerSyncRowSource
from sqlspec.exceptions import ImproperConfigurationError, StackExecutionError
from sqlspec.observability import _runtime as observability_runtime
from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry, SyncStoragePipeline
from sqlspec.utils.arrow_helpers import convert_dict_to_arrow_with_schema
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.schema import ValueT, to_value_type
from sqlspec.utils.type_guards import resolve_row_format

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
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

__all__ = ("SyncDataDictionaryBase", "SyncDriverAdapterBase", "SyncPoolConnectionContext", "SyncPoolSessionFactory")

_LOGGER_NAME: Final[str] = "sqlspec.driver"
logger = get_logger(_LOGGER_NAME)
_SyncResultT = TypeVar("_SyncResultT")
_SyncDriverT = TypeVar("_SyncDriverT", bound="SyncDriverAdapterBase")


@mypyc_attr(allow_interpreted_subclasses=True)
class SyncPoolConnectionContext:
    """Generic sync connection context using pool.get_connection() pattern.

    Subclass per adapter for type-safe ``provide_connection()`` return annotations.
    """

    __slots__ = ("_config", "_ctx")

    def __init__(self, config: Any) -> None:
        self._config = config
        self._ctx: Any = None

    def __enter__(self) -> Any:
        pool = self._config.provide_pool()
        self._ctx = pool.get_connection()
        return self._ctx.__enter__()

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> "bool | None":
        if self._ctx:
            return cast("bool | None", self._ctx.__exit__(exc_type, exc_val, exc_tb))
        return None


@mypyc_attr(allow_interpreted_subclasses=True)
class SyncPoolSessionFactory:
    """Generic sync session factory using pool.get_connection() pattern.

    Subclass per adapter for type-safe ``acquire_connection()`` return annotations.
    """

    __slots__ = ("_config", "_ctx")

    def __init__(self, config: Any) -> None:
        self._config = config
        self._ctx: Any = None

    def acquire_connection(self) -> Any:
        pool = self._config.provide_pool()
        self._ctx = pool.get_connection()
        return self._ctx.__enter__()

    def release_connection(self, _conn: Any, **kwargs: Any) -> None:
        if self._ctx is None:
            return
        self._ctx.__exit__(kwargs.get("exc_type"), kwargs.get("exc_val"), kwargs.get("exc_tb"))
        self._ctx = None


@mypyc_attr(allow_interpreted_subclasses=True)
class SyncDriverAdapterBase(CommonDriverAttributesMixin):
    """Base execution, transaction, Arrow, and storage adapter for synchronous database drivers.

    Provides uniform SQL compilation, parameter binding, statement caching, result mapping,
    Apache Arrow fetching, and storage pipeline execution over synchronous driver connections.
    """

    __slots__ = ()

    dialect: "DialectType | None" = None

    @property
    @abstractmethod
    def data_dictionary(self) -> "SyncDataDictionaryBase":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """

    def set_migration_session_schema(self, schema: str) -> None:
        """Set the default schema for migration SQL when supported.

        Args:
            schema: Schema requested for the current migration session.
        """
        log_with_context(logger, logging.DEBUG, "migration.schema.noop", schema=schema, driver=type(self).__name__)

    def set_migration_non_transactional_schema(self, schema: str) -> None:
        """Set the default schema for non-transactional migration SQL when supported.

        Args:
            schema: Schema requested for the current migration session.
        """
        self.set_migration_session_schema(schema)

    def reset_migration_session_schema(self) -> None:
        """Reset migration schema state after a non-transactional migration."""
        log_with_context(logger, logging.DEBUG, "migration.schema.reset.noop", driver=type(self).__name__)

    def has_schema(self, schema: str) -> bool:
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
    def dispatch_statement_execution(self, statement: "SQL", connection: "Any") -> "SQLResult":
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
                    with exc_handler:
                        result = self._dispatch_statement_with_cursor(
                            connection, statement, has_execution_parameters=bool(execution_parameters)
                        )
                else:
                    result = self._run_with_exception_handler(
                        exc_handler,
                        self._dispatch_statement_with_cursor,
                        connection,
                        statement,
                        has_execution_parameters=bool(execution_parameters),
                    )
                self._check_pending_exception(exc_handler)
                assert result is not None
                return result

            return self._execute_with_observability_sync(
                compiled_sql=compiled_sql,
                execution_parameters=execution_parameters,
                operation="SCRIPT" if statement.is_script else statement.operation_type,
                is_many=statement.is_many,
                is_script=statement.is_script,
                runner=self._dispatch_statement_observed_sync,
                connection=connection,
                statement=statement,
                has_execution_parameters=bool(execution_parameters),
            )
        finally:
            self._release_pooled_statement(statement)

    @abstractmethod
    def dispatch_execute(self, cursor: Any, statement: "SQL") -> ExecutionResult:
        """Execute a single SQL statement.

        Must be implemented by each driver for database-specific execution logic.

        Args:
            cursor: Database cursor/connection object
            statement: SQL statement object with all necessary data and configuration

        Returns:
            ExecutionResult with execution data
        """

    @abstractmethod
    def dispatch_execute_many(self, cursor: Any, statement: "SQL") -> ExecutionResult:
        """Execute SQL with multiple parameter sets (executemany).

        Must be implemented by each driver for database-specific executemany logic.

        Args:
            cursor: Database cursor/connection object
            statement: SQL statement object with all necessary data and configuration

        Returns:
            ExecutionResult with execution data for the many operation
        """

    def dispatch_execute_script(self, cursor: Any, statement: "SQL") -> ExecutionResult:
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
                sub_result = self.dispatch_execute(cursor, single_stmt)
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

    def dispatch_special_handling(self, cursor: Any, statement: "SQL") -> "SQLResult | None":
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
    def begin(self) -> None:
        """Begin a database transaction on the current connection."""

    @abstractmethod
    def commit(self) -> None:
        """Commit the current transaction on the current connection."""

    @abstractmethod
    def rollback(self) -> None:
        """Rollback the current transaction on the current connection."""

    def create_savepoint(self, name: str) -> None:
        """Create a savepoint within the current transaction."""
        self.execute_script(f"SAVEPOINT {validate_savepoint_name(name)}")

    def release_savepoint(self, name: str) -> None:
        """Release a previously created savepoint."""
        self.execute_script(f"RELEASE SAVEPOINT {validate_savepoint_name(name)}")

    def rollback_to_savepoint(self, name: str) -> None:
        """Roll back the current transaction to a previously created savepoint."""
        self.execute_script(f"ROLLBACK TO SAVEPOINT {validate_savepoint_name(name)}")

    def transaction(self) -> "_SyncDriverTransaction[Self]":
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

                with session.transaction():
                    session.execute(
                        "INSERT INTO items (id) VALUES (:id)", id=1
                    )

        Returns:
            A context manager yielding this driver.
        """
        return _SyncDriverTransaction(self)

    @abstractmethod
    def with_cursor(self, connection: Any) -> Any:
        """Create and return a context manager for cursor acquisition and cleanup.

        Returns a context manager that yields a cursor for database operations.
        Concrete implementations handle database-specific cursor creation and cleanup.
        """

    @abstractmethod
    def handle_database_exceptions(self) -> "SyncExceptionHandler":
        """Return a context manager that maps driver errors to SQLSpec exceptions.

        Returns:
            Synchronous exception handler exposing mapped errors via ``pending_exception``.
        """

    def execute(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute a statement with parameter handling."""
        fast_params = _resolve_fast_params(parameters, kwargs)
        if (
            self._stmt_cache_enabled
            and (statement_config is None or statement_config is self.statement_config)
            and isinstance(statement, str)
            and fast_params is not None
        ):
            fast_result = self._cached_execution(statement, fast_params)
            if fast_result is not None:
                return cast("SQLResult", fast_result)
        sql_statement = self.prepare_statement(
            statement, parameters, statement_config=statement_config or self.statement_config, kwargs=kwargs
        )
        return self.dispatch_statement_execution(statement=sql_statement, connection=self.connection)

    def execute_many(
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

        if isinstance(statement, str) and not filters and not kwargs:
            sql_statement = SQL(statement, parameters, statement_config=config, is_many=True)
        elif isinstance(statement, SQL):
            statement_seed = statement.raw_expression or statement.raw_sql
            sql_statement = SQL(statement_seed, parameters, statement_config=config, is_many=True, **kwargs)
        else:
            base_statement = self.prepare_statement(statement, filters, statement_config=config, kwargs=kwargs)
            statement_seed = base_statement.raw_expression or base_statement.raw_sql
            sql_statement = SQL(statement_seed, parameters, statement_config=config, is_many=True, **kwargs)

        return self.dispatch_statement_execution(statement=sql_statement, connection=self.connection)

    def execute_script(
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
        return self.dispatch_statement_execution(statement=sql_statement.as_script(), connection=self.connection)

    @overload
    def select(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "list[SchemaT]": ...
    @overload
    def select(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "list[dict[str, Any]]": ...
    def select(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "list[SchemaT] | list[dict[str, Any]]":
        """Execute a select statement and return all rows."""
        result = self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        return result.get_data(schema_type=schema_type)

    @overload
    def fetch(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "list[SchemaT]": ...
    @overload
    def fetch(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "list[dict[str, Any]]": ...
    def fetch(
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
        return self.select(statement, *parameters, schema_type=schema_type, statement_config=statement_config, **kwargs)

    @overload
    def select_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SchemaT": ...
    @overload
    def select_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    def select_one(
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
        result = self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        try:
            return result.one(schema_type=schema_type)
        except ValueError as error:
            handle_single_row_error(error)

    @overload
    def fetch_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SchemaT": ...
    @overload
    def fetch_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    def fetch_one(
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
        return self.select_one(
            statement, *parameters, schema_type=schema_type, statement_config=statement_config, **kwargs
        )

    @overload
    def select_one_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SchemaT | None": ...
    @overload
    def select_one_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "dict[str, Any] | None": ...
    def select_one_or_none(
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
        result = self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        return result.one_or_none(schema_type=schema_type)

    @overload
    def fetch_one_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SchemaT | None": ...
    @overload
    def fetch_one_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "dict[str, Any] | None": ...
    def fetch_one_or_none(
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
        return self.select_one_or_none(
            statement, *parameters, schema_type=schema_type, statement_config=statement_config, **kwargs
        )

    @overload
    def select_value(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: "type[ValueT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "ValueT": ...
    @overload
    def select_value(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> Any: ...
    def select_value(
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
            The scalar value, optionally converted to the specified type.
        """
        result = self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        try:
            value = result.scalar()
        except ValueError as error:
            handle_single_row_error(error)

        if value_type is not None:
            return to_value_type(value, value_type)
        return value

    @overload
    def fetch_value(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: "type[ValueT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "ValueT": ...
    @overload
    def fetch_value(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> Any: ...
    def fetch_value(
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
        return self.select_value(
            statement, *parameters, value_type=value_type, statement_config=statement_config, **kwargs
        )

    @overload
    def select_value_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: "type[ValueT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "ValueT | None": ...
    @overload
    def select_value_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> Any: ...
    def select_value_or_none(
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
        result = self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        value = result.scalar_or_none()

        if value is None:
            return None
        if value_type is not None:
            return to_value_type(value, value_type)
        return value

    @overload
    def fetch_value_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: "type[ValueT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "ValueT | None": ...
    @overload
    def fetch_value_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> Any: ...
    def fetch_value_or_none(
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
        return self.select_value_or_none(
            statement, *parameters, value_type=value_type, statement_config=statement_config, **kwargs
        )

    @overload
    def select_with_total(
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
    def select_with_total(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> "tuple[list[dict[str, Any]], int]": ...
    def select_with_total(
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
            result = self.execute(modified_sql)
            rows = result.all()
            data, total = self._extract_total_from_rows(rows)

            if schema_type is not None:
                return (cast("list[SchemaT]", self.to_schema(data, schema_type=schema_type)), total)
            return (data, total)

        count_result = self.execute(self._count_query(sql_statement))
        select_result = self.execute(sql_statement)

        return (select_result.get_data(schema_type=schema_type), count_result.scalar())

    @overload
    def fetch_with_total(
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
    def fetch_with_total(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> "tuple[list[dict[str, Any]], int]": ...
    def fetch_with_total(
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
        return self.select_with_total(
            statement,
            *parameters,
            schema_type=schema_type,
            statement_config=statement_config,
            count_with_window=count_with_window,
            **kwargs,
        )

    def select_to_arrow(
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
        """Execute query and return results as Apache Arrow format.

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

        result = self.execute(statement, *parameters, statement_config=statement_config, **kwargs)

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

    def fetch_to_arrow(
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
        """Execute query and return results as Apache Arrow format.

        This is an alias for :meth:`select_to_arrow` provided for users familiar
        with asyncpg's fetch() naming convention.

        See Also:
            select_to_arrow(): Primary method with identical behavior and full documentation
        """
        return self.select_to_arrow(
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
    ) -> "SyncRowStream[SchemaT]": ...
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
    ) -> "SyncRowStream[dict[str, Any]]": ...
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
    ) -> "SyncRowStream[SchemaT] | SyncRowStream[dict[str, Any]]":
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
        return SyncRowStream(_LazyEagerSyncRowSource(self, sql_statement, chunk_size), schema_type=schema_type)

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
    ) -> "SyncRowStream[SchemaT]": ...
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
    ) -> "SyncRowStream[dict[str, Any]]": ...
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
    ) -> "SyncRowStream[SchemaT] | SyncRowStream[dict[str, Any]]":
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

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "SyncRowStream[dict[str, Any]] | None":
        """Adapter hook returning a native row stream, or None when unsupported."""
        _ = (statement, chunk_size)
        return None

    def execute_stack(self, stack: "StatementStack", *, continue_on_error: bool = False) -> "tuple[StackResult, ...]":
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
                    self.begin()
                    started_transaction = True

                for index, operation in enumerate(stack.operations):
                    try:
                        result = self._execute_stack_operation(operation)
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
                                self.rollback()
                            except Exception as rollback_error:  # pragma: no cover
                                logger.debug("Rollback after stack failure failed: %s", rollback_error)
                            started_transaction = False

                        if continue_on_error:
                            if not caller_in_transaction:
                                self._rollback_failed_stack()
                            observer.record_operation_error(stack_error)
                            results.append(StackResult.from_error(stack_error))
                            continue

                        raise stack_error from exc

                    results.append(StackResult(result=result))

                    if continue_on_error and not caller_in_transaction:
                        self._commit_stack_success()

                if started_transaction:
                    self.commit()
            except Exception:
                if started_transaction:
                    try:
                        self.rollback()
                    except Exception as rollback_error:  # pragma: no cover
                        logger.debug("Rollback after stack failure failed: %s", rollback_error)
                raise

        return tuple(results)

    def select_to_storage(
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

    def load_from_arrow(
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
        """
        self._raise_storage_not_implemented("load_from_arrow")

    def load_from_storage(
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

    def load_from_records(
        self,
        table: str,
        records: "Sequence[Mapping[str, Any]] | Sequence[Sequence[Any]]",
        *,
        columns: "list[str] | None" = None,
        overwrite: bool = False,
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

        Returns:
            StorageBridgeJob with execution telemetry.
        """
        prepared_records = self._prepare_records_for_arrow(records)
        arrow_table = self._records_to_arrow_table(prepared_records, columns)
        return self.load_from_arrow(table, arrow_table, overwrite=overwrite)

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
    def _check_pending_exception(exc_handler: SyncExceptionHandler) -> None:
        """Raise any pending mapped exception after context manager exit."""
        pending = exc_handler.pending_exception
        if pending is not None:
            if pending.__cause__ is not None:
                raise pending
            raise pending from None

    def _run_with_exception_handler(
        self, exc_handler: SyncExceptionHandler, operation: "Callable[..., _SyncResultT]", *args: Any, **kwargs: Any
    ) -> "_SyncResultT | None":
        """Run a sync operation without inheriting the caller's active exception state."""
        return _run_with_sync_exception_handler(exc_handler, operation, *args, **kwargs)

    def _execute_with_observability_sync(
        self,
        *,
        compiled_sql: str,
        execution_parameters: Any,
        operation: "OperationType",
        is_many: bool,
        is_script: bool,
        runner: "Callable[..., SQLResult]",
        **runner_kwargs: Any,
    ) -> "SQLResult":
        """Execute a sync runner while emitting spans, lifecycle hooks, and statement events."""
        runtime = self._observability
        if runtime is None or runtime.is_idle:
            return runner(**runner_kwargs)
        driver_name = type(self).__name__
        query_context = {
            "sql": compiled_sql,
            "parameters": execution_parameters,
            "driver": driver_name,
            "operation": operation,
            "is_many": is_many,
            "is_script": is_script,
        }
        runtime.emit_query_start_sync(**query_context)
        sql_hash = None
        if runtime.span_manager.is_enabled or (
            runtime.has_statement_observers and runtime.config.logging and runtime.config.logging.include_sql_hash
        ):
            sql_hash = observability_runtime.compute_sql_hash(compiled_sql)
        span = runtime.start_query_span(compiled_sql, operation, driver_name, sql_hash=sql_hash)
        started = perf_counter()
        self._pending_statement_profile = None
        try:
            result = runner(**runner_kwargs)
        except Exception as exc:
            self._pending_statement_profile = None
            runtime.span_manager.end_span(span, error=exc)
            runtime.emit_error_sync(exc, **query_context)
            raise
        profile: dict[str, Any] | None = self._pending_statement_profile
        self._pending_statement_profile = None
        runtime.span_manager.end_span(span)
        duration = perf_counter() - started
        runtime.emit_query_complete_sync(**{**query_context, "rows_affected": result.rows_affected})
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

    def _dispatch_statement_observed_sync(
        self, connection: Any, statement: "SQL", *, has_execution_parameters: bool
    ) -> "SQLResult":
        """Execute a statement inside an exception handler and raise any mapped error."""
        exc_handler = self.handle_database_exceptions()
        result: SQLResult | None = None
        try:
            if sys.exc_info()[1] is None:
                with exc_handler:
                    result = self._dispatch_statement_with_cursor(
                        connection, statement, has_execution_parameters=has_execution_parameters
                    )
            else:
                result = self._run_with_exception_handler(
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

    def _execute_cache_hit(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: CachedQuery
    ) -> "SQLResult":
        """Execute pre-compiled query via ultra-fast path (sync).

        Uses a DB-API direct path when available (`cursor.execute`) and falls
        back to dispatch_execute() for adapters with non-standard cursor APIs.
        For DML operations, returns DMLResult to avoid full SQLResult costs.

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
            with exc_handler:
                result = self._execute_cache_hit_with_cursor(sql, params, cached)
        else:
            result = self._run_with_exception_handler(
                exc_handler, self._execute_cache_hit_with_cursor, sql, params, cached
            )
        self._check_pending_exception(exc_handler)
        assert result is not None
        return result

    def _execute_cache_hit_observed(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: CachedQuery
    ) -> "SQLResult":
        """Execute a fast-path cache hit wrapped in observability instrumentation."""
        obs_params = list(params) if isinstance(params, tuple) else params
        return self._execute_with_observability_sync(
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

    def _execute_cached_statement(self, statement: "SQL") -> "SQLResult":
        """Execute pre-compiled query via fast path.

        The statement is already compiled by _cached_execution, so dispatch_execute
        will hit the fast path in _compiled_statement (is_processed check).
        """
        exc_handler = self.handle_database_exceptions()
        result: SQLResult | None = None
        try:
            if sys.exc_info()[1] is None:
                with exc_handler:
                    result = self._execute_cached_statement_with_cursor(statement)
            else:
                result = self._run_with_exception_handler(
                    exc_handler, self._execute_cached_statement_with_cursor, statement
                )
            self._check_pending_exception(exc_handler)
            assert result is not None
            return result
        finally:
            self._release_pooled_statement(statement)

    def _execute_cached_statement_observed(
        self, statement: "SQL", compiled_sql: str, execution_parameters: Any, operation: "OperationType"
    ) -> "SQLResult":
        """Execute a rebound cached statement wrapped in observability instrumentation."""
        obs_params = list(execution_parameters) if isinstance(execution_parameters, tuple) else execution_parameters
        return self._execute_with_observability_sync(
            compiled_sql=compiled_sql,
            execution_parameters=obs_params,
            operation=operation,
            is_many=False,
            is_script=False,
            runner=self._execute_cached_statement,
            statement=statement,
        )

    def _dispatch_statement_with_cursor(
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
            cursor = cursor_manager.__enter__()
            cursor_entered = True
            special_result = self.dispatch_special_handling(cursor, statement)
            if special_result is not None:
                result = special_result
            elif statement.is_script:
                execution_result = self.dispatch_execute_script(cursor, statement)
            elif statement.is_many:
                if has_execution_parameters:
                    execution_result = self.dispatch_execute_many(cursor, statement)
                else:
                    execution_result = self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
            else:
                execution_result = self.dispatch_execute(cursor, statement)
            if special_result is None:
                assert execution_result is not None
                result = self.build_statement_result(statement, execution_result)
        except Exception as exc:
            error = exc
        finally:
            if cursor_entered:
                if error is None:
                    cursor_manager.__exit__(None, None, None)
                else:
                    exit_suppressed = bool(cursor_manager.__exit__(type(error), error, error.__traceback__))
        if error is not None and not exit_suppressed:
            raise error
        assert result is not None
        return result

    def _execute_cache_hit_with_cursor(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: CachedQuery
    ) -> "SQLResult":
        """Execute a cached query while owning only the cursor context."""
        direct_statement: SQL | None = None
        result: SQLResult | None = None
        try:
            with self.with_cursor(self.connection) as cursor:
                wire_sql = self._format_wire_sql(cached.compiled_sql)
                returns_rows = cached.operation_profile.returns_rows
                execute = getattr(cursor, "execute", None) if cursor is not self.connection else None
                fetchall = getattr(cursor, "fetchall", None) if execute is not None and returns_rows else None
                can_use_cursor_fast_path = execute is not None and (
                    (returns_rows and fetchall is not None) or (not returns_rows and hasattr(cursor, "rowcount"))
                )
                if can_use_cursor_fast_path:
                    assert execute is not None
                    if params:
                        execute(wire_sql, params)
                    else:
                        execute(wire_sql)
                    if returns_rows:
                        assert fetchall is not None
                        fetched_data = fetchall()
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
                    execution_result = self.dispatch_execute(cursor, direct_statement)
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

    def _execute_cached_statement_with_cursor(self, statement: "SQL") -> "SQLResult":
        """Execute a prepared statement while owning only the cursor context."""
        with self.with_cursor(self.connection) as cursor:
            execution_result = self.dispatch_execute(cursor, statement)
            return self.build_statement_result(statement, execution_result)

    def _connection_in_transaction(self) -> bool:
        """Check if the connection is inside a transaction.

        Each adapter MUST override this method with direct attribute access
        for optimal mypyc performance. Do not use getattr chains.

        Raises:
            NotImplementedError: Always - subclasses must override.
        """
        msg = "Adapters must override _connection_in_transaction()"
        raise NotImplementedError(msg)

    def _execute_stack_operation(self, operation: "StackOperation") -> "SQLResult | ArrowResult | None":
        kwargs = dict(operation.keyword_arguments) if operation.keyword_arguments else {}

        if operation.method == "execute":
            return self.execute(operation.statement, *operation.arguments, **kwargs)

        if operation.method == "execute_many":
            if not operation.arguments:
                msg = "execute_many stack operation requires parameter sets"
                raise ValueError(msg)
            parameter_sets = operation.arguments[0]
            filters = operation.arguments[1:]
            return self.execute_many(operation.statement, parameter_sets, *filters, **kwargs)

        if operation.method == "execute_script":
            return self.execute_script(operation.statement, *operation.arguments, **kwargs)

        if operation.method == "execute_arrow":
            return self.select_to_arrow(operation.statement, *operation.arguments, **kwargs)

        msg = f"Unsupported stack operation method: {operation.method}"
        raise ValueError(msg)

    def _rollback_failed_stack(self) -> None:
        """Attempt to rollback after a stack operation error to clear connection state."""
        try:
            self.rollback()
        except Exception as rollback_error:  # pragma: no cover
            logger.debug("Rollback after stack error failed: %s", rollback_error)

    def _commit_stack_success(self) -> None:
        """Attempt to commit after a successful stack operation when not batching."""
        try:
            self.commit()
        except Exception as commit_error:  # pragma: no cover
            logger.debug("Commit after stack operation failed: %s", commit_error)

    def _storage_pipeline(self) -> "SyncStoragePipeline":
        """Get or create a sync storage pipeline.

        Returns:
            SyncStoragePipeline instance.
        """
        factory = self.storage_pipeline_factory
        if factory is None:
            return SyncStoragePipeline()
        return cast("SyncStoragePipeline", factory())

    def _write_storage_result(
        self,
        result: "ArrowResult",
        destination: "StorageDestination",
        *,
        format_hint: "StorageFormat | None" = None,
        storage_options: "dict[str, Any] | None" = None,
        pipeline: "SyncStoragePipeline | None" = None,
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
            telemetry = result.write_to_storage_sync(
                destination, format_hint=format_hint, storage_options=storage_options, pipeline=pipeline
            )
        except Exception as exc:
            runtime.end_storage_span(span, error=exc)
            raise
        telemetry = runtime.annotate_storage_telemetry(telemetry)
        runtime.end_storage_span(span, telemetry=telemetry)
        return telemetry

    def _read_storage_arrow(
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
            table, telemetry = pipeline.read_arrow(source, file_format=file_format, storage_options=storage_options)
        except Exception as exc:
            runtime.end_storage_span(span, error=exc)
            raise
        telemetry = runtime.annotate_storage_telemetry(telemetry)
        runtime.end_storage_span(span, telemetry=telemetry)
        return table, telemetry


@mypyc_attr(allow_interpreted_subclasses=True)
class SyncDataDictionaryBase(DataDictionaryDialectMixin, DataDictionaryMixin):
    """Base class for synchronous data dictionary implementations.

    Uses Python-compatible class layouts for cross-module inheritance.
    Child classes define dialect as a class attribute.
    """

    dialect: "ClassVar[str]"
    """Dialect identifier. Must be defined by subclasses as a class attribute."""

    def __init__(self) -> None:
        self._version_cache: dict[int, VersionInfo | None] = {}
        self._version_fetch_attempted: set[int] = set()

    def get_metadata_capabilities(
        self, driver: Any, domains: "Sequence[str] | None" = None
    ) -> "MetadataCapabilityProfile":
        """Get data-dictionary capability profile.

        Args:
            driver: Sync database driver instance.
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

    def get_system_metadata_capabilities(
        self, driver: Any, domains: "Sequence[str] | None" = None
    ) -> "tuple[SystemMetadataCapability, ...]":
        """Get opt-in system metadata capability disclosures.

        Args:
            driver: Sync database driver instance.
            domains: Optional system metadata domains to report.

        Returns:
            System metadata capabilities. Base implementations report requested domains as unsupported.
        """
        requested_domains = DEFAULT_SYSTEM_METADATA_DOMAINS if domains is None else tuple(domains)
        return system_metadata_capabilities_from_domains(requested_domains)

    def get_schemas(self, driver: Any) -> "MetadataResult":
        """Get schema metadata or an unsupported-domain result."""
        return self._unsupported_metadata("schemas")

    def get_objects(self, driver: Any, schema: "str | None" = None) -> "MetadataResult":
        """Get database object metadata or an unsupported-domain result."""
        return self._unsupported_metadata("objects")

    def get_table_details(self, driver: Any, table: str, schema: "str | None" = None) -> "MetadataResult":
        """Get rich table metadata or an unsupported-domain result."""
        return self._unsupported_metadata("tables")

    def get_constraints(self, driver: Any, table: "str | None" = None, schema: "str | None" = None) -> "MetadataResult":
        """Get constraint metadata or an unsupported-domain result."""
        return self._unsupported_metadata("constraints")

    def get_views(self, driver: Any, schema: "str | None" = None) -> "MetadataResult":
        """Get view metadata or an unsupported-domain result."""
        return self._unsupported_metadata("views")

    def get_routines(self, driver: Any, schema: "str | None" = None) -> "MetadataResult":
        """Get routine metadata or an unsupported-domain result."""
        return self._unsupported_metadata("routines")

    def get_privileges(
        self, driver: Any, object_name: "str | None" = None, schema: "str | None" = None
    ) -> "MetadataResult":
        """Get privilege metadata or an unsupported-domain result."""
        return self._unsupported_metadata("privileges")

    def get_dependencies(
        self, driver: Any, object_name: "str | None" = None, schema: "str | None" = None
    ) -> "MetadataResult":
        """Get dependency metadata or an unsupported-domain result."""
        return self._unsupported_metadata("dependencies")

    def get_ddl(
        self,
        driver: Any,
        object_name: str,
        schema: "str | None" = None,
        *,
        object_type: str = "table",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
    ) -> "DDLResult":
        """Get object DDL or an explicit unsupported DDL result."""
        return self.get_object_ddl(
            driver,
            object_name,
            schema=schema,
            object_type=object_type,
            include_dependencies=include_dependencies,
            prefer_native=prefer_native,
            redact=redact,
        )

    def get_object_ddl(
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
        """Get one object's DDL or an explicit unsupported DDL result."""
        if object_type.lower() == "table" and (not prefer_native or target_dialect is not None):
            return self._synthesize_table_ddl(driver, object_name, schema=schema, target_dialect=target_dialect)
        if type(self).get_ddl is not SyncDataDictionaryBase.get_ddl:
            native_res = self.get_ddl(
                driver,
                object_name,
                schema=schema,
                object_type=object_type,
                include_dependencies=include_dependencies,
                prefer_native=prefer_native,
                redact=redact,
            )
            if native_res.status == MetadataSupport.SUPPORTED and native_res.ddl:
                if target_dialect is not None:
                    return self._transpile_ddl_result(native_res, target_dialect=target_dialect)
                return native_res
        if object_type.lower() == "table":
            return self._synthesize_table_ddl(driver, object_name, schema=schema, target_dialect=target_dialect)
        return self._unsupported_ddl(object_name, schema=schema, object_type=object_type)

    def get_schema_ddl(
        self,
        driver: Any,
        schema: "str | None" = None,
        *,
        include_domains: "Sequence[str] | None" = None,
        exclude_domains: "Sequence[str] | None" = None,
        exclude_tables: "Sequence[str] | None" = ("ddl_migrations",),
        direction: "Literal['create', 'drop']" = "create",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
        target_dialect: "str | None" = None,
    ) -> "MetadataResult":
        """Get schema DDL items in topological dependency order."""
        allowed_domains = {"sequences", "tables", "views"}
        if include_domains is not None:
            normalized_inc = {d.strip().lower().rstrip("s") + "s" for d in include_domains if d.strip()}
            allowed_domains &= normalized_inc
        if exclude_domains is not None:
            normalized_exc = {d.strip().lower().rstrip("s") + "s" for d in exclude_domains if d.strip()}
            allowed_domains -= normalized_exc

        excluded_table_names = {t.strip().lower() for t in (exclude_tables or ()) if t and t.strip()}
        ddl_results: list[DDLResult] = []
        collected_fks: list[Any] = []

        if "tables" in allowed_domains:
            tables = self.get_tables(driver, schema=schema)
            for tbl_item in tables:
                tbl_name, tbl_schema = self._extract_table_name_and_schema(tbl_item, default_schema=schema)
                if not tbl_name or tbl_name.lower() in excluded_table_names:
                    continue
                if not prefer_native or target_dialect is not None:
                    ddl_res = self._synthesize_table_ddl(
                        driver, tbl_name, schema=tbl_schema, target_dialect=target_dialect
                    )
                else:
                    ddl_res = self.get_object_ddl(
                        driver,
                        tbl_name,
                        schema=tbl_schema,
                        object_type="table",
                        include_dependencies=include_dependencies,
                        prefer_native=prefer_native,
                        redact=redact,
                    )
                if ddl_res.status == MetadataSupport.SUPPORTED and ddl_res.ddl:
                    ddl_results.append(ddl_res)

        if "views" in allowed_domains:
            try:
                views_res = self.get_views(driver, schema=schema)
            except Exception:
                views_res = self._unsupported_metadata("views")
            if views_res.capability.support == MetadataSupport.SUPPORTED:
                for view_row in views_res.items:
                    view_ddl = self._build_view_ddl_from_row(
                        view_row, default_schema=schema, target_dialect=target_dialect
                    )
                    if view_ddl is not None:
                        ddl_results.append(view_ddl)

        dependency_items: Sequence[Any] = ()
        if include_dependencies:
            with contextlib.suppress(Exception):
                schema_fks = self.get_foreign_keys(driver, table=None, schema=schema)
                if schema_fks:
                    collected_fks.extend(schema_fks)
            if not collected_fks:
                for item in ddl_results:
                    if item.identity.object_type.lower() == "table":
                        with contextlib.suppress(Exception):
                            tbl_fks = self.get_foreign_keys(
                                driver, table=item.identity.name, schema=item.identity.schema
                            )
                            if tbl_fks:
                                collected_fks.extend(tbl_fks)
            try:
                deps_res = self.get_dependencies(driver, schema=schema)
                if deps_res.capability.support == MetadataSupport.SUPPORTED:
                    dependency_items = deps_res.items
            except Exception:
                dependency_items = ()

        return self._finalize_schema_ddl_results(
            ddl_results,
            foreign_keys=collected_fks,
            dependency_items=dependency_items,
            exclude_tables=exclude_tables,
            direction=direction,
            target_dialect=target_dialect,
        )

    def get_table_profile(
        self,
        driver: Any,
        table: str,
        schema: "str | None" = None,
        *,
        sample_size: int = 1000,
        target_dialect: "str | None" = None,
    ) -> "list[ColumnProfileMetadata]":
        """Profile live table columns and infer refined SQL types from existing data."""
        resolved_table = self.resolve_identifier(table)
        resolved_schema = self.resolve_schema(schema)
        columns = self.get_columns(driver, table=resolved_table, schema=resolved_schema)
        if not columns:
            return []

        stats_row: Mapping[str, Any] | None = None
        try:
            profile_sql = self._build_table_profile_sql(resolved_table, columns, schema=resolved_schema)
            stats_rows = driver.select(profile_sql)
            if stats_rows and isinstance(stats_rows[0], Mapping):
                stats_row = stats_rows[0]
        except Exception:
            stats_row = None

        sample_rows: list[Mapping[str, Any]] = []
        try:
            sample_sql = self._build_table_sample_sql(resolved_table, schema=resolved_schema, sample_size=sample_size)
            raw_samples = driver.select(sample_sql)
            sample_rows = [row for row in raw_samples if isinstance(row, Mapping)]
        except Exception:
            sample_rows = []

        return self._compile_column_profiles(columns, stats_row, sample_rows, target_dialect=target_dialect)

    def infer_table_ddl_from_data(
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
        profiles = self.get_table_profile(
            driver, table, schema=schema, sample_size=sample_size, target_dialect=target_dialect
        )
        resolved_table = self.resolve_identifier(table)
        resolved_schema = self.resolve_schema(schema)
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

    def get_system_metadata(
        self, driver: Any, request: "SystemMetadataRequest | str | None" = None, **kwargs: Any
    ) -> "SystemMetadataResult":
        """Get opt-in system metadata or a gated/unsupported result."""
        metadata_request = ensure_system_metadata_request(request, **kwargs)
        capability = unsupported_system_metadata_capability(metadata_request.domain)
        return system_metadata_gated_result(metadata_request, capability)

    @abstractmethod
    def get_version(self, driver: Any) -> "VersionInfo | None":
        """Get database version information.

        Args:
            driver: Sync database driver instance

        Returns:
            Version information or None if detection fails
        """

    @abstractmethod
    def get_feature_flag(self, driver: Any, feature: str) -> bool:
        """Check if database supports a specific feature.

        Args:
            driver: Sync database driver instance
            feature: Feature name to check

        Returns:
            True if feature is supported, False otherwise
        """

    @abstractmethod
    def get_optimal_type(self, driver: Any, type_category: str) -> str:
        """Get optimal database type for a category.

        Args:
            driver: Sync database driver instance
            type_category: Type category

        Returns:
            Database-specific type name
        """

    @abstractmethod
    def get_tables(self, driver: Any, schema: "str | None" = None) -> "list[TableMetadata]":
        """Get list of tables in schema.

        Args:
            driver: Sync database driver instance
            schema: Schema name (None for default)

        Returns:
            List of table metadata dictionaries
        """

    @abstractmethod
    def get_columns(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema.

        Args:
            driver: Sync database driver instance
            table: Table name (None to fetch columns for all tables in schema)
            schema: Schema name (None for default)

        Returns:
            List of column metadata dictionaries
        """

    @abstractmethod
    def get_indexes(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index information for a table or schema.

        Args:
            driver: Sync database driver instance
            table: Table name (None to fetch indexes for all tables in schema)
            schema: Schema name (None for default)

        Returns:
            List of index metadata dictionaries
        """

    @abstractmethod
    def get_foreign_keys(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata.

        Args:
            driver: Sync database driver instance
            table: Optional table name filter
            schema: Optional schema name filter

        Returns:
            List of foreign key metadata
        """

    def _synthesize_table_ddl(
        self,
        driver: Any,
        table_name: str,
        *,
        schema: "str | None" = None,
        target_dialect: "str | None" = None,
        warnings: tuple[str, ...] = (),
    ) -> "DDLResult":
        """Synthesize table DDL from get_columns, get_constraints, get_foreign_keys, and get_indexes."""
        resolved_schema: str | None = schema
        resolved_table: str = table_name
        if type(self).dialect not in {"generic", "unknown", ""} and hasattr(self, "resolve_schema"):
            try:
                resolved_schema = self.resolve_schema(schema)
                resolved_table = self.resolve_identifier(table_name)
            except Exception:
                resolved_schema = schema
                resolved_table = table_name
        try:
            columns = self.get_columns(driver, table=resolved_table, schema=resolved_schema)
        except Exception:
            columns = []
        if not columns:
            return self._unsupported_ddl(resolved_table, schema=resolved_schema, object_type="table")

        constraints: Sequence[Any] = ()
        try:
            constraints_res = self.get_constraints(driver, table=resolved_table, schema=resolved_schema)
            if constraints_res.capability.support == MetadataSupport.SUPPORTED:
                constraints = constraints_res.items
        except Exception:
            constraints = ()

        foreign_keys: Sequence[Any] = ()
        try:
            foreign_keys = self.get_foreign_keys(driver, table=resolved_table, schema=resolved_schema)
        except Exception:
            foreign_keys = ()

        indexes: Sequence[Any] = ()
        try:
            indexes = self.get_indexes(driver, table=resolved_table, schema=resolved_schema)
        except Exception:
            indexes = ()

        return self.build_synthesized_table_ddl(
            resolved_table,
            columns,
            schema=resolved_schema,
            constraints=constraints,
            foreign_keys=foreign_keys,
            indexes=indexes,
            target_dialect=target_dialect,
            warnings=warnings,
        )

    def _unsupported_metadata(self, domain: str) -> "MetadataResult":
        return MetadataResult.unsupported(domain)

    def _unsupported_ddl(self, object_name: str, *, schema: "str | None", object_type: str) -> "DDLResult":
        return DDLResult.unsupported(
            ObjectIdentity(object_name, object_type, schema=schema, dialect=self.dialect),
            warnings=(f"{self.dialect} DDL extraction is not implemented",),
        )


@mypyc_attr(allow_interpreted_subclasses=False)
class _SyncDriverTransaction(Generic[_SyncDriverT]):
    """Context manager that commits on success and rolls back on error.

    Inside another SQLSpec transaction block on the same driver, the block runs in
    a savepoint that is released on success and rolled back to on error.
    """

    __slots__ = ("_driver", "_entered", "_savepoint")

    def __init__(self, driver: "_SyncDriverT") -> None:
        self._driver = driver
        self._savepoint: str | None = None
        self._entered = False

    def __enter__(self) -> "_SyncDriverT":
        driver = self._driver
        depth = driver._transaction_depth
        if depth > 0:
            name = f"sqlspec_sp_{depth}"
            try:
                driver.create_savepoint(name)
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
                driver.begin()
        driver._transaction_depth = depth + 1
        self._entered = True
        return driver

    def __exit__(
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
                    driver.release_savepoint(savepoint)
                else:
                    driver.rollback_to_savepoint(savepoint)
                return False
            if exc_type is not None:
                driver.rollback()
                return False
            try:
                driver.commit()
            except BaseException:
                try:
                    driver.rollback()
                except Exception:
                    logger.debug("Rollback after a failed commit also failed", exc_info=True)
                raise
            return False
        finally:
            if entered and driver._transaction_depth > 0:
                driver._transaction_depth -= 1
