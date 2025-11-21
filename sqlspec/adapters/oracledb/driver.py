"""Oracle Driver"""

import contextlib
import logging
import re
from typing import TYPE_CHECKING, Any, Final, NamedTuple, cast

import oracledb
from oracledb import AsyncCursor, Cursor

from sqlspec.adapters.oracledb._types import OracleAsyncConnection, OracleSyncConnection
from sqlspec.adapters.oracledb.data_dictionary import OracleAsyncDataDictionary, OracleSyncDataDictionary
from sqlspec.adapters.oracledb.type_converter import OracleTypeConverter
from sqlspec.core import (
    SQL,
    DriverParameterProfile,
    ParameterStyle,
    StackResult,
    StatementConfig,
    StatementStack,
    build_statement_config_from_profile,
    create_arrow_result,
    create_sql_result,
    get_cache_config,
    register_driver_profile,
)
from sqlspec.driver import (
    AsyncDataDictionaryBase,
    AsyncDriverAdapterBase,
    SyncDataDictionaryBase,
    SyncDriverAdapterBase,
)
from sqlspec.driver._common import StackExecutionObserver, VersionInfo, describe_stack_statement, hash_stack_operations
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    SQLParsingError,
    SQLSpecError,
    StackExecutionError,
    TransactionError,
    UniqueViolationError,
)
from sqlspec.utils.logging import log_with_context
from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from collections.abc import Sequence
    from contextlib import AbstractAsyncContextManager, AbstractContextManager
    from typing import Protocol

    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, SQLResult, Statement, StatementConfig, StatementFilter
    from sqlspec.core.stack import StackOperation
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import (
        AsyncStoragePipeline,
        StorageBridgeJob,
        StorageDestination,
        StorageFormat,
        StorageTelemetry,
        SyncStoragePipeline,
    )
    from sqlspec.typing import ArrowReturnFormat, StatementParameters

    class _PipelineDriver(Protocol):
        statement_config: StatementConfig
        driver_features: "dict[str, Any]"

        def prepare_statement(
            self,
            statement: "str | Statement | QueryBuilder",
            parameters: "tuple[Any, ...] | dict[str, Any] | None",
            *,
            statement_config: StatementConfig,
            kwargs: "dict[str, Any]",
        ) -> SQL: ...

        def _get_compiled_sql(self, statement: SQL, statement_config: StatementConfig) -> "tuple[str, Any]": ...


logger = logging.getLogger(__name__)

# Oracle-specific constants
LARGE_STRING_THRESHOLD = 4000  # Threshold for large string parameters to avoid ORA-01704

_type_converter = OracleTypeConverter()

IMPLICIT_UPPER_COLUMN_PATTERN: Final[re.Pattern[str]] = re.compile(r"^(?!\d)(?:[A-Z0-9_]+)$")


__all__ = (
    "OracleAsyncDriver",
    "OracleAsyncExceptionHandler",
    "OracleSyncDriver",
    "OracleSyncExceptionHandler",
    "oracledb_statement_config",
)

PIPELINE_MIN_DRIVER_VERSION: Final[tuple[int, int, int]] = (2, 4, 0)
PIPELINE_MIN_DATABASE_MAJOR: Final[int] = 23
_VERSION_COMPONENTS: Final[int] = 3


def _parse_version_tuple(version: str) -> "tuple[int, int, int]":
    parts = [int(part) for part in version.split(".") if part.isdigit()]
    while len(parts) < _VERSION_COMPONENTS:
        parts.append(0)
    return parts[0], parts[1], parts[2]


_ORACLEDB_VERSION: Final[tuple[int, int, int]] = _parse_version_tuple(getattr(oracledb, "__version__", "0.0.0"))


class _CompiledStackOperation(NamedTuple):
    statement: SQL
    sql: str
    parameters: Any
    method: str
    returns_rows: bool
    summary: str


class OraclePipelineMixin:
    """Shared helpers for Oracle pipeline execution."""

    __slots__ = ()

    def _pipeline_driver(self) -> "_PipelineDriver":
        return cast("_PipelineDriver", self)

    def _stack_native_blocker(self, stack: "StatementStack") -> "str | None":
        for operation in stack.operations:
            if operation.method == "execute_arrow":
                return "arrow_operation"
            if operation.method == "execute_script":
                return "script_operation"
        return None

    def _log_pipeline_skip(self, reason: str, stack: "StatementStack") -> None:
        log_level = logging.INFO if reason == "env_override" else logging.DEBUG
        log_with_context(
            logger,
            log_level,
            "stack.native_pipeline.skip",
            driver=type(self).__name__,
            reason=reason,
            hashed_operations=hash_stack_operations(stack),
        )

    def _prepare_pipeline_operation(self, operation: "StackOperation") -> _CompiledStackOperation:
        driver = self._pipeline_driver()
        kwargs = dict(operation.keyword_arguments) if operation.keyword_arguments else {}
        statement_config = kwargs.pop("statement_config", None)
        config = statement_config or driver.statement_config

        if operation.method == "execute":
            sql_statement = driver.prepare_statement(
                operation.statement, operation.arguments, statement_config=config, kwargs=kwargs
            )
        elif operation.method == "execute_many":
            if not operation.arguments:
                msg = "execute_many stack operation requires parameter sets"
                raise ValueError(msg)
            parameter_sets = operation.arguments[0]
            filters = operation.arguments[1:]
            sql_statement = self._build_execute_many_statement(
                operation.statement, parameter_sets, filters, config, kwargs
            )
        else:
            msg = f"Unsupported stack operation method: {operation.method}"
            raise ValueError(msg)

        compiled_sql, prepared_parameters = driver._get_compiled_sql(  # pyright: ignore[reportPrivateUsage]
            sql_statement, config
        )
        summary = describe_stack_statement(operation.statement)
        return _CompiledStackOperation(
            statement=sql_statement,
            sql=compiled_sql,
            parameters=prepared_parameters,
            method=operation.method,
            returns_rows=sql_statement.returns_rows(),
            summary=summary,
        )

    def _build_execute_many_statement(
        self,
        statement: "str | Statement | QueryBuilder",
        parameter_sets: "Sequence[StatementParameters]",
        filters: "tuple[StatementParameters | StatementFilter, ...]",
        statement_config: "StatementConfig",
        kwargs: "dict[str, Any]",
    ) -> SQL:
        driver = self._pipeline_driver()
        if isinstance(statement, SQL):
            return SQL(statement.raw_sql, parameter_sets, statement_config=statement_config, is_many=True, **kwargs)

        base_statement = driver.prepare_statement(statement, filters, statement_config=statement_config, kwargs=kwargs)
        return SQL(base_statement.raw_sql, parameter_sets, statement_config=statement_config, is_many=True, **kwargs)

    def _add_pipeline_operation(self, pipeline: Any, operation: _CompiledStackOperation) -> None:
        parameters = operation.parameters or []
        if operation.method == "execute":
            if operation.returns_rows:
                pipeline.add_fetchall(operation.sql, parameters)
            else:
                pipeline.add_execute(operation.sql, parameters)
            return

        if operation.method == "execute_many":
            pipeline.add_executemany(operation.sql, parameters)
            return

        msg = f"Unsupported pipeline operation: {operation.method}"
        raise ValueError(msg)

    def _build_stack_results_from_pipeline(
        self,
        compiled_operations: "Sequence[_CompiledStackOperation]",
        pipeline_results: "Sequence[Any]",
        continue_on_error: bool,
        observer: StackExecutionObserver,
    ) -> "list[StackResult]":
        stack_results: list[StackResult] = []
        for index, (compiled, result) in enumerate(zip(compiled_operations, pipeline_results, strict=False)):
            error = getattr(result, "error", None)
            if error is not None:
                stack_error = StackExecutionError(
                    index,
                    compiled.summary,
                    error,
                    adapter=type(self).__name__,
                    mode="continue-on-error" if continue_on_error else "fail-fast",
                )
                if continue_on_error:
                    observer.record_operation_error(stack_error)
                    stack_results.append(StackResult.from_error(stack_error))
                    continue
                raise stack_error

            stack_results.append(self._pipeline_result_to_stack_result(compiled, result))
        return stack_results

    def _pipeline_result_to_stack_result(self, operation: _CompiledStackOperation, pipeline_result: Any) -> StackResult:
        rows = getattr(pipeline_result, "rows", None)
        columns = getattr(pipeline_result, "columns", None)
        data = self._rows_from_pipeline_result(columns, rows) if operation.returns_rows else None
        metadata: dict[str, Any] = {"pipeline_operation": operation.method}

        warning = getattr(pipeline_result, "warning", None)
        if warning is not None:
            metadata["warning"] = warning

        return_value = getattr(pipeline_result, "return_value", None)
        if return_value is not None:
            metadata["return_value"] = return_value

        rowcount = self._rows_affected_from_pipeline(operation, pipeline_result, data)
        sql_result = create_sql_result(operation.statement, data=data, rows_affected=rowcount, metadata=metadata)
        return StackResult.from_sql_result(sql_result)

    def _rows_affected_from_pipeline(
        self, operation: _CompiledStackOperation, pipeline_result: Any, data: "list[dict[str, Any]] | None"
    ) -> int:
        rowcount = getattr(pipeline_result, "rowcount", None)
        if isinstance(rowcount, int) and rowcount >= 0:
            return rowcount
        if operation.method == "execute_many":
            parameter_sets = operation.parameters or ()
            try:
                return len(parameter_sets)
            except TypeError:
                return 0
        if operation.method == "execute" and not operation.returns_rows:
            return 1
        if operation.returns_rows:
            return len(data or [])
        return 0

    def _rows_from_pipeline_result(self, columns: Any, rows: Any) -> "list[dict[str, Any]]":
        if not rows:
            return []

        driver = self._pipeline_driver()
        if columns:
            names = [getattr(column, "name", f"column_{index}") for index, column in enumerate(columns)]
        else:
            first = rows[0]
            names = [f"column_{index}" for index in range(len(first) if hasattr(first, "__len__") else 0)]
        names = _normalize_column_names(names, driver.driver_features)

        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            if isinstance(row, dict):
                normalized_rows.append(row)
                continue
            normalized_rows.append(dict(zip(names, row, strict=False)))
        return normalized_rows

    def _wrap_pipeline_error(
        self, error: Exception, stack: "StatementStack", continue_on_error: bool
    ) -> StackExecutionError:
        mode = "continue-on-error" if continue_on_error else "fail-fast"
        return StackExecutionError(
            -1, "Oracle pipeline execution failed", error, adapter=type(self).__name__, mode=mode
        )


def _normalize_column_names(column_names: "list[str]", driver_features: "dict[str, Any]") -> "list[str]":
    should_lowercase = driver_features.get("enable_lowercase_column_names", False)
    if not should_lowercase:
        return column_names
    normalized: list[str] = []
    for name in column_names:
        if name and IMPLICIT_UPPER_COLUMN_PATTERN.fullmatch(name):
            normalized.append(name.lower())
        else:
            normalized.append(name)
    return normalized


def _oracle_insert_statement(table: str, columns: "list[str]") -> str:
    column_list = ", ".join(columns)
    placeholders = ", ".join(f":{idx + 1}" for idx in range(len(columns)))
    return f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"


def _oracle_truncate_statement(table: str) -> str:
    return f"TRUNCATE TABLE {table}"


def _coerce_sync_row_values(row: "tuple[Any, ...]") -> "list[Any]":
    """Coerce LOB handles to concrete values for synchronous execution.

    Processes each value in the row, reading LOB objects and applying
    type detection for JSON values stored in CLOBs.

    Args:
        row: Tuple of column values from database fetch.

    Returns:
        List of coerced values with LOBs read to strings/bytes.
    """
    coerced_values: list[Any] = []
    for value in row:
        if hasattr(value, "read"):
            try:
                processed_value = value.read()
            except Exception:
                coerced_values.append(value)
                continue
            if isinstance(processed_value, str):
                processed_value = _type_converter.convert_if_detected(processed_value)
            coerced_values.append(processed_value)
        else:
            coerced_values.append(value)
    return coerced_values


async def _coerce_async_row_values(row: "tuple[Any, ...]") -> "list[Any]":
    """Coerce LOB handles to concrete values for asynchronous execution.

    Processes each value in the row, reading LOB objects asynchronously
    and applying type detection for JSON values stored in CLOBs.

    Args:
        row: Tuple of column values from database fetch.

    Returns:
        List of coerced values with LOBs read to strings/bytes.
    """
    coerced_values: list[Any] = []
    for value in row:
        if hasattr(value, "read"):
            try:
                processed_value = await _type_converter.process_lob(value)
            except Exception:
                coerced_values.append(value)
                continue
            if isinstance(processed_value, str):
                processed_value = _type_converter.convert_if_detected(processed_value)
            coerced_values.append(processed_value)
        else:
            coerced_values.append(value)
    return coerced_values


ORA_CHECK_CONSTRAINT = 2290
ORA_INTEGRITY_RANGE_START = 2200
ORA_INTEGRITY_RANGE_END = 2300
ORA_PARSING_RANGE_START = 900
ORA_PARSING_RANGE_END = 1000
ORA_TABLESPACE_FULL = 1652


class OracleSyncCursor:
    """Sync context manager for Oracle cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: OracleSyncConnection) -> None:
        self.connection = connection
        self.cursor: Cursor | None = None

    def __enter__(self) -> Cursor:
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        if self.cursor is not None:
            self.cursor.close()


class OracleAsyncCursor:
    """Async context manager for Oracle cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: OracleAsyncConnection) -> None:
        self.connection = connection
        self.cursor: AsyncCursor | None = None

    async def __aenter__(self) -> AsyncCursor:
        self.cursor = self.connection.cursor()
        return self.cursor

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = (exc_type, exc_val, exc_tb)  # Mark as intentionally unused
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                # Oracle async cursors have a synchronous close method
                # but we need to ensure proper cleanup in the event loop context
                self.cursor.close()


class OracleExceptionHandler:
    """Context manager for handling Oracle database exceptions.

    Maps Oracle ORA-XXXXX error codes to specific SQLSpec exceptions
    for better error handling in application code.
    """

    __slots__ = ()

    def _map_oracle_exception(self, e: Any) -> None:
        """Map Oracle exception to SQLSpec exception.

        Args:
            e: oracledb.DatabaseError instance
        """
        error_obj = e.args[0] if e.args else None
        if not error_obj:
            self._raise_generic_error(e, None)

        error_code = getattr(error_obj, "code", None)

        if not error_code:
            self._raise_generic_error(e, None)

        if error_code == 1:
            self._raise_unique_violation(e, error_code)
        elif error_code in {2291, 2292}:
            self._raise_foreign_key_violation(e, error_code)
        elif error_code == ORA_CHECK_CONSTRAINT:
            self._raise_check_violation(e, error_code)
        elif error_code in {1400, 1407}:
            self._raise_not_null_violation(e, error_code)
        elif error_code and ORA_INTEGRITY_RANGE_START <= error_code < ORA_INTEGRITY_RANGE_END:
            self._raise_integrity_error(e, error_code)
        elif error_code in {1017, 12154, 12541, 12545, 12514, 12505}:
            self._raise_connection_error(e, error_code)
        elif error_code in {60, 8176}:
            self._raise_transaction_error(e, error_code)
        elif error_code in {1722, 1858, 1840}:
            self._raise_data_error(e, error_code)
        elif error_code and ORA_PARSING_RANGE_START <= error_code < ORA_PARSING_RANGE_END:
            self._raise_parsing_error(e, error_code)
        elif error_code == ORA_TABLESPACE_FULL:
            self._raise_operational_error(e, error_code)
        else:
            self._raise_generic_error(e, error_code)

    def _raise_unique_violation(self, e: Any, code: int) -> None:
        msg = f"Oracle unique constraint violation [ORA-{code:05d}]: {e}"
        raise UniqueViolationError(msg) from e

    def _raise_foreign_key_violation(self, e: Any, code: int) -> None:
        msg = f"Oracle foreign key constraint violation [ORA-{code:05d}]: {e}"
        raise ForeignKeyViolationError(msg) from e

    def _raise_check_violation(self, e: Any, code: int) -> None:
        msg = f"Oracle check constraint violation [ORA-{code:05d}]: {e}"
        raise CheckViolationError(msg) from e

    def _raise_not_null_violation(self, e: Any, code: int) -> None:
        msg = f"Oracle not-null constraint violation [ORA-{code:05d}]: {e}"
        raise NotNullViolationError(msg) from e

    def _raise_integrity_error(self, e: Any, code: int) -> None:
        msg = f"Oracle integrity constraint violation [ORA-{code:05d}]: {e}"
        raise IntegrityError(msg) from e

    def _raise_parsing_error(self, e: Any, code: int) -> None:
        msg = f"Oracle SQL syntax error [ORA-{code:05d}]: {e}"
        raise SQLParsingError(msg) from e

    def _raise_connection_error(self, e: Any, code: int) -> None:
        msg = f"Oracle connection error [ORA-{code:05d}]: {e}"
        raise DatabaseConnectionError(msg) from e

    def _raise_transaction_error(self, e: Any, code: int) -> None:
        msg = f"Oracle transaction error [ORA-{code:05d}]: {e}"
        raise TransactionError(msg) from e

    def _raise_data_error(self, e: Any, code: int) -> None:
        msg = f"Oracle data error [ORA-{code:05d}]: {e}"
        raise DataError(msg) from e

    def _raise_operational_error(self, e: Any, code: int) -> None:
        msg = f"Oracle operational error [ORA-{code:05d}]: {e}"
        raise OperationalError(msg) from e

    def _raise_generic_error(self, e: Any, code: "int | None") -> None:
        msg = f"Oracle database error [ORA-{code:05d}]: {e}" if code else f"Oracle database error: {e}"
        raise SQLSpecError(msg) from e


class OracleSyncExceptionHandler(OracleExceptionHandler):
    """Sync Context manager for handling Oracle database exceptions.

    Maps Oracle ORA-XXXXX error codes to specific SQLSpec exceptions
    for better error handling in application code.
    """

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = exc_tb
        if exc_type is None:
            return
        if issubclass(exc_type, oracledb.DatabaseError):
            self._map_oracle_exception(exc_val)


class OracleAsyncExceptionHandler(OracleExceptionHandler):
    """Async context manager for handling Oracle database exceptions.

    Maps Oracle ORA-XXXXX error codes to specific SQLSpec exceptions
    for better error handling in application code.
    """

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = exc_tb
        if exc_type is None:
            return
        if issubclass(exc_type, oracledb.DatabaseError):
            self._map_oracle_exception(exc_val)


class OracleSyncDriver(OraclePipelineMixin, SyncDriverAdapterBase):
    """Synchronous Oracle Database driver.

    Provides Oracle Database connectivity with parameter style conversion,
    error handling, and transaction management.
    """

    __slots__ = ("_data_dictionary", "_oracle_version", "_pipeline_support", "_pipeline_support_reason")
    dialect = "oracle"

    def __init__(
        self,
        connection: OracleSyncConnection,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            cache_config = get_cache_config()
            statement_config = oracledb_statement_config.replace(
                enable_caching=cache_config.compiled_cache_enabled,
                enable_parsing=True,
                enable_validation=True,
                dialect="oracle",
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: SyncDataDictionaryBase | None = None
        self._pipeline_support: bool | None = None
        self._pipeline_support_reason: str | None = None
        self._oracle_version: VersionInfo | None = None

    def with_cursor(self, connection: OracleSyncConnection) -> OracleSyncCursor:
        """Create context manager for Oracle cursor.

        Args:
            connection: Oracle database connection

        Returns:
            Context manager for cursor operations
        """
        return OracleSyncCursor(connection)

    def handle_database_exceptions(self) -> "AbstractContextManager[None]":
        """Handle database-specific exceptions and wrap them appropriately."""
        return OracleSyncExceptionHandler()

    def _try_special_handling(self, cursor: Any, statement: "SQL") -> "SQLResult | None":
        """Hook for Oracle-specific special operations.

        Oracle doesn't have complex special operations like PostgreSQL COPY,
        so this always returns None to proceed with standard execution.

        Args:
            cursor: Oracle cursor object
            statement: SQL statement to analyze

        Returns:
            None - always proceeds with standard execution for Oracle
        """
        _ = (cursor, statement)  # Mark as intentionally unused
        return None

    def _execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL script with statement splitting and parameter handling.

        Parameters are embedded as static values for script execution compatibility.

        Args:
            cursor: Oracle cursor object
            statement: SQL script statement to execute

        Returns:
            Execution result containing statement count and success information
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_cursor = cursor

        for stmt in statements:
            cursor.execute(stmt, prepared_parameters or {})
            successful_count += 1

        return self.create_execution_result(
            last_cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def execute_stack(self, stack: "StatementStack", *, continue_on_error: bool = False) -> "tuple[StackResult, ...]":
        """Execute a StatementStack using Oracle's pipeline when available."""

        if not isinstance(stack, StatementStack) or not stack:
            return super().execute_stack(stack, continue_on_error=continue_on_error)

        blocker = self._stack_native_blocker(stack)
        if blocker is not None:
            self._log_pipeline_skip(blocker, stack)
            return super().execute_stack(stack, continue_on_error=continue_on_error)

        if not self._pipeline_native_supported():
            self._log_pipeline_skip(self._pipeline_support_reason or "database_version", stack)
            return super().execute_stack(stack, continue_on_error=continue_on_error)

        return self._execute_stack_native(stack, continue_on_error=continue_on_error)

    def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using Oracle batch processing.

        Args:
            cursor: Oracle cursor object
            statement: SQL statement with multiple parameter sets

        Returns:
            Execution result with affected row count

        Raises:
            ValueError: If no parameters are provided
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        # Parameter validation for executemany
        if not prepared_parameters:
            msg = "execute_many requires parameters"
            raise ValueError(msg)

        # Oracle-specific fix: Ensure parameters are in list format for executemany
        # Oracle expects a list of sequences, not a tuple of sequences
        if isinstance(prepared_parameters, tuple):
            prepared_parameters = list(prepared_parameters)

        cursor.executemany(sql, prepared_parameters)

        # Calculate affected rows based on parameter count
        affected_rows = len(prepared_parameters) if prepared_parameters else 0

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    def _execute_stack_native(self, stack: "StatementStack", *, continue_on_error: bool) -> "tuple[StackResult, ...]":
        compiled_operations = [self._prepare_pipeline_operation(op) for op in stack.operations]
        pipeline = oracledb.create_pipeline()
        for compiled in compiled_operations:
            self._add_pipeline_operation(pipeline, compiled)

        results: list[StackResult] = []
        started_transaction = False

        with StackExecutionObserver(self, stack, continue_on_error, native_pipeline=True) as observer:
            try:
                if not continue_on_error and not self._connection_in_transaction():
                    self.begin()
                    started_transaction = True

                pipeline_results = self.connection.run_pipeline(pipeline, continue_on_error=continue_on_error)
                results = self._build_stack_results_from_pipeline(
                    compiled_operations, pipeline_results, continue_on_error, observer
                )

                if started_transaction:
                    self.commit()
            except Exception as exc:
                if started_transaction:
                    try:
                        self.rollback()
                    except Exception as rollback_error:  # pragma: no cover - diagnostics only
                        logger.debug("Rollback after pipeline failure failed: %s", rollback_error)
                raise self._wrap_pipeline_error(exc, stack, continue_on_error) from exc

        return tuple(results)

    def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement with Oracle data handling.

        Args:
            cursor: Oracle cursor object
            statement: SQL statement to execute

        Returns:
            Execution result containing data for SELECT statements or row count for others
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        # Oracle-specific: Use setinputsizes for large string parameters to avoid ORA-01704
        if prepared_parameters and isinstance(prepared_parameters, dict):
            for param_name, param_value in prepared_parameters.items():
                if isinstance(param_value, str) and len(param_value) > LARGE_STRING_THRESHOLD:
                    clob = self.connection.createlob(oracledb.DB_TYPE_CLOB)
                    clob.write(param_value)
                    prepared_parameters[param_name] = clob

        cursor.execute(sql, prepared_parameters or {})

        # SELECT result processing for Oracle
        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]
            column_names = _normalize_column_names(column_names, self.driver_features)

            # Oracle returns tuples - convert to consistent dict format after LOB hydration
            data = [dict(zip(column_names, _coerce_sync_row_values(row), strict=False)) for row in fetched_data]

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        # Non-SELECT result processing
        affected_rows = cursor.rowcount if cursor.rowcount is not None else 0
        return self.create_execution_result(cursor, rowcount_override=affected_rows)

    def select_to_storage(
        self,
        statement: "Statement | QueryBuilder | SQL | str",
        destination: "StorageDestination",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        partitioner: "dict[str, Any] | None" = None,
        format_hint: "StorageFormat | None" = None,
        telemetry: "StorageTelemetry | None" = None,
        **kwargs: Any,
    ) -> "StorageBridgeJob":
        """Execute a query and stream Arrow-formatted output to storage (sync)."""

        self._require_capability("arrow_export_enabled")
        arrow_result = self.select_to_arrow(statement, *parameters, statement_config=statement_config, **kwargs)
        sync_pipeline: SyncStoragePipeline = cast("SyncStoragePipeline", self._storage_pipeline())
        telemetry_payload = self._write_result_to_storage_sync(
            arrow_result, destination, format_hint=format_hint, pipeline=sync_pipeline
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload, telemetry)

    def _detect_oracle_version(self) -> "VersionInfo | None":
        if self._oracle_version is not None:
            return self._oracle_version
        version = self.data_dictionary.get_version(self)
        self._oracle_version = version
        return version

    def _detect_oracledb_version(self) -> "tuple[int, int, int]":
        return _ORACLEDB_VERSION

    def _pipeline_native_supported(self) -> bool:
        if self._pipeline_support is not None:
            return self._pipeline_support

        if self.stack_native_disabled:
            self._pipeline_support = False
            self._pipeline_support_reason = "env_override"
            return False

        if self._detect_oracledb_version() < PIPELINE_MIN_DRIVER_VERSION:
            self._pipeline_support = False
            self._pipeline_support_reason = "driver_version"
            return False

        if not hasattr(self.connection, "run_pipeline"):
            self._pipeline_support = False
            self._pipeline_support_reason = "driver_api_missing"
            return False

        version_info = self._detect_oracle_version()
        if version_info and version_info.major >= PIPELINE_MIN_DATABASE_MAJOR:
            self._pipeline_support = True
            self._pipeline_support_reason = None
            return True

        self._pipeline_support = False
        self._pipeline_support_reason = "database_version"
        return False

    def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, Any] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        """Load Arrow data into Oracle using batched executemany calls."""

        self._require_capability("arrow_import_enabled")
        arrow_table = self._coerce_arrow_table(source)
        if overwrite:
            self._truncate_table_sync(table)
        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            statement = _oracle_insert_statement(table, columns)
            with self.with_cursor(self.connection) as cursor, self.handle_database_exceptions():
                cursor.executemany(statement, records)
        telemetry_payload = self._build_ingest_telemetry(arrow_table)
        telemetry_payload["destination"] = table
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload, telemetry)

    def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        partitioner: "dict[str, Any] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        """Load staged artifacts into Oracle."""

        arrow_table, inbound = self._read_arrow_from_storage_sync(source, file_format=file_format)
        return self.load_from_arrow(table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound)

    # Oracle transaction management
    def begin(self) -> None:
        """Begin a database transaction.

        Oracle handles transactions automatically, so this is a no-op.
        """
        # Oracle handles transactions implicitly

    def rollback(self) -> None:
        """Rollback the current transaction.

        Raises:
            SQLSpecError: If rollback fails
        """
        try:
            self.connection.rollback()
        except oracledb.Error as e:
            msg = f"Failed to rollback Oracle transaction: {e}"
            raise SQLSpecError(msg) from e

    def commit(self) -> None:
        """Commit the current transaction.

        Raises:
            SQLSpecError: If commit fails
        """
        try:
            self.connection.commit()
        except oracledb.Error as e:
            msg = f"Failed to commit Oracle transaction: {e}"
            raise SQLSpecError(msg) from e

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
    ) -> "Any":
        """Execute query and return results as Apache Arrow format using Oracle native support.

        This implementation uses Oracle's native fetch_df_all() method which returns
        an OracleDataFrame with Arrow PyCapsule interface, providing zero-copy data
        transfer and 5-10x performance improvement over dict conversion.

        Args:
            statement: SQL query string, Statement, or QueryBuilder
            *parameters: Query parameters (same format as execute()/select())
            statement_config: Optional statement configuration override
            return_format: "table" for pyarrow.Table (default), "batches" for RecordBatch
            native_only: If False, use base conversion path instead of native (default: False uses native)
            batch_size: Rows per batch when using "batches" format
            arrow_schema: Optional pyarrow.Schema for type casting
            **kwargs: Additional keyword arguments

        Returns:
            ArrowResult containing pyarrow.Table or RecordBatch

        Examples:
            >>> result = driver.select_to_arrow(
            ...     "SELECT * FROM users WHERE age > :1", (18,)
            ... )
            >>> df = result.to_pandas()
            >>> print(df.head())
        """
        # Check pyarrow is available
        ensure_pyarrow()

        # If native_only=False explicitly passed, use base conversion path
        if native_only is False:
            return super().select_to_arrow(
                statement,
                *parameters,
                statement_config=statement_config,
                return_format=return_format,
                native_only=native_only,
                batch_size=batch_size,
                arrow_schema=arrow_schema,
                **kwargs,
            )

        import pyarrow as pa

        # Prepare statement with parameters
        config = statement_config or self.statement_config
        prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
        sql, prepared_parameters = self._get_compiled_sql(prepared_statement, config)

        # Use Oracle's native fetch_df_all() for zero-copy Arrow transfer
        oracle_df = self.connection.fetch_df_all(
            statement=sql, parameters=prepared_parameters or [], arraysize=batch_size or 1000
        )

        # Convert OracleDataFrame to PyArrow Table using PyCapsule interface
        arrow_table = pa.table(oracle_df)

        # Apply schema casting if provided
        if arrow_schema is not None:
            if not isinstance(arrow_schema, pa.Schema):
                msg = f"arrow_schema must be a pyarrow.Schema, got {type(arrow_schema).__name__}"
                raise TypeError(msg)
            arrow_table = arrow_table.cast(arrow_schema)

        # Convert to batches if requested
        if return_format == "batches":
            batches = arrow_table.to_batches()
            arrow_data: Any = batches[0] if batches else pa.RecordBatch.from_pydict({})
        else:
            arrow_data = arrow_table

        # Get row count
        rows_affected = len(arrow_table)

        return create_arrow_result(statement=prepared_statement, data=arrow_data, rows_affected=rows_affected)

    @property
    def data_dictionary(self) -> "SyncDataDictionaryBase":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = OracleSyncDataDictionary()
        return self._data_dictionary

    def _truncate_table_sync(self, table: str) -> None:
        statement = _oracle_truncate_statement(table)
        with self.handle_database_exceptions():
            self.connection.execute(statement)


class OracleAsyncDriver(OraclePipelineMixin, AsyncDriverAdapterBase):
    """Asynchronous Oracle Database driver.

    Provides Oracle Database connectivity with parameter style conversion,
    error handling, and transaction management for async operations.
    """

    __slots__ = ("_data_dictionary", "_oracle_version", "_pipeline_support", "_pipeline_support_reason")
    dialect = "oracle"

    def __init__(
        self,
        connection: OracleAsyncConnection,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            cache_config = get_cache_config()
            statement_config = oracledb_statement_config.replace(
                enable_caching=cache_config.compiled_cache_enabled,
                enable_parsing=True,
                enable_validation=True,
                dialect="oracle",
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: AsyncDataDictionaryBase | None = None
        self._pipeline_support: bool | None = None
        self._pipeline_support_reason: str | None = None
        self._oracle_version: VersionInfo | None = None

    def with_cursor(self, connection: OracleAsyncConnection) -> OracleAsyncCursor:
        """Create context manager for Oracle cursor.

        Args:
            connection: Oracle database connection

        Returns:
            Context manager for cursor operations
        """
        return OracleAsyncCursor(connection)

    def handle_database_exceptions(self) -> "AbstractAsyncContextManager[None]":
        """Handle database-specific exceptions and wrap them appropriately."""
        return OracleAsyncExceptionHandler()

    async def _try_special_handling(self, cursor: Any, statement: "SQL") -> "SQLResult | None":
        """Hook for Oracle-specific special operations.

        Oracle doesn't have complex special operations like PostgreSQL COPY,
        so this always returns None to proceed with standard execution.

        Args:
            cursor: Oracle cursor object
            statement: SQL statement to analyze

        Returns:
            None - always proceeds with standard execution for Oracle
        """
        _ = (cursor, statement)  # Mark as intentionally unused
        return None

    async def _execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL script with statement splitting and parameter handling.

        Parameters are embedded as static values for script execution compatibility.

        Args:
            cursor: Oracle cursor object
            statement: SQL script statement to execute

        Returns:
            Execution result containing statement count and success information
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_cursor = cursor

        for stmt in statements:
            await cursor.execute(stmt, prepared_parameters or {})
            successful_count += 1

        return self.create_execution_result(
            last_cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    async def execute_stack(
        self, stack: "StatementStack", *, continue_on_error: bool = False
    ) -> "tuple[StackResult, ...]":
        """Execute a StatementStack using Oracle's pipeline when available."""

        if not isinstance(stack, StatementStack) or not stack:
            return await super().execute_stack(stack, continue_on_error=continue_on_error)

        blocker = self._stack_native_blocker(stack)
        if blocker is not None:
            self._log_pipeline_skip(blocker, stack)
            return await super().execute_stack(stack, continue_on_error=continue_on_error)

        if not await self._pipeline_native_supported():
            self._log_pipeline_skip(self._pipeline_support_reason or "database_version", stack)
            return await super().execute_stack(stack, continue_on_error=continue_on_error)

        return await self._execute_stack_native(stack, continue_on_error=continue_on_error)

    async def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using Oracle batch processing.

        Args:
            cursor: Oracle cursor object
            statement: SQL statement with multiple parameter sets

        Returns:
            Execution result with affected row count

        Raises:
            ValueError: If no parameters are provided
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        # Parameter validation for executemany
        if not prepared_parameters:
            msg = "execute_many requires parameters"
            raise ValueError(msg)

        await cursor.executemany(sql, prepared_parameters)

        # Calculate affected rows based on parameter count
        affected_rows = len(prepared_parameters) if prepared_parameters else 0

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    async def _execute_stack_native(
        self, stack: "StatementStack", *, continue_on_error: bool
    ) -> "tuple[StackResult, ...]":
        compiled_operations = [self._prepare_pipeline_operation(op) for op in stack.operations]
        pipeline = oracledb.create_pipeline()
        for compiled in compiled_operations:
            self._add_pipeline_operation(pipeline, compiled)

        results: list[StackResult] = []
        started_transaction = False

        with StackExecutionObserver(self, stack, continue_on_error, native_pipeline=True) as observer:
            try:
                if not continue_on_error and not self._connection_in_transaction():
                    await self.begin()
                    started_transaction = True

                pipeline_results = await self.connection.run_pipeline(pipeline, continue_on_error=continue_on_error)
                results = self._build_stack_results_from_pipeline(
                    compiled_operations, pipeline_results, continue_on_error, observer
                )

                if started_transaction:
                    await self.commit()
            except Exception as exc:
                if started_transaction:
                    try:
                        await self.rollback()
                    except Exception as rollback_error:  # pragma: no cover - diagnostics only
                        logger.debug("Rollback after pipeline failure failed: %s", rollback_error)
                raise self._wrap_pipeline_error(exc, stack, continue_on_error) from exc

        return tuple(results)

    async def _pipeline_native_supported(self) -> bool:
        if self._pipeline_support is not None:
            return self._pipeline_support

        if self.stack_native_disabled:
            self._pipeline_support = False
            self._pipeline_support_reason = "env_override"
            return False

        if self._detect_oracledb_version() < PIPELINE_MIN_DRIVER_VERSION:
            self._pipeline_support = False
            self._pipeline_support_reason = "driver_version"
            return False

        if not hasattr(self.connection, "run_pipeline"):
            self._pipeline_support = False
            self._pipeline_support_reason = "driver_api_missing"
            return False

        version_info = await self._detect_oracle_version()
        if version_info and version_info.major >= PIPELINE_MIN_DATABASE_MAJOR:
            self._pipeline_support = True
            self._pipeline_support_reason = None
            return True

        self._pipeline_support = False
        self._pipeline_support_reason = "database_version"
        return False

    async def _detect_oracle_version(self) -> "VersionInfo | None":
        if self._oracle_version is not None:
            return self._oracle_version
        version = await self.data_dictionary.get_version(self)
        self._oracle_version = version
        return version

    def _detect_oracledb_version(self) -> "tuple[int, int, int]":
        return _ORACLEDB_VERSION

    async def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement with Oracle data handling.

        Args:
            cursor: Oracle cursor object
            statement: SQL statement to execute

        Returns:
            Execution result containing data for SELECT statements or row count for others
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        # Oracle-specific: Use setinputsizes for large string parameters to avoid ORA-01704
        if prepared_parameters and isinstance(prepared_parameters, dict):
            for param_name, param_value in prepared_parameters.items():
                if isinstance(param_value, str) and len(param_value) > LARGE_STRING_THRESHOLD:
                    clob = await self.connection.createlob(oracledb.DB_TYPE_CLOB)
                    await clob.write(param_value)
                    prepared_parameters[param_name] = clob

        await cursor.execute(sql, prepared_parameters or {})

        # SELECT result processing for Oracle
        is_select_like = statement.returns_rows() or self._should_force_select(statement, cursor)

        if is_select_like:
            fetched_data = await cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]
            column_names = _normalize_column_names(column_names, self.driver_features)

            # Oracle returns tuples - convert to consistent dict format after LOB hydration
            data = []
            for row in fetched_data:
                coerced_row = await _coerce_async_row_values(row)
                data.append(dict(zip(column_names, coerced_row, strict=False)))

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        # Non-SELECT result processing
        affected_rows = cursor.rowcount if cursor.rowcount is not None else 0
        return self.create_execution_result(cursor, rowcount_override=affected_rows)

    async def select_to_storage(
        self,
        statement: "Statement | QueryBuilder | SQL | str",
        destination: "StorageDestination",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        partitioner: "dict[str, Any] | None" = None,
        format_hint: "StorageFormat | None" = None,
        telemetry: "StorageTelemetry | None" = None,
        **kwargs: Any,
    ) -> "StorageBridgeJob":
        """Execute a query and write Arrow-compatible output to storage (async)."""

        self._require_capability("arrow_export_enabled")
        arrow_result = await self.select_to_arrow(statement, *parameters, statement_config=statement_config, **kwargs)
        async_pipeline: AsyncStoragePipeline = cast("AsyncStoragePipeline", self._storage_pipeline())
        telemetry_payload = await self._write_result_to_storage_async(
            arrow_result, destination, format_hint=format_hint, pipeline=async_pipeline
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload, telemetry)

    async def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, Any] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        """Asynchronously load Arrow data into Oracle."""

        self._require_capability("arrow_import_enabled")
        arrow_table = self._coerce_arrow_table(source)
        if overwrite:
            await self._truncate_table_async(table)
        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            statement = _oracle_insert_statement(table, columns)
            async with self.with_cursor(self.connection) as cursor, self.handle_database_exceptions():
                await cursor.executemany(statement, records)
        telemetry_payload = self._build_ingest_telemetry(arrow_table)
        telemetry_payload["destination"] = table
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload, telemetry)

    async def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        partitioner: "dict[str, Any] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        """Asynchronously load staged artifacts into Oracle."""

        arrow_table, inbound = await self._read_arrow_from_storage_async(source, file_format=file_format)
        return await self.load_from_arrow(
            table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound
        )

    # Oracle transaction management
    async def begin(self) -> None:
        """Begin a database transaction.

        Oracle handles transactions automatically, so this is a no-op.
        """
        # Oracle handles transactions implicitly

    async def rollback(self) -> None:
        """Rollback the current transaction.

        Raises:
            SQLSpecError: If rollback fails
        """
        try:
            await self.connection.rollback()
        except oracledb.Error as e:
            msg = f"Failed to rollback Oracle transaction: {e}"
            raise SQLSpecError(msg) from e

    async def commit(self) -> None:
        """Commit the current transaction.

        Raises:
            SQLSpecError: If commit fails
        """
        try:
            await self.connection.commit()
        except oracledb.Error as e:
            msg = f"Failed to commit Oracle transaction: {e}"
            raise SQLSpecError(msg) from e

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
    ) -> "Any":
        """Execute query and return results as Apache Arrow format using Oracle native support.

        This implementation uses Oracle's native fetch_df_all() method which returns
        an OracleDataFrame with Arrow PyCapsule interface, providing zero-copy data
        transfer and 5-10x performance improvement over dict conversion.

        Args:
            statement: SQL query string, Statement, or QueryBuilder
            *parameters: Query parameters (same format as execute()/select())
            statement_config: Optional statement configuration override
            return_format: "table" for pyarrow.Table (default), "batches" for RecordBatch
            native_only: If False, use base conversion path instead of native (default: False uses native)
            batch_size: Rows per batch when using "batches" format
            arrow_schema: Optional pyarrow.Schema for type casting
            **kwargs: Additional keyword arguments

        Returns:
            ArrowResult containing pyarrow.Table or RecordBatch

        Examples:
            >>> result = await driver.select_to_arrow(
            ...     "SELECT * FROM users WHERE age > :1", (18,)
            ... )
            >>> df = result.to_pandas()
            >>> print(df.head())
        """
        # Check pyarrow is available
        ensure_pyarrow()

        # If native_only=False explicitly passed, use base conversion path
        if native_only is False:
            return await super().select_to_arrow(
                statement,
                *parameters,
                statement_config=statement_config,
                return_format=return_format,
                native_only=native_only,
                batch_size=batch_size,
                arrow_schema=arrow_schema,
                **kwargs,
            )

        import pyarrow as pa

        # Prepare statement with parameters
        config = statement_config or self.statement_config
        prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
        sql, prepared_parameters = self._get_compiled_sql(prepared_statement, config)

        # Use Oracle's native fetch_df_all() for zero-copy Arrow transfer
        oracle_df = await self.connection.fetch_df_all(
            statement=sql, parameters=prepared_parameters or [], arraysize=batch_size or 1000
        )

        # Convert OracleDataFrame to PyArrow Table using PyCapsule interface
        arrow_table = pa.table(oracle_df)

        # Apply schema casting if provided
        if arrow_schema is not None:
            if not isinstance(arrow_schema, pa.Schema):
                msg = f"arrow_schema must be a pyarrow.Schema, got {type(arrow_schema).__name__}"
                raise TypeError(msg)
            arrow_table = arrow_table.cast(arrow_schema)

        # Convert to batches if requested
        if return_format == "batches":
            batches = arrow_table.to_batches()
            arrow_data: Any = batches[0] if batches else pa.RecordBatch.from_pydict({})
        else:
            arrow_data = arrow_table

        # Get row count
        rows_affected = len(arrow_table)

        return create_arrow_result(statement=prepared_statement, data=arrow_data, rows_affected=rows_affected)

    @property
    def data_dictionary(self) -> "AsyncDataDictionaryBase":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = OracleAsyncDataDictionary()
        return self._data_dictionary

    async def _truncate_table_async(self, table: str) -> None:
        statement = _oracle_truncate_statement(table)
        async with self.handle_database_exceptions():
            await self.connection.execute(statement)


def _build_oracledb_profile() -> DriverParameterProfile:
    """Create the OracleDB driver parameter profile."""

    return DriverParameterProfile(
        name="OracleDB",
        default_style=ParameterStyle.POSITIONAL_COLON,
        supported_styles={ParameterStyle.NAMED_COLON, ParameterStyle.POSITIONAL_COLON, ParameterStyle.QMARK},
        default_execution_style=ParameterStyle.NAMED_COLON,
        supported_execution_styles={ParameterStyle.NAMED_COLON, ParameterStyle.POSITIONAL_COLON},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        default_dialect="oracle",
    )


_ORACLE_PROFILE = _build_oracledb_profile()

register_driver_profile("oracledb", _ORACLE_PROFILE)

oracledb_statement_config = build_statement_config_from_profile(
    _ORACLE_PROFILE, statement_overrides={"dialect": "oracle"}, json_serializer=to_json
)
