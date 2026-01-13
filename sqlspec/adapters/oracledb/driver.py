"""Oracle Driver"""

import contextlib
import logging
from typing import TYPE_CHECKING, Any, NamedTuple, cast

import oracledb
from oracledb import AsyncCursor, Cursor

from sqlspec.adapters.oracledb._typing import (
    OracleAsyncConnection,
    OracleAsyncSessionContext,
    OracleSyncConnection,
    OracleSyncSessionContext,
)
from sqlspec.adapters.oracledb.core import (
    ORACLEDB_VERSION,
    build_insert_statement,
    build_pipeline_stack_result,
    build_truncate_statement,
    coerce_large_string_parameters_async,
    coerce_large_string_parameters_sync,
    collect_async_rows,
    collect_sync_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    normalize_column_names,
    normalize_execute_many_parameters_async,
    normalize_execute_many_parameters_sync,
    resolve_rowcount,
)
from sqlspec.adapters.oracledb.data_dictionary import OracledbAsyncDataDictionary, OracledbSyncDataDictionary
from sqlspec.core import (
    SQL,
    StackResult,
    StatementConfig,
    StatementStack,
    build_arrow_result_from_table,
    get_cache_config,
    register_driver_profile,
)
from sqlspec.driver import (
    AsyncDriverAdapterBase,
    StackExecutionObserver,
    SyncDriverAdapterBase,
    describe_stack_statement,
    hash_stack_operations,
)
from sqlspec.exceptions import ImproperConfigurationError, SQLSpecError, StackExecutionError
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.type_guards import has_pipeline_capability

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.adapters.oracledb._typing import OraclePipelineDriver
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, Statement, StatementConfig, StatementFilter
    from sqlspec.core.stack import StackOperation
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.typing import ArrowReturnFormat, StatementParameters, VersionInfo


logger = get_logger(__name__)

# Oracle-specific constants
LARGE_STRING_THRESHOLD = 4000  # Threshold for large string parameters to avoid ORA-01704

__all__ = (
    "OracleAsyncDriver",
    "OracleAsyncExceptionHandler",
    "OracleAsyncSessionContext",
    "OracleSyncDriver",
    "OracleSyncExceptionHandler",
    "OracleSyncSessionContext",
)

PIPELINE_MIN_DRIVER_VERSION: "tuple[int, int, int]" = (2, 4, 0)
PIPELINE_MIN_DATABASE_MAJOR: int = 23


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
        driver = cast("OraclePipelineDriver", self)
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
            if isinstance(operation.statement, SQL):
                statement_seed = operation.statement.raw_expression or operation.statement.raw_sql
                sql_statement = SQL(statement_seed, parameter_sets, statement_config=config, is_many=True, **kwargs)
            else:
                base_statement = driver.prepare_statement(
                    operation.statement, filters, statement_config=config, kwargs=kwargs
                )
                statement_seed = base_statement.raw_expression or base_statement.raw_sql
                sql_statement = SQL(statement_seed, parameter_sets, statement_config=config, is_many=True, **kwargs)
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
        driver = cast("OraclePipelineDriver", self)
        stack_results: list[StackResult] = []
        for index, (compiled, result) in enumerate(zip(compiled_operations, pipeline_results, strict=False)):
            try:
                error = result.error
            except AttributeError:
                error = None
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

            stack_results.append(
                build_pipeline_stack_result(
                    compiled.statement,
                    compiled.method,
                    compiled.returns_rows,
                    compiled.parameters,
                    result,
                    driver.driver_features,
                )
            )
        return stack_results

    def _wrap_pipeline_error(
        self, error: Exception, stack: "StatementStack", continue_on_error: bool
    ) -> StackExecutionError:
        mode = "continue-on-error" if continue_on_error else "fail-fast"
        return StackExecutionError(
            -1, "Oracle pipeline execution failed", error, adapter=type(self).__name__, mode=mode
        )


class OracleSyncCursor:
    """Sync context manager for Oracle cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: OracleSyncConnection) -> None:
        self.connection = connection
        self.cursor: Cursor | None = None

    def __enter__(self) -> Cursor:
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: object) -> None:
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


class OracleSyncExceptionHandler:
    """Sync Context manager for handling Oracle database exceptions.

    Maps Oracle ORA-XXXXX error codes to specific SQLSpec exceptions
    for better error handling in application code.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __exit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    def __enter__(self) -> "OracleSyncExceptionHandler":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        _ = exc_tb
        if exc_type is None:
            return False
        if issubclass(exc_type, oracledb.DatabaseError):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class OracleAsyncExceptionHandler:
    """Async context manager for handling Oracle database exceptions.

    Maps Oracle ORA-XXXXX error codes to specific SQLSpec exceptions
    for better error handling in application code.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __aexit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    async def __aenter__(self) -> "OracleAsyncExceptionHandler":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        _ = exc_tb
        if exc_type is None:
            return False
        if issubclass(exc_type, oracledb.DatabaseError):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


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
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: OracledbSyncDataDictionary | None = None
        self._pipeline_support: bool | None = None
        self._pipeline_support_reason: str | None = None
        self._oracle_version: VersionInfo | None = None

    # ─────────────────────────────────────────────────────────────────────────────
    # CORE DISPATCH METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def dispatch_execute(self, cursor: "Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement with Oracle data handling.

        Args:
            cursor: Oracle cursor object
            statement: SQL statement to execute

        Returns:
            Execution result containing data for SELECT statements or row count for others

        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        prepared_parameters = coerce_large_string_parameters_sync(
            self.connection, prepared_parameters, lob_type=oracledb.DB_TYPE_CLOB, threshold=LARGE_STRING_THRESHOLD
        )
        prepared_parameters = cast("list[Any] | tuple[Any, ...] | dict[Any, Any] | None", prepared_parameters)

        cursor.execute(sql, prepared_parameters or {})

        # SELECT result processing for Oracle
        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            data, column_names = collect_sync_rows(
                cast("list[Any] | None", fetched_data), cursor.description, self.driver_features
            )

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        # Non-SELECT result processing
        affected_rows = resolve_rowcount(cursor)
        return self.create_execution_result(cursor, rowcount_override=affected_rows)

    def dispatch_execute_many(self, cursor: "Cursor", statement: "SQL") -> "ExecutionResult":
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

        prepared_parameters = normalize_execute_many_parameters_sync(prepared_parameters)
        cursor.executemany(sql, prepared_parameters)

        affected_rows = len(prepared_parameters)

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    def dispatch_execute_script(self, cursor: "Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute SQL script with statement splitting and parameter handling.

        Parameters are embedded as static values for script execution compatibility.

        Args:
            cursor: Oracle cursor object
            statement: SQL script statement to execute

        Returns:
            Execution result containing statement count and success information

        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        prepared_parameters = cast("list[Any] | tuple[Any, ...] | dict[Any, Any] | None", prepared_parameters)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_cursor = cursor

        for stmt in statements:
            cursor.execute(stmt, prepared_parameters or {})
            successful_count += 1

        return self.create_execution_result(
            last_cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    # ─────────────────────────────────────────────────────────────────────────────
    # TRANSACTION MANAGEMENT
    # ─────────────────────────────────────────────────────────────────────────────

    def begin(self) -> None:
        """Begin a database transaction.

        Oracle handles transactions automatically, so this is a no-op.
        """
        # Oracle handles transactions implicitly

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

    def with_cursor(self, connection: OracleSyncConnection) -> OracleSyncCursor:
        """Create context manager for Oracle cursor.

        Args:
            connection: Oracle database connection

        Returns:
            Context manager for cursor operations

        """
        return OracleSyncCursor(connection)

    def handle_database_exceptions(self) -> "OracleSyncExceptionHandler":
        """Handle database-specific exceptions and wrap them appropriately."""
        return OracleSyncExceptionHandler()

    # ─────────────────────────────────────────────────────────────────────────────
    # ARROW API METHODS
    # ─────────────────────────────────────────────────────────────────────────────

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

        This implementation uses Oracle's native execute_df()/fetch_df_all() methods
        which return OracleDataFrame objects with Arrow PyCapsule interface, providing
        zero-copy data transfer and 5-10x performance improvement over dict conversion.
        If native Arrow is unavailable and native_only is False, it falls back to the
        conversion path.

        Args:
            statement: SQL query string, Statement, or QueryBuilder
            *parameters: Query parameters (same format as execute()/select())
            statement_config: Optional statement configuration override
            return_format: "table" for pyarrow.Table (default), "batch" for RecordBatch,
                "batches" for list of RecordBatch, "reader" for RecordBatchReader
            native_only: If True, raise error if native Arrow is unavailable
            batch_size: Rows per batch when using "batch" or "batches" format
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
        ensure_pyarrow()

        import pyarrow as pa

        config = statement_config or self.statement_config
        prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
        sql, prepared_parameters = self._get_compiled_sql(prepared_statement, config)

        try:
            oracle_df = self._execute_arrow_dataframe(sql, prepared_parameters, batch_size)
        except AttributeError as exc:
            if native_only:
                msg = "Oracle native Arrow support is not available for this connection."
                raise ImproperConfigurationError(msg) from exc
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

        arrow_table = pa.table(oracle_df)
        column_names = normalize_column_names(arrow_table.column_names, self.driver_features)
        if column_names != arrow_table.column_names:
            arrow_table = arrow_table.rename_columns(column_names)

        return build_arrow_result_from_table(
            prepared_statement,
            arrow_table,
            return_format=return_format,
            batch_size=batch_size,
            arrow_schema=arrow_schema,
        )

    # ─────────────────────────────────────────────────────────────────────────────
    # STACK EXECUTION METHODS
    # ─────────────────────────────────────────────────────────────────────────────

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

    # ─────────────────────────────────────────────────────────────────────────────
    # STORAGE API METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def select_to_storage(
        self,
        statement: "Statement | QueryBuilder | SQL | str",
        destination: "StorageDestination",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        partitioner: "dict[str, object] | None" = None,
        format_hint: "StorageFormat | None" = None,
        telemetry: "StorageTelemetry | None" = None,
        **kwargs: Any,
    ) -> "StorageBridgeJob":
        """Execute a query and stream Arrow-formatted output to storage (sync)."""
        self._require_capability("arrow_export_enabled")
        arrow_result = self.select_to_arrow(statement, *parameters, statement_config=statement_config, **kwargs)
        sync_pipeline = self._storage_pipeline()
        telemetry_payload = self._write_result_to_storage_sync(
            arrow_result, destination, format_hint=format_hint, pipeline=sync_pipeline
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload, telemetry)

    def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        """Load Arrow data into Oracle using batched executemany calls."""
        self._require_capability("arrow_import_enabled")
        arrow_table = self._coerce_arrow_table(source)
        if overwrite:
            statement = build_truncate_statement(table)
            with self.handle_database_exceptions():
                self.connection.execute(statement)
        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            statement = build_insert_statement(table, columns)
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
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        """Load staged artifacts into Oracle."""
        arrow_table, inbound = self._read_arrow_from_storage_sync(source, file_format=file_format)
        return self.load_from_arrow(table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound)

    # ─────────────────────────────────────────────────────────────────────────────
    # UTILITY METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    @property
    def data_dictionary(self) -> "OracledbSyncDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries

        """
        if self._data_dictionary is None:
            self._data_dictionary = OracledbSyncDataDictionary()
        return self._data_dictionary

    # ─────────────────────────────────────────────────────────────────────────────
    # PRIVATE/INTERNAL METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction."""
        return False

    def _detect_oracle_version(self) -> "VersionInfo | None":
        if self._oracle_version is not None:
            return self._oracle_version
        version = self.data_dictionary.get_version(self)
        self._oracle_version = version
        return version

    def _detect_oracledb_version(self) -> "tuple[int, int, int]":
        return ORACLEDB_VERSION

    def _execute_arrow_dataframe(self, sql: str, parameters: "Any", batch_size: int | None) -> "Any":
        """Execute SQL and return an Oracle DataFrame."""
        params = parameters if parameters is not None else []
        try:
            execute_df = self.connection.execute_df
        except AttributeError:
            execute_df = None
        if execute_df is not None:
            try:
                return execute_df(sql, params, arraysize=batch_size or 1000)
            except TypeError:
                return execute_df(sql, params)
        return self.connection.fetch_df_all(statement=sql, parameters=params, arraysize=batch_size or 1000)

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

        if not has_pipeline_capability(self.connection):
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
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: OracledbAsyncDataDictionary | None = None
        self._pipeline_support: bool | None = None
        self._pipeline_support_reason: str | None = None
        self._oracle_version: VersionInfo | None = None

    # ─────────────────────────────────────────────────────────────────────────────
    # CORE DISPATCH METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    async def dispatch_execute(self, cursor: "AsyncCursor", statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement with Oracle data handling.

        Args:
            cursor: Oracle cursor object
            statement: SQL statement to execute

        Returns:
            Execution result containing data for SELECT statements or row count for others

        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        prepared_parameters = await coerce_large_string_parameters_async(
            self.connection, prepared_parameters, lob_type=oracledb.DB_TYPE_CLOB, threshold=LARGE_STRING_THRESHOLD
        )
        prepared_parameters = cast("list[Any] | tuple[Any, ...] | dict[Any, Any] | None", prepared_parameters)

        await cursor.execute(sql, prepared_parameters or {})

        # SELECT result processing for Oracle
        is_select_like = statement.returns_rows() or self._should_force_select(statement, cursor)

        if is_select_like:
            fetched_data = await cursor.fetchall()
            data, column_names = await collect_async_rows(
                cast("list[Any] | None", fetched_data), cursor.description, self.driver_features
            )

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        # Non-SELECT result processing
        affected_rows = resolve_rowcount(cursor)
        return self.create_execution_result(cursor, rowcount_override=affected_rows)

    async def dispatch_execute_many(self, cursor: "AsyncCursor", statement: "SQL") -> "ExecutionResult":
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

        prepared_parameters = normalize_execute_many_parameters_async(prepared_parameters)
        await cursor.executemany(sql, prepared_parameters)

        affected_rows = len(prepared_parameters)

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    async def dispatch_execute_script(self, cursor: "AsyncCursor", statement: "SQL") -> "ExecutionResult":
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
        script_params = cast("dict[str, Any]", prepared_parameters or {})

        successful_count = 0
        last_cursor = cursor

        for stmt in statements:
            await cursor.execute(stmt, script_params)
            successful_count += 1

        return self.create_execution_result(
            last_cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    # ─────────────────────────────────────────────────────────────────────────────
    # TRANSACTION MANAGEMENT
    # ─────────────────────────────────────────────────────────────────────────────

    async def begin(self) -> None:
        """Begin a database transaction.

        Oracle handles transactions automatically, so this is a no-op.
        """
        # Oracle handles transactions implicitly

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

    def with_cursor(self, connection: OracleAsyncConnection) -> OracleAsyncCursor:
        """Create context manager for Oracle cursor.

        Args:
            connection: Oracle database connection

        Returns:
            Context manager for cursor operations

        """
        return OracleAsyncCursor(connection)

    def handle_database_exceptions(self) -> "OracleAsyncExceptionHandler":
        """Handle database-specific exceptions and wrap them appropriately."""
        return OracleAsyncExceptionHandler()

    # ─────────────────────────────────────────────────────────────────────────────
    # ARROW API METHODS
    # ─────────────────────────────────────────────────────────────────────────────

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

        This implementation uses Oracle's native execute_df()/fetch_df_all() methods
        which return OracleDataFrame objects with Arrow PyCapsule interface, providing
        zero-copy data transfer and 5-10x performance improvement over dict conversion.
        If native Arrow is unavailable and native_only is False, it falls back to the
        conversion path.

        Args:
            statement: SQL query string, Statement, or QueryBuilder
            *parameters: Query parameters (same format as execute()/select())
            statement_config: Optional statement configuration override
            return_format: "table" for pyarrow.Table (default), "batch" for RecordBatch,
                "batches" for list of RecordBatch, "reader" for RecordBatchReader
            native_only: If True, raise error if native Arrow is unavailable
            batch_size: Rows per batch when using "batch" or "batches" format
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
        ensure_pyarrow()

        import pyarrow as pa

        config = statement_config or self.statement_config
        prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
        sql, prepared_parameters = self._get_compiled_sql(prepared_statement, config)

        try:
            oracle_df = await self._execute_arrow_dataframe(sql, prepared_parameters, batch_size)
        except AttributeError as exc:
            if native_only:
                msg = "Oracle native Arrow support is not available for this connection."
                raise ImproperConfigurationError(msg) from exc
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

        arrow_table = pa.table(oracle_df)
        column_names = normalize_column_names(arrow_table.column_names, self.driver_features)
        if column_names != arrow_table.column_names:
            arrow_table = arrow_table.rename_columns(column_names)

        return build_arrow_result_from_table(
            prepared_statement,
            arrow_table,
            return_format=return_format,
            batch_size=batch_size,
            arrow_schema=arrow_schema,
        )

    # ─────────────────────────────────────────────────────────────────────────────
    # STACK EXECUTION METHODS
    # ─────────────────────────────────────────────────────────────────────────────

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

    # ─────────────────────────────────────────────────────────────────────────────
    # STORAGE API METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    async def select_to_storage(
        self,
        statement: "Statement | QueryBuilder | SQL | str",
        destination: "StorageDestination",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        partitioner: "dict[str, object] | None" = None,
        format_hint: "StorageFormat | None" = None,
        telemetry: "StorageTelemetry | None" = None,
        **kwargs: Any,
    ) -> "StorageBridgeJob":
        """Execute a query and write Arrow-compatible output to storage (async)."""
        self._require_capability("arrow_export_enabled")
        arrow_result = await self.select_to_arrow(statement, *parameters, statement_config=statement_config, **kwargs)
        async_pipeline = self._storage_pipeline()
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
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        """Asynchronously load Arrow data into Oracle."""
        self._require_capability("arrow_import_enabled")
        arrow_table = self._coerce_arrow_table(source)
        if overwrite:
            statement = build_truncate_statement(table)
            async with self.handle_database_exceptions():
                await self.connection.execute(statement)
        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            statement = build_insert_statement(table, columns)
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
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        """Asynchronously load staged artifacts into Oracle."""
        arrow_table, inbound = await self._read_arrow_from_storage_async(source, file_format=file_format)
        return await self.load_from_arrow(
            table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound
        )

    # ─────────────────────────────────────────────────────────────────────────────
    # UTILITY METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    @property
    def data_dictionary(self) -> "OracledbAsyncDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries

        """
        if self._data_dictionary is None:
            self._data_dictionary = OracledbAsyncDataDictionary()
        return self._data_dictionary

    # ─────────────────────────────────────────────────────────────────────────────
    # PRIVATE/INTERNAL METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction."""
        return False

    async def _detect_oracle_version(self) -> "VersionInfo | None":
        if self._oracle_version is not None:
            return self._oracle_version
        version = await self.data_dictionary.get_version(self)
        self._oracle_version = version
        return version

    def _detect_oracledb_version(self) -> "tuple[int, int, int]":
        return ORACLEDB_VERSION

    async def _execute_arrow_dataframe(self, sql: str, parameters: "Any", batch_size: int | None) -> "Any":
        """Execute SQL and return an Oracle DataFrame."""
        params = parameters if parameters is not None else []
        try:
            execute_df = self.connection.execute_df
        except AttributeError:
            execute_df = None
        if execute_df is not None:
            try:
                return await execute_df(sql, params, arraysize=batch_size or 1000)
            except TypeError:
                return await execute_df(sql, params)
        return await self.connection.fetch_df_all(statement=sql, parameters=params, arraysize=batch_size or 1000)

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

        if not has_pipeline_capability(self.connection):
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


register_driver_profile("oracledb", driver_profile)
