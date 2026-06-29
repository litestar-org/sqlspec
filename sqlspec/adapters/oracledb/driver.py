"""Oracle Driver"""

import logging
from typing import TYPE_CHECKING, Any, NamedTuple, cast

from sqlspec.adapters.oracledb._typing import (
    DB_TYPE_BLOB,
    DB_TYPE_CLOB,
    OracleAsyncConnection,
    OracleAsyncCursor,
    OracleAsyncSessionContext,
    OracleSyncConnection,
    OracleSyncCursor,
    OracleSyncSessionContext,
)
from sqlspec.adapters.oracledb._typing import DatabaseError as OracleDatabaseError
from sqlspec.adapters.oracledb._typing import Error as OracleError
from sqlspec.adapters.oracledb._typing import create_pipeline as create_oracle_pipeline
from sqlspec.adapters.oracledb.core import (
    ORACLEDB_VERSION,
    OracleAsyncStreamSource,
    OracleSyncStreamSource,
    build_arrow_fetch_kwargs,
    build_fetch_kwargs,
    build_insert_statement,
    build_pipeline_stack_result,
    build_truncate_statement,
    coerce_large_parameters_async,
    coerce_large_parameters_sync,
    collect_async_rows,
    collect_sync_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    normalize_column_names,
    normalize_execute_many_parameters_async,
    normalize_execute_many_parameters_sync,
    resolve_row_metadata,
    resolve_rowcount,
    supports_df_batches,
    supports_direct_path_load,
)
from sqlspec.adapters.oracledb.data_dictionary import OracledbAsyncDataDictionary, OracledbSyncDataDictionary
from sqlspec.core import (
    SQL,
    StackResult,
    StatementConfig,
    StatementStack,
    build_arrow_result_from_table,
    create_arrow_result,
    get_cache_config,
    register_driver_profile,
)
from sqlspec.driver import (
    AsyncDriverAdapterBase,
    AsyncRowStream,
    BaseAsyncExceptionHandler,
    BaseSyncExceptionHandler,
    StackExecutionObserver,
    SyncDriverAdapterBase,
    SyncRowStream,
    describe_stack_statement,
    hash_stack_operations,
)
from sqlspec.exceptions import ImproperConfigurationError, SQLSpecError, StackExecutionError
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.text import normalize_identifier, quote_identifier, split_qualified_identifier
from sqlspec.utils.type_guards import has_pipeline_capability

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.adapters.oracledb._typing import OraclePipelineDriver
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, Statement, StatementConfig, StatementFilter
    from sqlspec.core.stack import StackOperation
    from sqlspec.data_dictionary import VersionInfo
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.typing import ArrowRecordBatch, ArrowReturnFormat, ArrowSchema, StatementParameters

__all__ = (
    "OracleAsyncDriver",
    "OracleAsyncExceptionHandler",
    "OracleAsyncSessionContext",
    "OracleSyncDriver",
    "OracleSyncExceptionHandler",
    "OracleSyncSessionContext",
)


logger = get_logger(__name__)


def _resolve_direct_path_target(connection: Any, table: str) -> tuple[str, str]:
    parts = split_qualified_identifier(table, quote_chars='"', allow_bracket_quotes=False)
    if not parts:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    if len(parts) == 1:
        return connection.username, parts[0]
    return ".".join(parts[:-1]), parts[-1]


# Oracle SQL-context byte thresholds (4000 / 2000) live in driver_features so users
# on MAX_STRING_SIZE=EXTENDED databases can override them; defaults are wired in
# core.apply_driver_features and read at the dispatch_execute call sites below.


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


class OracleSyncExceptionHandler(BaseSyncExceptionHandler):
    """Sync Context manager for handling Oracle database exceptions.

    Maps Oracle ORA-XXXXX error codes to specific SQLSpec exceptions
    for better error handling in application code.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __exit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        if exc_type is None:
            return False
        if issubclass(exc_type, OracleDatabaseError):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class OracleAsyncExceptionHandler(BaseAsyncExceptionHandler):
    """Async context manager for handling Oracle database exceptions.

    Maps Oracle ORA-XXXXX error codes to specific SQLSpec exceptions
    for better error handling in application code.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __aexit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        if exc_type is None:
            return False
        if issubclass(exc_type, OracleDatabaseError):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class OracleSyncDriver(OraclePipelineMixin, SyncDriverAdapterBase):
    """Synchronous Oracle Database driver.

    Provides Oracle Database connectivity with parameter style conversion,
    error handling, and transaction management.
    """

    __slots__ = (
        "_data_dictionary",
        "_oracle_version",
        "_pipeline_support",
        "_pipeline_support_reason",
        "_row_metadata_cache",
    )
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
        self._row_metadata_cache: dict[int, tuple[Any, list[str], bool]] = {}

    # ─────────────────────────────────────────────────────────────────────────────
    # CORE DISPATCH METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def dispatch_execute(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement with Oracle data handling.

        For SELECT-like statements, fetches all rows, resolves row metadata, and
        applies LOB coercion if needed. For non-SELECT statements, resolves and
        returns the affected row count.

        Args:
            cursor: Oracle cursor object
            statement: SQL statement to execute

        Returns:
            Execution result containing data for SELECT statements or row count for others
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        prepared_parameters = coerce_large_parameters_sync(
            self.connection,
            prepared_parameters,
            clob_type=DB_TYPE_CLOB,
            blob_type=DB_TYPE_BLOB,
            varchar2_byte_limit=self.driver_features.get("oracle_varchar2_byte_limit", 4000),
            raw_byte_limit=self.driver_features.get("oracle_raw_byte_limit", 2000),
        )
        prepared_parameters = cast("list[Any] | tuple[Any, ...] | dict[Any, Any] | None", prepared_parameters)

        cursor.execute(sql, prepared_parameters or {}, **build_fetch_kwargs(self.driver_features))

        is_select_like = statement.returns_rows() or self._should_force_select(statement, cursor)

        if is_select_like:
            fetched_data = cursor.fetchall()
            column_names, requires_lob_coercion = self._resolve_row_metadata(cursor.description)
            data, column_names = collect_sync_rows(
                cast("list[Any] | None", fetched_data),
                cursor.description,
                self.driver_features,
                column_names=column_names,
                requires_lob_coercion=requires_lob_coercion,
            )

            return self.create_execution_result(
                cursor,
                selected_data=data,
                column_names=column_names,
                data_row_count=len(data),
                is_select_result=True,
                row_format="tuple",
            )

        affected_rows = resolve_rowcount(cursor)
        return self.create_execution_result(cursor, rowcount_override=affected_rows)

    def dispatch_execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using Oracle batch processing.

        Args:
            cursor: Oracle cursor object
            statement: SQL statement with multiple parameter sets

        Returns:
            Execution result with affected row count
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        prepared_parameters = normalize_execute_many_parameters_sync(prepared_parameters)
        execution_args = statement.statement_config.execution_args or {}
        batch_errors = bool(execution_args.get("oracle_batch_errors", False))
        array_dml_row_counts = bool(execution_args.get("oracle_array_dml_row_counts", False))
        cursor.executemany(sql, prepared_parameters, batcherrors=batch_errors, arraydmlrowcounts=array_dml_row_counts)

        affected_rows = len(prepared_parameters)
        special_data: dict[str, Any] | None = None
        if batch_errors or array_dml_row_counts:
            special_data = {}
            if batch_errors:
                special_data["oracle_batch_errors"] = [
                    {
                        "offset": cast("int | None", getattr(error, "offset", None)),
                        "code": cast("int | None", getattr(error, "code", None)),
                        "message": cast("str", getattr(error, "message", str(error))),
                    }
                    for error in cursor.getbatcherrors()
                ]
            if array_dml_row_counts:
                row_counts = list(cursor.getarraydmlrowcounts())
                special_data["oracle_dml_row_counts"] = row_counts
                affected_rows = sum(row_counts)

        return self.create_execution_result(
            cursor, rowcount_override=affected_rows, special_data=special_data, is_many_result=True
        )

    def dispatch_execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
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
        except OracleError as e:
            msg = f"Failed to commit Oracle transaction: {e}"
            raise SQLSpecError(msg) from e

    def rollback(self) -> None:
        """Rollback the current transaction.

        Raises:
            SQLSpecError: If rollback fails
        """
        try:
            self.connection.rollback()
        except OracleError as e:
            msg = f"Failed to rollback Oracle transaction: {e}"
            raise SQLSpecError(msg) from e

    def set_migration_session_schema(self, schema: str) -> None:
        """Set Oracle CURRENT_SCHEMA for migration SQL."""
        normalized_schema = normalize_identifier(schema, "oracle")
        quoted_schema = quote_identifier(normalized_schema)
        with self.with_cursor(self.connection) as cursor:
            cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {quoted_schema}")

    def has_schema(self, schema: str) -> bool:
        """Return whether an Oracle schema/user exists."""
        normalized_schema = normalize_identifier(schema, "oracle")
        with self.with_cursor(self.connection) as cursor:
            cursor.execute("SELECT 1 FROM ALL_USERS WHERE USERNAME = :schema_name", {"schema_name": normalized_schema})
            return cursor.fetchone() is not None

    def with_cursor(self, connection: OracleSyncConnection) -> OracleSyncCursor:
        """Create context manager for Oracle cursor.

        Args:
            connection: Oracle database connection

        Returns:
            Context manager for cursor operations
        """
        return OracleSyncCursor(
            connection,
            arraysize=self.driver_features.get("arraysize"),
            prefetchrows=self.driver_features.get("prefetchrows"),
        )

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "SyncRowStream[dict[str, Any]] | None":
        """Return a native oracledb row stream backed by chunked ``fetchmany``."""
        if not statement.returns_rows():
            return None
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        stream_parameters = cast("list[object] | tuple[object, ...] | dict[object, object] | None", prepared_parameters)
        return SyncRowStream(OracleSyncStreamSource(self, sql, stream_parameters, chunk_size))

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
    ) -> "ArrowResult":
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
        """
        ensure_pyarrow()

        import pyarrow as pa

        config = statement_config or self.statement_config
        prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
        sql, prepared_parameters = self._get_compiled_sql(prepared_statement, config)

        try:
            if return_format in {"batches", "reader"} and supports_df_batches(self.connection):
                import pyarrow as pa

                record_batches, batch_schema = self._fetch_arrow_record_batches(
                    sql, prepared_parameters, batch_size, arrow_schema
                )
                rows_affected = sum(batch.num_rows for batch in record_batches)
                if batch_schema is None:
                    batch_schema = pa.schema([])
                arrow_data: object
                if return_format == "reader":
                    arrow_data = pa.RecordBatchReader.from_batches(batch_schema, record_batches)
                else:
                    arrow_data = record_batches
                return create_arrow_result(prepared_statement, arrow_data, rows_affected=rows_affected)

            oracle_df = self._execute_arrow_dataframe(sql, prepared_parameters, batch_size)
        except AttributeError as exc:
            if native_only:
                msg = "Oracle native Arrow support is not available for this connection."
                raise ImproperConfigurationError(msg) from exc
            return super().select_to_arrow(
                prepared_statement,
                statement_config=config,
                return_format=return_format,
                native_only=native_only,
                batch_size=batch_size,
                arrow_schema=arrow_schema,
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
            exc_handler = self.handle_database_exceptions()
            with self.with_cursor(self.connection) as cursor, exc_handler:
                cursor.execute(statement)
            if exc_handler.pending_exception is not None:
                raise exc_handler.pending_exception from None
        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            use_direct_path = self.driver_features.get(
                "enable_direct_path_load", True
            ) is not False and supports_direct_path_load(self.connection)
            if use_direct_path:
                schema_name, table_name = _resolve_direct_path_target(self.connection, table)
                exc_handler = self.handle_database_exceptions()
                with exc_handler:
                    self.connection.direct_path_load(
                        schema_name=schema_name, table_name=table_name, column_names=columns, data=records
                    )
                if exc_handler.pending_exception is not None:
                    raise exc_handler.pending_exception from None
            else:
                statement = build_insert_statement(table, columns)
                exc_handler = self.handle_database_exceptions()
                with self.with_cursor(self.connection) as cursor, exc_handler:
                    cursor.executemany(statement, records)
                if exc_handler.pending_exception is not None:
                    raise exc_handler.pending_exception from None
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

    def collect_rows(self, cursor: Any, fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        """Collect Oracle sync rows for the direct execution path."""
        column_names, requires_lob_coercion = self._resolve_row_metadata(cursor.description)
        data, column_names = collect_sync_rows(
            cast("list[Any] | None", fetched),
            cursor.description,
            self.driver_features,
            column_names=column_names,
            requires_lob_coercion=requires_lob_coercion,
        )
        return data, column_names, len(data)

    def resolve_rowcount(self, cursor: Any) -> int:
        """Resolve rowcount from Oracle cursor for the direct execution path."""
        return resolve_rowcount(cursor)

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

    def _resolve_row_metadata(self, description: Any) -> "tuple[list[str], bool]":
        return resolve_row_metadata(description, self.driver_features, self._row_metadata_cache)

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
        return self.connection.fetch_df_all(
            statement=sql,
            parameters=params,
            arraysize=batch_size or 1000,
            **build_arrow_fetch_kwargs(self.driver_features),
        )

    def _fetch_arrow_record_batches(
        self,
        sql: str,
        parameters: "StatementParameters | None",
        batch_size: int | None,
        arrow_schema: "ArrowSchema | None",
    ) -> "tuple[list[ArrowRecordBatch], ArrowSchema | None]":
        import pyarrow as pa

        params = parameters if parameters is not None else []
        record_batches: list[ArrowRecordBatch] = []
        batch_schema: ArrowSchema | None = None
        for oracle_df in self.connection.fetch_df_batches(
            statement=sql, parameters=params, size=batch_size or 1000, **build_arrow_fetch_kwargs(self.driver_features)
        ):
            batch_table = pa.table(oracle_df)
            column_names = normalize_column_names(batch_table.column_names, self.driver_features)
            if column_names != batch_table.column_names:
                batch_table = batch_table.rename_columns(column_names)
            if arrow_schema is not None:
                batch_table = batch_table.cast(arrow_schema)
            if batch_schema is None:
                batch_schema = batch_table.schema
            record_batches.extend(batch_table.to_batches())
        return record_batches, batch_schema

    def _execute_stack_native(self, stack: "StatementStack", *, continue_on_error: bool) -> "tuple[StackResult, ...]":
        compiled_operations = [self._prepare_pipeline_operation(op) for op in stack.operations]
        pipeline = create_oracle_pipeline()
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
                    except Exception as rollback_error:  # pragma: no cover
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

    __slots__ = (
        "_data_dictionary",
        "_oracle_version",
        "_pipeline_support",
        "_pipeline_support_reason",
        "_row_metadata_cache",
    )
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
        self._row_metadata_cache: dict[int, tuple[Any, list[str], bool]] = {}

    # ─────────────────────────────────────────────────────────────────────────────
    # CORE DISPATCH METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    async def dispatch_execute(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement with Oracle data handling.

        For SELECT-like statements, fetches all rows, resolves row metadata, and
        applies LOB coercion if needed. For non-SELECT statements, resolves and
        returns the affected row count.

        Args:
            cursor: Oracle cursor object
            statement: SQL statement to execute

        Returns:
            Execution result containing data for SELECT statements or row count for others
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        prepared_parameters = await coerce_large_parameters_async(
            self.connection,
            prepared_parameters,
            clob_type=DB_TYPE_CLOB,
            blob_type=DB_TYPE_BLOB,
            varchar2_byte_limit=self.driver_features.get("oracle_varchar2_byte_limit", 4000),
            raw_byte_limit=self.driver_features.get("oracle_raw_byte_limit", 2000),
        )
        prepared_parameters = cast("list[Any] | tuple[Any, ...] | dict[Any, Any] | None", prepared_parameters)

        await cursor.execute(sql, prepared_parameters or {}, **build_fetch_kwargs(self.driver_features))

        is_select_like = statement.returns_rows() or self._should_force_select(statement, cursor)

        if is_select_like:
            fetched_data = await cursor.fetchall()
            column_names, requires_lob_coercion = self._resolve_row_metadata(cursor.description)
            data, column_names = await collect_async_rows(
                cast("list[Any] | None", fetched_data),
                cursor.description,
                self.driver_features,
                column_names=column_names,
                requires_lob_coercion=requires_lob_coercion,
            )

            return self.create_execution_result(
                cursor,
                selected_data=data,
                column_names=column_names,
                data_row_count=len(data),
                is_select_result=True,
                row_format="tuple",
            )

        affected_rows = resolve_rowcount(cursor)
        return self.create_execution_result(cursor, rowcount_override=affected_rows)

    async def dispatch_execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using Oracle batch processing.

        Args:
            cursor: Oracle cursor object
            statement: SQL statement with multiple parameter sets

        Returns:
            Execution result with affected row count
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        prepared_parameters = normalize_execute_many_parameters_async(prepared_parameters)
        execution_args = statement.statement_config.execution_args or {}
        batch_errors = bool(execution_args.get("oracle_batch_errors", False))
        array_dml_row_counts = bool(execution_args.get("oracle_array_dml_row_counts", False))
        await cursor.executemany(
            sql, prepared_parameters, batcherrors=batch_errors, arraydmlrowcounts=array_dml_row_counts
        )

        affected_rows = len(prepared_parameters)
        special_data: dict[str, Any] | None = None
        if batch_errors or array_dml_row_counts:
            special_data = {}
            if batch_errors:
                special_data["oracle_batch_errors"] = [
                    {
                        "offset": cast("int | None", getattr(error, "offset", None)),
                        "code": cast("int | None", getattr(error, "code", None)),
                        "message": cast("str", getattr(error, "message", str(error))),
                    }
                    for error in cursor.getbatcherrors()
                ]
            if array_dml_row_counts:
                row_counts = list(cursor.getarraydmlrowcounts())
                special_data["oracle_dml_row_counts"] = row_counts
                affected_rows = sum(row_counts)

        return self.create_execution_result(
            cursor, rowcount_override=affected_rows, special_data=special_data, is_many_result=True
        )

    async def dispatch_execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
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
        except OracleError as e:
            msg = f"Failed to commit Oracle transaction: {e}"
            raise SQLSpecError(msg) from e

    async def rollback(self) -> None:
        """Rollback the current transaction.

        Raises:
            SQLSpecError: If rollback fails
        """
        try:
            await self.connection.rollback()
        except OracleError as e:
            msg = f"Failed to rollback Oracle transaction: {e}"
            raise SQLSpecError(msg) from e

    async def set_migration_session_schema(self, schema: str) -> None:
        """Set Oracle CURRENT_SCHEMA for migration SQL."""
        normalized_schema = normalize_identifier(schema, "oracle")
        quoted_schema = quote_identifier(normalized_schema)
        async with self.with_cursor(self.connection) as cursor:
            await cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {quoted_schema}")

    async def has_schema(self, schema: str) -> bool:
        """Return whether an Oracle schema/user exists."""
        normalized_schema = normalize_identifier(schema, "oracle")
        async with self.with_cursor(self.connection) as cursor:
            await cursor.execute(
                "SELECT 1 FROM ALL_USERS WHERE USERNAME = :schema_name", {"schema_name": normalized_schema}
            )
            row = await cursor.fetchone()
            return row is not None

    def with_cursor(self, connection: OracleAsyncConnection) -> OracleAsyncCursor:
        """Create context manager for Oracle cursor.

        Args:
            connection: Oracle database connection

        Returns:
            Context manager for cursor operations
        """
        return OracleAsyncCursor(
            connection,
            arraysize=self.driver_features.get("arraysize"),
            prefetchrows=self.driver_features.get("prefetchrows"),
        )

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "AsyncRowStream[dict[str, Any]] | None":
        """Return a native oracledb row stream backed by chunked ``fetchmany``."""
        if not statement.returns_rows():
            return None
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        stream_parameters = cast("list[object] | tuple[object, ...] | dict[object, object] | None", prepared_parameters)
        return AsyncRowStream(OracleAsyncStreamSource(self, sql, stream_parameters, chunk_size))

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
    ) -> "ArrowResult":
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
        """
        ensure_pyarrow()

        import pyarrow as pa

        config = statement_config or self.statement_config
        prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
        sql, prepared_parameters = self._get_compiled_sql(prepared_statement, config)

        try:
            if return_format in {"batches", "reader"} and supports_df_batches(self.connection):
                import pyarrow as pa

                record_batches, batch_schema = await self._fetch_arrow_record_batches(
                    sql, prepared_parameters, batch_size, arrow_schema
                )
                rows_affected = sum(batch.num_rows for batch in record_batches)
                if batch_schema is None:
                    batch_schema = pa.schema([])
                arrow_data: object
                if return_format == "reader":
                    arrow_data = pa.RecordBatchReader.from_batches(batch_schema, record_batches)
                else:
                    arrow_data = record_batches
                return create_arrow_result(prepared_statement, arrow_data, rows_affected=rows_affected)

            oracle_df = await self._execute_arrow_dataframe(sql, prepared_parameters, batch_size)
        except AttributeError as exc:
            if native_only:
                msg = "Oracle native Arrow support is not available for this connection."
                raise ImproperConfigurationError(msg) from exc
            return await super().select_to_arrow(
                prepared_statement,
                statement_config=config,
                return_format=return_format,
                native_only=native_only,
                batch_size=batch_size,
                arrow_schema=arrow_schema,
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
            exc_handler = self.handle_database_exceptions()
            async with exc_handler:
                await self.connection.execute(statement)
            if exc_handler.pending_exception is not None:
                raise exc_handler.pending_exception from None
        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            use_direct_path = self.driver_features.get(
                "enable_direct_path_load", True
            ) is not False and supports_direct_path_load(self.connection)
            if use_direct_path:
                schema_name, table_name = _resolve_direct_path_target(self.connection, table)
                exc_handler = self.handle_database_exceptions()
                async with exc_handler:
                    await self.connection.direct_path_load(
                        schema_name=schema_name, table_name=table_name, column_names=columns, data=records
                    )
                if exc_handler.pending_exception is not None:
                    raise exc_handler.pending_exception from None
            else:
                statement = build_insert_statement(table, columns)
                exc_handler = self.handle_database_exceptions()
                async with self.with_cursor(self.connection) as cursor, exc_handler:
                    await cursor.executemany(statement, records)
                if exc_handler.pending_exception is not None:
                    raise exc_handler.pending_exception from None
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

    def collect_rows(self, cursor: Any, fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        """Collect Oracle async rows for the direct execution path.

        Uses synchronous LOB coercion. For async LOB coercion, the standard
        dispatch path via collect_async_rows is used instead.
        """
        column_names, requires_lob_coercion = self._resolve_row_metadata(cursor.description)
        data, column_names = collect_sync_rows(
            cast("list[Any] | None", fetched),
            cursor.description,
            self.driver_features,
            column_names=column_names,
            requires_lob_coercion=requires_lob_coercion,
        )
        return data, column_names, len(data)

    def resolve_rowcount(self, cursor: Any) -> int:
        """Resolve rowcount from Oracle cursor for the direct execution path."""
        return resolve_rowcount(cursor)

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

    def _resolve_row_metadata(self, description: Any) -> "tuple[list[str], bool]":
        return resolve_row_metadata(description, self.driver_features, self._row_metadata_cache)

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
        return await self.connection.fetch_df_all(
            statement=sql,
            parameters=params,
            arraysize=batch_size or 1000,
            **build_arrow_fetch_kwargs(self.driver_features),
        )

    async def _fetch_arrow_record_batches(
        self,
        sql: str,
        parameters: "StatementParameters | None",
        batch_size: int | None,
        arrow_schema: "ArrowSchema | None",
    ) -> "tuple[list[ArrowRecordBatch], ArrowSchema | None]":
        import pyarrow as pa

        params = parameters if parameters is not None else []
        record_batches: list[ArrowRecordBatch] = []
        batch_schema: ArrowSchema | None = None
        async for oracle_df in self.connection.fetch_df_batches(
            statement=sql, parameters=params, size=batch_size or 1000, **build_arrow_fetch_kwargs(self.driver_features)
        ):
            batch_table = pa.table(oracle_df)
            column_names = normalize_column_names(batch_table.column_names, self.driver_features)
            if column_names != batch_table.column_names:
                batch_table = batch_table.rename_columns(column_names)
            if arrow_schema is not None:
                batch_table = batch_table.cast(arrow_schema)
            if batch_schema is None:
                batch_schema = batch_table.schema
            record_batches.extend(batch_table.to_batches())
        return record_batches, batch_schema

    async def _execute_stack_native(
        self, stack: "StatementStack", *, continue_on_error: bool
    ) -> "tuple[StackResult, ...]":
        compiled_operations = [self._prepare_pipeline_operation(op) for op in stack.operations]
        pipeline = create_oracle_pipeline()
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
                    except Exception as rollback_error:  # pragma: no cover
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
