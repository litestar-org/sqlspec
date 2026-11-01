"""Oracle Database driver implementation."""

import logging
from itertools import chain
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Any, Final, Literal, NamedTuple, Protocol, cast, overload

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
from sqlspec.adapters.oracledb._typing import oracledb_create_pipeline as create_oracle_pipeline
from sqlspec.adapters.oracledb.core import (
    ORACLEDB_VERSION,
    OracleAsyncStreamSource,
    OracleSyncStreamSource,
    apply_oracle_session_trace_attributes,
    build_arrow_fetch_kwargs,
    build_dbms_cloud_copy_data_sql,
    build_fetch_kwargs,
    build_insert_statement,
    build_pipeline_stack_result,
    build_truncate_statement,
    coerce_large_parameters_async,
    coerce_large_parameters_sync,
    coerce_many_parameters_async,
    coerce_many_parameters_sync,
    collect_async_rows,
    collect_sync_rows,
    connection_is_thin,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    normalize_column_names,
    resolve_dbms_cloud_source_uri,
    resolve_row_metadata,
    resolve_rowcount,
    supports_df_batches,
    supports_direct_path_load,
)
from sqlspec.adapters.oracledb.data_dictionary import (
    OracledbAsyncDataDictionary,
    OracledbSyncDataDictionary,
    OracleVersionCache,
    resolve_oracle_connection_major,
)
from sqlspec.core import (
    SQL,
    DMLResult,
    SQLResult,
    StackResult,
    StatementConfig,
    StatementStack,
    build_arrow_result_from_table,
    create_arrow_result,
    get_cache_config,
    register_driver_profile,
)
from sqlspec.core.explain import (
    ORACLE_EXPLAIN_DISPLAY_CURSOR_META_KEY,
    ORACLE_EXPLAIN_PREFIX,
    ORACLE_EXPLAIN_XPLAN_FORMAT_META_KEY,
    ORACLE_MANAGED_EXPLAIN_META_KEY,
)
from sqlspec.core.sqlcommenter import SQLCommenterContext, format_wire_sqlcomment, strip_sqlcomment
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
    validate_savepoint_name,
)
from sqlspec.exceptions import ImproperConfigurationError, SQLSpecError, StackExecutionError
from sqlspec.utils.arrow_helpers import coerce_oracle_df_to_arrow_table
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.module_loader import ensure_pyarrow, import_optional
from sqlspec.utils.text import normalize_identifier, quote_identifier, split_qualified_identifier
from sqlspec.utils.type_guards import has_pipeline_capability, is_async_readable
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, Statement, StatementFilter
    from sqlspec.core.cache import CachedQuery
    from sqlspec.core.stack import StackOperation
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.typing import ArrowRecordBatch, ArrowReturnFormat, ArrowSchema, SchemaT, StatementParameters

__all__ = (
    "OracleAsyncDriver",
    "OracleAsyncExceptionHandler",
    "OracleAsyncSessionContext",
    "OracleSyncDriver",
    "OracleSyncExceptionHandler",
    "OracleSyncSessionContext",
)


logger = get_logger(__name__)
pa: Any = import_optional("pyarrow")

PLAN_STATEMENT_ID_MAX_LENGTH: Final[int] = 30
"""Width of the PLAN_TABLE STATEMENT_ID column."""

PLAN_STATEMENT_ID_PREFIX: Final[str] = "sqlspec_"

PLAN_DISPLAY_SQL: Final[str] = "SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY(NULL, :statement_id, 'TYPICAL'))"

PLAN_CLEANUP_SQL: Final[str] = "DELETE FROM plan_table WHERE statement_id = :statement_id"


class OraclePipelineDriver(Protocol):
    """Protocol for Oracle pipeline driver methods used in stack execution."""

    statement_config: "StatementConfig"
    driver_features: "dict[str, Any]"

    def prepare_statement(
        self,
        statement: "str | Statement | QueryBuilder",
        parameters: "tuple[Any, ...] | dict[str, Any] | None",
        *,
        statement_config: "StatementConfig | None" = None,
        kwargs: "dict[str, Any] | None" = None,
    ) -> "SQL": ...

    def _compiled_sql(self, statement: "SQL", statement_config: "StatementConfig") -> "tuple[str, Any]": ...


PIPELINE_MIN_DRIVER_VERSION: "tuple[int, int, int]" = (2, 4, 0)
"""Lowest python-oracledb version that supports pipelining."""

PIPELINE_MIN_DATABASE_MAJOR: int = 23
"""Lowest Oracle Database major version pipelining is used on.

This is a policy threshold for when pipelining is worth the round trips, not a
correctness guard: python-oracledb self-gates through ``supports_pipelining()``
and falls back to sequential execution on databases that cannot pipeline.
"""


class OraclePipelineMixin:
    """Shared helpers for Oracle pipeline execution."""

    __slots__ = ()

    def _apply_session_tracing(self, connection: Any = None) -> None:
        if not getattr(self, "_session_tracing_enabled", False):
            return
        driver = cast("OraclePipelineDriver", self)
        target_conn = connection if connection is not None else getattr(self, "connection", None)
        if target_conn is None:
            return
        ctx = SQLCommenterContext.get() or {}
        attrs = driver.statement_config.sqlcommenter_attributes or {}
        default_module = (
            ctx.get("module")
            or ctx.get("framework")
            or ctx.get("controller")
            or attrs.get("module")
            or attrs.get("framework")
            or attrs.get("controller")
            or "sqlspec"
        )
        default_action = ctx.get("action") or ctx.get("route") or attrs.get("action") or attrs.get("route")
        apply_oracle_session_trace_attributes(target_conn, module=default_module, action=default_action)

    def _format_wire_sql(self, sql: str, config: "StatementConfig | None" = None) -> str:
        driver = cast("OraclePipelineDriver", self)
        effective_config = config or driver.statement_config
        if getattr(self, "_session_tracing_enabled", False):
            return strip_sqlcomment(sql)
        if not effective_config.enable_sqlcommenter:
            return sql
        return format_wire_sqlcomment(sql, effective_config)

    def _compiled_sql(
        self, statement: "SQL", statement_config: "StatementConfig", flatten_single_parameters: bool = False
    ) -> "tuple[str, Any]":
        compiled_sql, prepared_parameters = super()._compiled_sql(  # type: ignore[misc]
            statement, statement_config, flatten_single_parameters=flatten_single_parameters
        )
        if getattr(self, "_session_tracing_enabled", False):
            return strip_sqlcomment(compiled_sql), prepared_parameters
        return compiled_sql, prepared_parameters

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

    def _prepare_pipeline_operation(self, operation: "StackOperation") -> "_CompiledStackOperation":
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

        compiled_sql, prepared_parameters = driver._compiled_sql(  # pyright: ignore[reportPrivateUsage]
            sql_statement, config
        )
        summary = describe_stack_statement(operation.statement)
        return _CompiledStackOperation(
            statement=sql_statement,
            sql=compiled_sql,
            parameters=prepared_parameters,
            method=operation.method,
            returns_rows=sql_statement.returns_rows()
            and sql_statement.operation_type not in {"INSERT", "UPDATE", "DELETE"},
            summary=summary,
        )

    def _add_pipeline_operation(self, pipeline: Any, operation: "_CompiledStackOperation") -> None:
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
                mapped_error = create_mapped_exception(error) if not isinstance(error, SQLSpecError) else error
                stack_error = StackExecutionError(
                    index,
                    compiled.summary,
                    mapped_error,
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
        mapped_error = create_mapped_exception(error) if not isinstance(error, SQLSpecError) else error
        return StackExecutionError(
            -1, "Oracle pipeline execution failed", mapped_error, adapter=type(self).__name__, mode=mode
        )


class OracleSyncExceptionHandler(BaseSyncExceptionHandler):
    """Synchronous context manager for handling Oracle database exceptions.

    Maps Oracle ``ORA-XXXXX`` error codes to SQLSpec exceptions stored on
    ``pending_exception``.
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

    Maps Oracle ``ORA-XXXXX`` error codes to SQLSpec exceptions stored on
    ``pending_exception``.
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
        "_fetch_kwargs",
        "_oracle_version_cache",
        "_pipeline_support",
        "_pipeline_support_reason",
        "_row_metadata_cache",
        "_session_tracing_enabled",
        "_transaction_active",
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
        self._oracle_version_cache: OracleVersionCache | None = None
        self._row_metadata_cache: dict[int, tuple[Any, list[str], bool]] = {}
        self._transaction_active = False
        self._fetch_kwargs = build_fetch_kwargs(self.driver_features)
        self._session_tracing_enabled = bool(
            self.driver_features.get("enable_session_tracing", False) or self.statement_config.enable_sqlcommenter
        )

    @property
    def data_dictionary(self) -> "OracledbSyncDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = OracledbSyncDataDictionary()
        return self._data_dictionary

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

        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)

        prepared_parameters = coerce_large_parameters_sync(
            self.connection,
            prepared_parameters,
            clob_type=DB_TYPE_CLOB,
            blob_type=DB_TYPE_BLOB,
            varchar2_byte_limit=self.driver_features.get("oracle_varchar2_byte_limit", 4000),
            raw_byte_limit=self.driver_features.get("oracle_raw_byte_limit", 2000),
            version_cache=self._oracle_version_cache,
        )
        prepared_parameters = cast("list[Any] | tuple[Any, ...] | dict[Any, Any] | None", prepared_parameters)

        explained_sql = _managed_explain_statement(statement, sql)
        if explained_sql is not None:
            display_cursor, xplan_format = _managed_explain_options(statement)
            return self._dispatch_explain_plan(
                cursor, explained_sql, prepared_parameters, display_cursor=display_cursor, xplan_format=xplan_format
            )

        cursor.execute(sql, prepared_parameters or {}, **self._fetch_kwargs)

        is_select_like = statement.returns_rows() or self._should_force_select(statement, cursor)

        if is_select_like and cursor.description is not None:
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

    def _execute_cache_hit_with_cursor(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: "CachedQuery"
    ) -> "SQLResult":
        """Execute a cached query while owning only the cursor context."""
        with self.with_cursor(self.connection) as cursor:
            wire_sql = self._format_wire_sql(cached.compiled_sql)
            returns_rows = cached.operation_profile.returns_rows
            fetch_kwargs = self._fetch_kwargs
            parameters = coerce_large_parameters_sync(
                self.connection,
                params,
                clob_type=DB_TYPE_CLOB,
                blob_type=DB_TYPE_BLOB,
                varchar2_byte_limit=self.driver_features.get("oracle_varchar2_byte_limit", 4000),
                raw_byte_limit=self.driver_features.get("oracle_raw_byte_limit", 2000),
                version_cache=self._oracle_version_cache,
            )
            cast("Any", cursor).execute(wire_sql, parameters or {}, **fetch_kwargs)
            if returns_rows and cursor.description is not None:
                fetched_data = cursor.fetchall()
                column_names, requires_lob_coercion = self._resolve_row_metadata(cursor.description)
                data, column_names = collect_sync_rows(
                    cast("list[Any] | None", fetched_data),
                    cursor.description,
                    self.driver_features,
                    column_names=column_names,
                    requires_lob_coercion=requires_lob_coercion,
                )
                return self.build_cached_select_result(
                    sql,
                    params,
                    cached,
                    selected_data=data,
                    column_names=column_names,
                    data_row_count=len(data),
                    row_format="tuple",
                    compiled_sql=wire_sql,
                )
            affected_rows = resolve_rowcount(cursor)
            return cast("SQLResult", DMLResult(cached.operation_type, affected_rows))

    def dispatch_execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using Oracle batch processing.

        Args:
            cursor: Oracle cursor object
            statement: SQL statement with multiple parameter sets

        Returns:
            Execution result with affected row count
        """
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)

        prepared_parameters = coerce_many_parameters_sync(
            self.connection,
            prepared_parameters,
            clob_type=DB_TYPE_CLOB,
            blob_type=DB_TYPE_BLOB,
            varchar2_byte_limit=self.driver_features.get("oracle_varchar2_byte_limit", 4000),
            raw_byte_limit=self.driver_features.get("oracle_raw_byte_limit", 2000),
            version_cache=self._oracle_version_cache,
        )
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
        """Execute SQL script with statement splitting and parameter handling."""
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        prepared_parameters = coerce_large_parameters_sync(
            self.connection,
            prepared_parameters,
            clob_type=DB_TYPE_CLOB,
            blob_type=DB_TYPE_BLOB,
            varchar2_byte_limit=self.driver_features.get("oracle_varchar2_byte_limit", 4000),
            raw_byte_limit=self.driver_features.get("oracle_raw_byte_limit", 2000),
            version_cache=self._oracle_version_cache,
        )
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_cursor = cursor
        pos_offset = 0
        total_statements = len(statements)
        runtime = self._observability
        has_observers = runtime is not None and runtime.has_statement_observers

        for idx, stmt in enumerate(statements):
            stmt_params, pos_offset = _slice_script_parameters(stmt, prepared_parameters, pos_offset)
            stmt_started = perf_counter() if has_observers else 0.0
            cursor.execute(stmt, stmt_params)
            successful_count += 1
            if has_observers:
                stmt_duration = perf_counter() - stmt_started
                single_stmt = self._sub_statement(stmt, stmt_params)
                try:
                    stmt_operation = single_stmt.operation_type
                finally:
                    self._release_pooled_statement(single_stmt)
                self._emit_script_statement_event(
                    sql=stmt,
                    parameters=stmt_params,
                    operation=stmt_operation,
                    rows_affected=resolve_rowcount(cursor),
                    duration_s=stmt_duration,
                    started_at=stmt_started,
                    profile=None,
                    script_ordinal=idx,
                    script_statement_count=total_statements,
                )

        self._pending_statement_profile = None
        return self.create_execution_result(
            last_cursor, statement_count=total_statements, successful_statements=successful_count, is_script_result=True
        )

    def begin(self) -> None:
        """Begin a database transaction.

        Oracle starts a transaction implicitly on the first DML, so no explicit
        statement is issued; the active-transaction flag is set here.
        """
        self._transaction_active = True

    def commit(self) -> None:
        """Commit the current transaction.

        Raises:
            SQLSpecError: If commit fails
        """

        try:
            self.connection.commit()
        except OracleError as e:
            raise create_mapped_exception(e) from e
        finally:
            self._transaction_active = False

    def rollback(self) -> None:
        """Rollback the current transaction.

        Raises:
            SQLSpecError: If rollback fails
        """

        try:
            self.connection.rollback()
        except OracleError as e:
            raise create_mapped_exception(e) from e
        finally:
            self._transaction_active = False

    def release_savepoint(self, name: str) -> None:
        """Validate the savepoint name; Oracle releases savepoints when the transaction ends."""
        validate_savepoint_name(name)

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
            on_enter=self._apply_session_tracing if self._session_tracing_enabled else None,
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
        """Execute a query and stream rows in chunks with Oracle fetch tuning."""
        fetch_lobs = cast("bool | None", kwargs.pop("fetch_lobs", None))
        if chunk_size < 1:
            msg = "chunk_size must be greater than or equal to 1"
            raise ValueError(msg)
        config = statement_config or self.statement_config
        sql_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
        stream = self.dispatch_select_stream(sql_statement, chunk_size, fetch_lobs=fetch_lobs)
        if stream is not None:
            return stream._with_schema_type(schema_type)
        return super().select_stream(
            sql_statement,
            schema_type=schema_type,
            statement_config=config,
            chunk_size=chunk_size,
            native_only=native_only,
        )

    def dispatch_select_stream(
        self, statement: "SQL", chunk_size: int, fetch_lobs: bool | None = None
    ) -> "SyncRowStream[dict[str, Any]] | None":
        """Return a native oracledb row stream backed by chunked ``fetchmany``."""
        if not statement.returns_rows():
            return None
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        stream_parameters = cast("list[object] | tuple[object, ...] | dict[object, object] | None", prepared_parameters)
        return SyncRowStream(OracleSyncStreamSource(self, sql, stream_parameters, chunk_size, fetch_lobs=fetch_lobs))

    def handle_database_exceptions(self) -> "OracleSyncExceptionHandler":
        """Handle database-specific exceptions and wrap them appropriately."""
        return OracleSyncExceptionHandler()

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

        config = statement_config or self.statement_config
        prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
        sql, prepared_parameters = self._compiled_sql(prepared_statement, config)

        prepared_parameters = coerce_large_parameters_sync(
            self.connection,
            prepared_parameters,
            clob_type=DB_TYPE_CLOB,
            blob_type=DB_TYPE_BLOB,
            varchar2_byte_limit=self.driver_features.get("oracle_varchar2_byte_limit", 4000),
            raw_byte_limit=self.driver_features.get("oracle_raw_byte_limit", 2000),
            version_cache=self._oracle_version_cache,
        )
        prepared_parameters = cast("list[Any] | tuple[Any, ...] | dict[Any, Any] | None", prepared_parameters)

        exc_handler = self.handle_database_exceptions()
        try:
            self._apply_session_tracing(self.connection)
            if return_format == "reader" and supports_df_batches(self.connection):
                batch_generator = self._iter_arrow_record_batches(sql, prepared_parameters, batch_size, arrow_schema)
                try:
                    first_batch = next(batch_generator)
                except StopIteration:
                    empty_schema = arrow_schema if arrow_schema is not None else pa.schema([])
                    reader = pa.RecordBatchReader.from_batches(empty_schema, ())
                    return create_arrow_result(prepared_statement, reader, rows_affected=0)
                schema = arrow_schema if arrow_schema is not None else first_batch.schema
                lazy_reader = pa.RecordBatchReader.from_batches(schema, chain((first_batch,), batch_generator))
                return create_arrow_result(prepared_statement, lazy_reader, rows_affected=-1)

            if return_format == "batches" and supports_df_batches(self.connection):
                batches_result = cast(
                    "tuple[list[ArrowRecordBatch], ArrowSchema | None]",
                    self._run_with_exception_handler(
                        exc_handler,
                        self._fetch_arrow_record_batches,
                        sql,
                        prepared_parameters,
                        batch_size,
                        arrow_schema,
                    ),
                )
                record_batches, _ = batches_result
                rows_affected = sum(batch.num_rows for batch in record_batches)
                return create_arrow_result(prepared_statement, record_batches, rows_affected=rows_affected)

            oracle_df = self._run_with_exception_handler(
                exc_handler, self._execute_arrow_dataframe, sql, prepared_parameters, batch_size
            )
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
        self._check_pending_exception(exc_handler)

        arrow_table = coerce_oracle_df_to_arrow_table(oracle_df, pa)
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
        telemetry_payload = self._write_storage_result(
            arrow_result, destination, format_hint=format_hint, pipeline=sync_pipeline
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
        stage_table: "bool | str | None" = False,
        conflict_keys: "Sequence[str] | None" = None,
        on_conflict: Literal["update", "nothing"] = "update",
        exclude_update_columns: "Sequence[str] | None" = None,
        drop_stage_table: bool = True,
        drop_indexes: bool = False,
        concurrent_indexes: bool = True,
        type_casts: "Mapping[str, str] | None" = None,
    ) -> "StorageBridgeJob":
        """Load Arrow data into Oracle using batched executemany calls."""
        self._require_capability("arrow_import_enabled")
        if drop_indexes:
            with self.without_indexes(table, concurrent=concurrent_indexes):
                return self.load_from_arrow(
                    table,
                    source,
                    partitioner=partitioner,
                    overwrite=overwrite,
                    telemetry=telemetry,
                    stage_table=stage_table,
                    conflict_keys=conflict_keys,
                    on_conflict=on_conflict,
                    exclude_update_columns=exclude_update_columns,
                    drop_stage_table=drop_stage_table,
                    drop_indexes=False,
                    concurrent_indexes=concurrent_indexes,
                    type_casts=type_casts,
                )
        if stage_table or conflict_keys:
            return self._execute_staged_load_sync(
                table,
                source,
                raw_loader=self.load_from_arrow,
                partitioner=partitioner,
                overwrite=overwrite,
                telemetry=telemetry,
                stage_table=stage_table,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                drop_stage_table=drop_stage_table,
                drop_indexes=False,
                concurrent_indexes=concurrent_indexes,
                type_casts=type_casts,
            )
        arrow_table = self._coerce_arrow_table(source)
        if overwrite:
            statement = build_truncate_statement(table)
            exc_handler = self.handle_database_exceptions()

            def _truncate() -> None:
                cursor = self.connection.cursor()
                try:
                    cursor.execute(statement)
                finally:
                    cursor.close()

            self._run_with_exception_handler(exc_handler, _truncate)
            self._check_pending_exception(exc_handler)
        use_direct_path = (
            self.driver_features.get("enable_direct_path_load", True) is not False
            and supports_direct_path_load(self.connection)
            and _arrow_schema_supports_direct_path(arrow_table)
        )
        if arrow_table.num_rows:
            if use_direct_path:
                schema_name, table_name = _resolve_direct_path_target(self.connection, table)
                exc_handler = self.handle_database_exceptions()
                self._run_with_exception_handler(
                    exc_handler,
                    self.connection.direct_path_load,
                    schema_name=schema_name,
                    table_name=table_name,
                    column_names=list(arrow_table.column_names),
                    data=arrow_table,
                )
                self._check_pending_exception(exc_handler)
            else:
                columns = list(arrow_table.column_names)
                statement = build_insert_statement(table, columns)
                exc_handler = self.handle_database_exceptions()

                def _insert_batches() -> None:
                    with self.with_cursor(self.connection) as cursor:
                        for batch in arrow_table.to_batches(max_chunksize=5000):
                            batch_rows = self._arrow_batch_to_rows(batch, columns)
                            coerced_rows = coerce_many_parameters_sync(
                                self.connection,
                                batch_rows,
                                clob_type=DB_TYPE_CLOB,
                                blob_type=DB_TYPE_BLOB,
                                varchar2_byte_limit=self.driver_features.get("oracle_varchar2_byte_limit", 4000),
                                raw_byte_limit=self.driver_features.get("oracle_raw_byte_limit", 2000),
                                version_cache=self._oracle_version_cache,
                            )
                            cursor.executemany(statement, coerced_rows)

                self._run_with_exception_handler(exc_handler, _insert_batches)
                self._check_pending_exception(exc_handler)
        telemetry_payload = self._ingest_telemetry(arrow_table)
        telemetry_payload["destination"] = table
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        credential_name: str | None = None,
        format_options: "Mapping[str, Any] | None" = None,
        **kwargs: Any,
    ) -> "StorageBridgeJob":
        """Load staged artifacts into Oracle via ``DBMS_CLOUD.COPY_DATA`` or client-side Arrow."""
        raw_credential = (
            credential_name if credential_name is not None else self.driver_features.get("dbms_cloud_credential")
        )
        effective_credential = (
            raw_credential.strip() if isinstance(raw_credential, str) and raw_credential.strip() else None
        )
        if effective_credential is not None and not kwargs:
            resolved_source: StorageDestination = (
                self._storage_pipeline().resolve_destination(source).uri
                if isinstance(source, str) and source.startswith("alias://")
                else source
            )
            remote_uri = (
                resolve_dbms_cloud_source_uri(resolved_source, file_format=file_format)
                if isinstance(resolved_source, (str, Path))
                else None
            )
            if remote_uri is not None:
                plsql, binds = build_dbms_cloud_copy_data_sql(
                    table,
                    credential_name=effective_credential,
                    file_uri=remote_uri,
                    file_format=file_format,
                    format_options=format_options,
                )
                truncate_sql = build_truncate_statement(table) if overwrite else None
                rows_processed = 0
                exc_handler = self.handle_database_exceptions()

                def _run_dbms_cloud() -> None:
                    nonlocal rows_processed
                    with self.with_cursor(self.connection) as cursor:
                        if truncate_sql is not None:
                            cursor.execute(truncate_sql)
                        cursor.execute(plsql, binds)
                        rows_processed = resolve_rowcount(cursor)

                self._run_with_exception_handler(exc_handler, _run_dbms_cloud)
                self._check_pending_exception(exc_handler)
                telemetry_payload: StorageTelemetry = {
                    "destination": table,
                    "backend": "dbms_cloud",
                    "format": file_format,
                    "rows_processed": rows_processed,
                    "extra": {"source_uri": remote_uri, "credential_name": effective_credential},
                }
                self._attach_partition_telemetry(telemetry_payload, partitioner)
                return self._storage_job(telemetry_payload)
        if kwargs:
            return super().load_from_storage(
                table, source, file_format=file_format, partitioner=partitioner, overwrite=overwrite, **kwargs
            )
        arrow_table, inbound = self._read_storage_arrow(source, file_format=file_format)
        return self.load_from_arrow(table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound)

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
        """Report whether a transaction is open on the connection.

        Oracle opens a transaction implicitly on the first DML, which the
        ownership flag alone cannot see, while ``begin()`` sets the flag without
        issuing SQL, which the native predicate alone cannot see. Both signals
        are required.
        """
        return self._transaction_active or bool(self.connection.transaction_in_progress)

    def _detect_oracledb_version(self) -> "tuple[int, int, int]":
        return ORACLEDB_VERSION

    def _resolve_row_metadata(self, description: Any) -> "tuple[list[str], bool]":
        return resolve_row_metadata(description, self.driver_features, self._row_metadata_cache)

    def _dispatch_explain_plan(
        self,
        cursor: Any,
        explained_sql: str,
        parameters: Any,
        *,
        display_cursor: bool = False,
        xplan_format: str = "TYPICAL",
    ) -> "ExecutionResult":
        """Request a plan, read it back, and release the plan table rows.

        Oracle writes the plan to a table instead of returning it, so the plan is
        tagged with a generated statement id, formatted through ``DBMS_XPLAN``, and
        deleted again. All three statements run in the caller's transaction, so the
        plan is read before a rollback can discard it. When ``display_cursor`` is
        enabled, the statement is executed first and formatted through
        ``DBMS_XPLAN.DISPLAY_CURSOR``.

        Args:
            cursor: Oracle cursor object.
            explained_sql: Statement whose plan is wanted.
            parameters: Bind values for the explained statement.
            display_cursor: Whether to inspect actual cursor execution statistics via ``DBMS_XPLAN.DISPLAY_CURSOR``.
            xplan_format: ``DBMS_XPLAN`` format string.

        Returns:
            Execution result carrying the formatted plan rows.
        """
        if display_cursor:
            cursor.execute(explained_sql, parameters or {}, **self._fetch_kwargs)
            if cursor.description is not None:
                cursor.fetchall()
            cursor.execute(_format_display_cursor_sql(xplan_format), {}, **self._fetch_kwargs)
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

        statement_id = _new_plan_statement_id()
        cursor.execute(_tagged_explain_plan_sql(statement_id, explained_sql), parameters or {})
        display_sql = PLAN_DISPLAY_SQL if xplan_format == "TYPICAL" else _format_plan_display_sql(xplan_format)

        try:
            cursor.execute(display_sql, {"statement_id": statement_id}, **self._fetch_kwargs)
            fetched_data = cursor.fetchall()
            column_names, requires_lob_coercion = self._resolve_row_metadata(cursor.description)
            data, column_names = collect_sync_rows(
                cast("list[Any] | None", fetched_data),
                cursor.description,
                self.driver_features,
                column_names=column_names,
                requires_lob_coercion=requires_lob_coercion,
            )
            result = self.create_execution_result(
                cursor,
                selected_data=data,
                column_names=column_names,
                data_row_count=len(data),
                is_select_result=True,
                row_format="tuple",
            )
        except Exception:
            try:
                cursor.execute(PLAN_CLEANUP_SQL, {"statement_id": statement_id})
            except Exception:
                logger.warning("Failed to clean up Oracle plan rows after plan retrieval failed", exc_info=True)
            raise
        else:
            cursor.execute(PLAN_CLEANUP_SQL, {"statement_id": statement_id})
        return result

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
        fetch_kwargs = build_arrow_fetch_kwargs(self.driver_features)
        try:
            return self.connection.fetch_df_all(
                statement=sql, parameters=params, arraysize=batch_size or 1000, **fetch_kwargs
            )
        except TypeError as exc:
            retry_kwargs = _retry_arrow_without_fetch_lobs(exc, fetch_kwargs)
            if retry_kwargs is None:
                raise
            return self.connection.fetch_df_all(
                statement=sql, parameters=params, arraysize=batch_size or 1000, **retry_kwargs
            )

    def _iter_arrow_record_batches(
        self,
        sql: str,
        parameters: "StatementParameters | None",
        batch_size: int | None,
        arrow_schema: "ArrowSchema | None",
    ) -> "Iterator[ArrowRecordBatch]":
        params = parameters if parameters is not None else []
        fetch_kwargs = build_arrow_fetch_kwargs(self.driver_features, arrow_schema=arrow_schema)
        try:
            oracle_dfs = self.connection.fetch_df_batches(
                statement=sql, parameters=params, size=batch_size or 1000, **fetch_kwargs
            )
        except TypeError as exc:
            retry_kwargs = _retry_arrow_without_fetch_lobs(exc, fetch_kwargs)
            if retry_kwargs is None:
                raise
            oracle_dfs = self.connection.fetch_df_batches(
                statement=sql, parameters=params, size=batch_size or 1000, **retry_kwargs
            )
        for oracle_df in oracle_dfs:
            batch_table = coerce_oracle_df_to_arrow_table(oracle_df, pa)
            column_names = normalize_column_names(batch_table.column_names, self.driver_features)
            if column_names != batch_table.column_names:
                batch_table = batch_table.rename_columns(column_names)
            if arrow_schema is not None:
                batch_table = batch_table.cast(arrow_schema)
            yield from batch_table.to_batches()

    def _fetch_arrow_record_batches(
        self,
        sql: str,
        parameters: "StatementParameters | None",
        batch_size: int | None,
        arrow_schema: "ArrowSchema | None",
    ) -> "tuple[list[ArrowRecordBatch], ArrowSchema | None]":
        params = parameters if parameters is not None else []
        record_batches: list[ArrowRecordBatch] = []
        batch_schema: ArrowSchema | None = None
        fetch_kwargs = build_arrow_fetch_kwargs(self.driver_features, arrow_schema=arrow_schema)
        try:
            oracle_dfs = self.connection.fetch_df_batches(
                statement=sql, parameters=params, size=batch_size or 1000, **fetch_kwargs
            )
        except TypeError as exc:
            retry_kwargs = _retry_arrow_without_fetch_lobs(exc, fetch_kwargs)
            if retry_kwargs is None:
                raise
            oracle_dfs = self.connection.fetch_df_batches(
                statement=sql, parameters=params, size=batch_size or 1000, **retry_kwargs
            )
        for oracle_df in oracle_dfs:
            batch_table = coerce_oracle_df_to_arrow_table(oracle_df, pa)
            column_names = normalize_column_names(batch_table.column_names, self.driver_features)
            if column_names != batch_table.column_names:
                batch_table = batch_table.rename_columns(column_names)
            if arrow_schema is not None:
                batch_table = batch_table.cast(arrow_schema)
            if batch_schema is None:
                batch_schema = batch_table.schema
            record_batches.extend(batch_table.to_batches())
        return record_batches, batch_schema

    def _pipeline_native_supported(self) -> bool:
        if self._pipeline_support is not None:
            return self._pipeline_support

        if self.stack_native_disabled:
            self._pipeline_support = False
            self._pipeline_support_reason = "env_override"
            return False

        self._pipeline_support = False
        self._pipeline_support_reason = "asyncio_thin_required"
        return False


class OracleAsyncDriver(OraclePipelineMixin, AsyncDriverAdapterBase):
    """Asynchronous Oracle Database driver.

    Provides Oracle Database connectivity with parameter style conversion,
    error handling, and transaction management for async operations.
    """

    __slots__ = (
        "_data_dictionary",
        "_fetch_kwargs",
        "_oracle_version_cache",
        "_pipeline_support",
        "_pipeline_support_reason",
        "_row_metadata_cache",
        "_session_tracing_enabled",
        "_transaction_active",
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
        self._oracle_version_cache: OracleVersionCache | None = None
        self._row_metadata_cache: dict[int, tuple[Any, list[str], bool]] = {}
        self._transaction_active = False
        self._fetch_kwargs = build_fetch_kwargs(self.driver_features)
        self._session_tracing_enabled = bool(
            self.driver_features.get("enable_session_tracing", False) or self.statement_config.enable_sqlcommenter
        )

    @property
    def data_dictionary(self) -> "OracledbAsyncDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = OracledbAsyncDataDictionary()
        return self._data_dictionary

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

        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)

        prepared_parameters = await coerce_large_parameters_async(
            self.connection,
            prepared_parameters,
            clob_type=DB_TYPE_CLOB,
            blob_type=DB_TYPE_BLOB,
            varchar2_byte_limit=self.driver_features.get("oracle_varchar2_byte_limit", 4000),
            raw_byte_limit=self.driver_features.get("oracle_raw_byte_limit", 2000),
            version_cache=self._oracle_version_cache,
        )
        prepared_parameters = cast("list[Any] | tuple[Any, ...] | dict[Any, Any] | None", prepared_parameters)

        explained_sql = _managed_explain_statement(statement, sql)
        if explained_sql is not None:
            display_cursor, xplan_format = _managed_explain_options(statement)
            return await self._dispatch_explain_plan(
                cursor, explained_sql, prepared_parameters, display_cursor=display_cursor, xplan_format=xplan_format
            )

        await cursor.execute(sql, prepared_parameters or {}, **self._fetch_kwargs)

        is_select_like = statement.returns_rows() or self._should_force_select(statement, cursor)

        if is_select_like and cursor.description is not None:
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

    async def _execute_cache_hit_with_cursor(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: "CachedQuery"
    ) -> "SQLResult":
        """Execute an async cached query while owning only the cursor context."""
        async with self.with_cursor(self.connection) as cursor:
            wire_sql = self._format_wire_sql(cached.compiled_sql)
            returns_rows = cached.operation_profile.returns_rows
            fetch_kwargs = self._fetch_kwargs
            parameters = await coerce_large_parameters_async(
                self.connection,
                params,
                clob_type=DB_TYPE_CLOB,
                blob_type=DB_TYPE_BLOB,
                varchar2_byte_limit=self.driver_features.get("oracle_varchar2_byte_limit", 4000),
                raw_byte_limit=self.driver_features.get("oracle_raw_byte_limit", 2000),
                version_cache=self._oracle_version_cache,
            )
            await cast("Any", cursor).execute(wire_sql, parameters or {}, **fetch_kwargs)
            if returns_rows and cursor.description is not None:
                fetched_data = await cursor.fetchall()
                column_names, requires_lob_coercion = self._resolve_row_metadata(cursor.description)
                data, column_names = await collect_async_rows(
                    cast("list[Any] | None", fetched_data),
                    cursor.description,
                    self.driver_features,
                    column_names=column_names,
                    requires_lob_coercion=requires_lob_coercion,
                )
                return self.build_cached_select_result(
                    sql,
                    params,
                    cached,
                    selected_data=data,
                    column_names=column_names,
                    data_row_count=len(data),
                    row_format="tuple",
                    compiled_sql=wire_sql,
                )
            affected_rows = resolve_rowcount(cursor)
            return cast("SQLResult", DMLResult(cached.operation_type, affected_rows))

    async def dispatch_execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using Oracle batch processing.

        Args:
            cursor: Oracle cursor object
            statement: SQL statement with multiple parameter sets

        Returns:
            Execution result with affected row count
        """
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)

        prepared_parameters = await coerce_many_parameters_async(
            self.connection,
            prepared_parameters,
            clob_type=DB_TYPE_CLOB,
            blob_type=DB_TYPE_BLOB,
            varchar2_byte_limit=self.driver_features.get("oracle_varchar2_byte_limit", 4000),
            raw_byte_limit=self.driver_features.get("oracle_raw_byte_limit", 2000),
            version_cache=self._oracle_version_cache,
        )
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
        """Execute async SQL script with statement splitting and parameter handling."""
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        prepared_parameters = await coerce_large_parameters_async(
            self.connection,
            prepared_parameters,
            clob_type=DB_TYPE_CLOB,
            blob_type=DB_TYPE_BLOB,
            varchar2_byte_limit=self.driver_features.get("oracle_varchar2_byte_limit", 4000),
            raw_byte_limit=self.driver_features.get("oracle_raw_byte_limit", 2000),
            version_cache=self._oracle_version_cache,
        )
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_cursor = cursor
        pos_offset = 0
        total_statements = len(statements)
        runtime = self._observability
        has_observers = runtime is not None and runtime.has_statement_observers

        for idx, stmt in enumerate(statements):
            stmt_params, pos_offset = _slice_script_parameters(stmt, prepared_parameters, pos_offset)
            stmt_started = perf_counter() if has_observers else 0.0
            await cursor.execute(stmt, stmt_params)
            successful_count += 1
            if has_observers:
                stmt_duration = perf_counter() - stmt_started
                single_stmt = self._sub_statement(stmt, stmt_params)
                try:
                    stmt_operation = single_stmt.operation_type
                finally:
                    self._release_pooled_statement(single_stmt)
                self._emit_script_statement_event(
                    sql=stmt,
                    parameters=stmt_params,
                    operation=stmt_operation,
                    rows_affected=resolve_rowcount(cursor),
                    duration_s=stmt_duration,
                    started_at=stmt_started,
                    profile=None,
                    script_ordinal=idx,
                    script_statement_count=total_statements,
                )

        self._pending_statement_profile = None
        return self.create_execution_result(
            last_cursor, statement_count=total_statements, successful_statements=successful_count, is_script_result=True
        )

    async def begin(self) -> None:
        """Begin a database transaction.

        Oracle starts a transaction implicitly on the first DML, so no explicit
        statement is issued; the active-transaction flag is set here.
        """
        self._transaction_active = True

    async def commit(self) -> None:
        """Commit the current transaction.

        Raises:
            SQLSpecError: If commit fails
        """

        try:
            await self.connection.commit()
        except OracleError as e:
            raise create_mapped_exception(e) from e
        finally:
            self._transaction_active = False

    async def rollback(self) -> None:
        """Rollback the current transaction.

        Raises:
            SQLSpecError: If rollback fails
        """

        try:
            await self.connection.rollback()
        except OracleError as e:
            raise create_mapped_exception(e) from e
        finally:
            self._transaction_active = False

    async def release_savepoint(self, name: str) -> None:
        """Validate the savepoint name; Oracle releases savepoints when the transaction ends."""
        validate_savepoint_name(name)

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
            on_enter=self._apply_session_tracing if self._session_tracing_enabled else None,
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
        """Execute a query and stream rows in chunks with Oracle fetch tuning."""
        fetch_lobs = cast("bool | None", kwargs.pop("fetch_lobs", None))
        if chunk_size < 1:
            msg = "chunk_size must be greater than or equal to 1"
            raise ValueError(msg)
        config = statement_config or self.statement_config
        sql_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
        stream = self.dispatch_select_stream(sql_statement, chunk_size, fetch_lobs=fetch_lobs)
        if stream is not None:
            return stream._with_schema_type(schema_type)
        return super().select_stream(
            sql_statement,
            schema_type=schema_type,
            statement_config=config,
            chunk_size=chunk_size,
            native_only=native_only,
        )

    def dispatch_select_stream(
        self, statement: "SQL", chunk_size: int, fetch_lobs: bool | None = None
    ) -> "AsyncRowStream[dict[str, Any]] | None":
        """Return a native oracledb row stream backed by chunked ``fetchmany``."""
        if not statement.returns_rows():
            return None
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        stream_parameters = cast("list[object] | tuple[object, ...] | dict[object, object] | None", prepared_parameters)
        return AsyncRowStream(OracleAsyncStreamSource(self, sql, stream_parameters, chunk_size, fetch_lobs=fetch_lobs))

    def handle_database_exceptions(self) -> "OracleAsyncExceptionHandler":
        """Handle database-specific exceptions and wrap them appropriately."""
        return OracleAsyncExceptionHandler()

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

        config = statement_config or self.statement_config
        prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
        sql, prepared_parameters = self._compiled_sql(prepared_statement, config)

        prepared_parameters = await coerce_large_parameters_async(
            self.connection,
            prepared_parameters,
            clob_type=DB_TYPE_CLOB,
            blob_type=DB_TYPE_BLOB,
            varchar2_byte_limit=self.driver_features.get("oracle_varchar2_byte_limit", 4000),
            raw_byte_limit=self.driver_features.get("oracle_raw_byte_limit", 2000),
            version_cache=self._oracle_version_cache,
        )
        prepared_parameters = cast("list[Any] | tuple[Any, ...] | dict[Any, Any] | None", prepared_parameters)

        exc_handler = self.handle_database_exceptions()
        try:
            self._apply_session_tracing(self.connection)
            if return_format in {"batches", "reader"} and supports_df_batches(self.connection):
                batches_result = cast(
                    "tuple[list[ArrowRecordBatch], ArrowSchema | None]",
                    await self._run_with_exception_handler(
                        exc_handler,
                        self._fetch_arrow_record_batches,
                        sql,
                        prepared_parameters,
                        batch_size,
                        arrow_schema,
                    ),
                )
                record_batches, batch_schema = batches_result
                rows_affected = sum(batch.num_rows for batch in record_batches)
                if batch_schema is None:
                    batch_schema = pa.schema([])
                arrow_data: object
                if return_format == "reader":
                    arrow_data = pa.RecordBatchReader.from_batches(batch_schema, record_batches)
                else:
                    arrow_data = record_batches
                return create_arrow_result(prepared_statement, arrow_data, rows_affected=rows_affected)

            oracle_df = await self._run_with_exception_handler(
                exc_handler, self._execute_arrow_dataframe, sql, prepared_parameters, batch_size
            )
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
        self._check_pending_exception(exc_handler)

        arrow_table = coerce_oracle_df_to_arrow_table(oracle_df, pa)
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
        telemetry_payload = await self._write_storage_result(
            arrow_result, destination, format_hint=format_hint, pipeline=async_pipeline
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    async def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
        stage_table: "bool | str | None" = False,
        conflict_keys: "Sequence[str] | None" = None,
        on_conflict: Literal["update", "nothing"] = "update",
        exclude_update_columns: "Sequence[str] | None" = None,
        drop_stage_table: bool = True,
        drop_indexes: bool = False,
        concurrent_indexes: bool = True,
        type_casts: "Mapping[str, str] | None" = None,
    ) -> "StorageBridgeJob":
        """Asynchronously load Arrow data into Oracle."""
        self._require_capability("arrow_import_enabled")
        if drop_indexes:
            async with self.without_indexes(table, concurrent=concurrent_indexes):
                return await self.load_from_arrow(
                    table,
                    source,
                    partitioner=partitioner,
                    overwrite=overwrite,
                    telemetry=telemetry,
                    stage_table=stage_table,
                    conflict_keys=conflict_keys,
                    on_conflict=on_conflict,
                    exclude_update_columns=exclude_update_columns,
                    drop_stage_table=drop_stage_table,
                    drop_indexes=False,
                    concurrent_indexes=concurrent_indexes,
                    type_casts=type_casts,
                )
        if stage_table or conflict_keys:
            return await self._execute_staged_load_async(
                table,
                source,
                raw_loader=self.load_from_arrow,
                partitioner=partitioner,
                overwrite=overwrite,
                telemetry=telemetry,
                stage_table=stage_table,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                drop_stage_table=drop_stage_table,
                drop_indexes=False,
                concurrent_indexes=concurrent_indexes,
                type_casts=type_casts,
            )
        arrow_table = self._coerce_arrow_table(source)
        if overwrite:
            statement = build_truncate_statement(table)
            exc_handler = self.handle_database_exceptions()
            await self._run_with_exception_handler(exc_handler, self.connection.execute, statement)
            self._check_pending_exception(exc_handler)
        use_direct_path = (
            self.driver_features.get("enable_direct_path_load", True) is not False
            and supports_direct_path_load(self.connection)
            and _arrow_schema_supports_direct_path(arrow_table)
        )
        if arrow_table.num_rows:
            if use_direct_path:
                schema_name, table_name = _resolve_direct_path_target(self.connection, table)
                exc_handler = self.handle_database_exceptions()
                await self._run_with_exception_handler(
                    exc_handler,
                    self.connection.direct_path_load,
                    schema_name=schema_name,
                    table_name=table_name,
                    column_names=list(arrow_table.column_names),
                    data=arrow_table,
                )
                self._check_pending_exception(exc_handler)
            else:
                columns = list(arrow_table.column_names)
                statement = build_insert_statement(table, columns)
                exc_handler = self.handle_database_exceptions()

                async def _insert_batches() -> None:
                    async with self.with_cursor(self.connection) as cursor:
                        for batch in arrow_table.to_batches(max_chunksize=5000):
                            batch_rows = self._arrow_batch_to_rows(batch, columns)
                            coerced_rows = await coerce_many_parameters_async(
                                self.connection,
                                batch_rows,
                                clob_type=DB_TYPE_CLOB,
                                blob_type=DB_TYPE_BLOB,
                                varchar2_byte_limit=self.driver_features.get("oracle_varchar2_byte_limit", 4000),
                                raw_byte_limit=self.driver_features.get("oracle_raw_byte_limit", 2000),
                                version_cache=self._oracle_version_cache,
                            )
                            await cursor.executemany(statement, coerced_rows)

                await self._run_with_exception_handler(exc_handler, _insert_batches)
                self._check_pending_exception(exc_handler)
        telemetry_payload = self._ingest_telemetry(arrow_table)
        telemetry_payload["destination"] = table
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    async def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        credential_name: str | None = None,
        format_options: "Mapping[str, Any] | None" = None,
        **kwargs: Any,
    ) -> "StorageBridgeJob":
        """Asynchronously load staged artifacts into Oracle via ``DBMS_CLOUD.COPY_DATA`` or client-side Arrow."""
        raw_credential = (
            credential_name if credential_name is not None else self.driver_features.get("dbms_cloud_credential")
        )
        effective_credential = (
            raw_credential.strip() if isinstance(raw_credential, str) and raw_credential.strip() else None
        )
        if effective_credential is not None and not kwargs:
            resolved_source: StorageDestination = (
                self._storage_pipeline().resolve_destination(source).uri
                if isinstance(source, str) and source.startswith("alias://")
                else source
            )
            remote_uri = (
                resolve_dbms_cloud_source_uri(resolved_source, file_format=file_format)
                if isinstance(resolved_source, (str, Path))
                else None
            )
            if remote_uri is not None:
                plsql, binds = build_dbms_cloud_copy_data_sql(
                    table,
                    credential_name=effective_credential,
                    file_uri=remote_uri,
                    file_format=file_format,
                    format_options=format_options,
                )
                truncate_sql = build_truncate_statement(table) if overwrite else None
                rows_processed = 0
                exc_handler = self.handle_database_exceptions()

                async def _run_dbms_cloud_async() -> None:
                    nonlocal rows_processed
                    async with self.with_cursor(self.connection) as cursor:
                        if truncate_sql is not None:
                            await cursor.execute(truncate_sql)
                        await cursor.execute(plsql, binds)
                        rows_processed = resolve_rowcount(cursor)

                await self._run_with_exception_handler(exc_handler, _run_dbms_cloud_async)
                self._check_pending_exception(exc_handler)
                telemetry_payload: StorageTelemetry = {
                    "destination": table,
                    "backend": "dbms_cloud",
                    "format": file_format,
                    "rows_processed": rows_processed,
                    "extra": {"source_uri": remote_uri, "credential_name": effective_credential},
                }
                self._attach_partition_telemetry(telemetry_payload, partitioner)
                return self._storage_job(telemetry_payload)
        if kwargs:
            return await super().load_from_storage(
                table, source, file_format=file_format, partitioner=partitioner, overwrite=overwrite, **kwargs
            )
        arrow_table, inbound = await self._read_storage_arrow(source, file_format=file_format)
        return await self.load_from_arrow(
            table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound
        )

    def collect_rows(self, cursor: Any, fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        """Collect Oracle async rows for the direct execution path.

        Falls back to the standard async dispatch path when rows contain async
        LOB locators, because those must be read with ``collect_async_rows``.
        """
        column_names, requires_lob_coercion = self._resolve_row_metadata(cursor.description)
        if fetched and any(_row_has_async_readable(row) for row in fetched):
            msg = "Oracle async LOB locators require async row collection"
            raise NotImplementedError(msg)
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
        """Report whether a transaction is open on the connection.

        Oracle opens a transaction implicitly on the first DML, which the
        ownership flag alone cannot see, while ``begin()`` sets the flag without
        issuing SQL, which the native predicate alone cannot see. Both signals
        are required.
        """
        return self._transaction_active or bool(self.connection.transaction_in_progress)

    def _detect_oracledb_version(self) -> "tuple[int, int, int]":
        return ORACLEDB_VERSION

    def _resolve_row_metadata(self, description: Any) -> "tuple[list[str], bool]":
        return resolve_row_metadata(description, self.driver_features, self._row_metadata_cache)

    async def _dispatch_explain_plan(
        self,
        cursor: Any,
        explained_sql: str,
        parameters: Any,
        *,
        display_cursor: bool = False,
        xplan_format: str = "TYPICAL",
    ) -> "ExecutionResult":
        """Request a plan, read it back, and release the plan table rows.

        Oracle writes the plan to a table instead of returning it, so the plan is
        tagged with a generated statement id, formatted through ``DBMS_XPLAN``, and
        deleted again. All three statements run in the caller's transaction, so the
        plan is read before a rollback can discard it. When ``display_cursor`` is
        enabled, the statement is executed first and formatted through
        ``DBMS_XPLAN.DISPLAY_CURSOR``.

        Args:
            cursor: Oracle cursor object.
            explained_sql: Statement whose plan is wanted.
            parameters: Bind values for the explained statement.
            display_cursor: Whether to inspect actual cursor execution statistics via ``DBMS_XPLAN.DISPLAY_CURSOR``.
            xplan_format: ``DBMS_XPLAN`` format string.

        Returns:
            Execution result carrying the formatted plan rows.
        """
        if display_cursor:
            await cursor.execute(explained_sql, parameters or {}, **self._fetch_kwargs)
            if cursor.description is not None:
                await cursor.fetchall()
            await cursor.execute(_format_display_cursor_sql(xplan_format), {}, **self._fetch_kwargs)
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

        statement_id = _new_plan_statement_id()
        await cursor.execute(_tagged_explain_plan_sql(statement_id, explained_sql), parameters or {})
        display_sql = PLAN_DISPLAY_SQL if xplan_format == "TYPICAL" else _format_plan_display_sql(xplan_format)

        try:
            await cursor.execute(display_sql, {"statement_id": statement_id}, **self._fetch_kwargs)
            fetched_data = await cursor.fetchall()
            column_names, requires_lob_coercion = self._resolve_row_metadata(cursor.description)
            data, column_names = await collect_async_rows(
                cast("list[Any] | None", fetched_data),
                cursor.description,
                self.driver_features,
                column_names=column_names,
                requires_lob_coercion=requires_lob_coercion,
            )
            result = self.create_execution_result(
                cursor,
                selected_data=data,
                column_names=column_names,
                data_row_count=len(data),
                is_select_result=True,
                row_format="tuple",
            )
        except Exception:
            try:
                await cursor.execute(PLAN_CLEANUP_SQL, {"statement_id": statement_id})
            except Exception:
                logger.warning("Failed to clean up Oracle plan rows after plan retrieval failed", exc_info=True)
            raise
        else:
            await cursor.execute(PLAN_CLEANUP_SQL, {"statement_id": statement_id})
        return result

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
        fetch_kwargs = build_arrow_fetch_kwargs(self.driver_features)
        try:
            return await self.connection.fetch_df_all(
                statement=sql, parameters=params, arraysize=batch_size or 1000, **fetch_kwargs
            )
        except TypeError as exc:
            retry_kwargs = _retry_arrow_without_fetch_lobs(exc, fetch_kwargs)
            if retry_kwargs is None:
                raise
            return await self.connection.fetch_df_all(
                statement=sql, parameters=params, arraysize=batch_size or 1000, **retry_kwargs
            )

    async def _fetch_arrow_record_batches(
        self,
        sql: str,
        parameters: "StatementParameters | None",
        batch_size: int | None,
        arrow_schema: "ArrowSchema | None",
    ) -> "tuple[list[ArrowRecordBatch], ArrowSchema | None]":
        params = parameters if parameters is not None else []
        record_batches: list[ArrowRecordBatch] = []
        batch_schema: ArrowSchema | None = None
        fetch_kwargs = build_arrow_fetch_kwargs(self.driver_features, arrow_schema=arrow_schema)
        try:
            oracle_dfs = self.connection.fetch_df_batches(
                statement=sql, parameters=params, size=batch_size or 1000, **fetch_kwargs
            )
        except TypeError as exc:
            retry_kwargs = _retry_arrow_without_fetch_lobs(exc, fetch_kwargs)
            if retry_kwargs is None:
                raise
            oracle_dfs = self.connection.fetch_df_batches(
                statement=sql, parameters=params, size=batch_size or 1000, **retry_kwargs
            )
        async for oracle_df in oracle_dfs:
            batch_table = coerce_oracle_df_to_arrow_table(oracle_df, pa)
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

        raw_operations = [self._prepare_pipeline_operation(op) for op in stack.operations]
        compiled_operations: list[_CompiledStackOperation] = []
        pipeline = create_oracle_pipeline()
        varchar2_limit = self.driver_features.get("oracle_varchar2_byte_limit", 4000)
        raw_limit = self.driver_features.get("oracle_raw_byte_limit", 2000)
        for compiled in raw_operations:
            if compiled.method == "execute_many":
                coerced_params: Any = await coerce_many_parameters_async(
                    self.connection,
                    compiled.parameters,
                    clob_type=DB_TYPE_CLOB,
                    blob_type=DB_TYPE_BLOB,
                    varchar2_byte_limit=varchar2_limit,
                    raw_byte_limit=raw_limit,
                    version_cache=self._oracle_version_cache,
                )
            else:
                coerced_params = await coerce_large_parameters_async(
                    self.connection,
                    compiled.parameters,
                    clob_type=DB_TYPE_CLOB,
                    blob_type=DB_TYPE_BLOB,
                    varchar2_byte_limit=varchar2_limit,
                    raw_byte_limit=raw_limit,
                    version_cache=self._oracle_version_cache,
                )
            coerced_op = compiled._replace(parameters=coerced_params)
            compiled_operations.append(coerced_op)
            self._add_pipeline_operation(pipeline, coerced_op)

        results: list[StackResult] = []
        owns_transaction = not self._connection_in_transaction()

        with StackExecutionObserver(self, stack, continue_on_error, native_pipeline=True) as observer:
            try:
                if owns_transaction and not continue_on_error:
                    await self.begin()

                self._apply_session_tracing(self.connection)
                pipeline_results = await self.connection.run_pipeline(pipeline, continue_on_error=True)
                results = self._build_stack_results_from_pipeline(
                    compiled_operations, pipeline_results, continue_on_error, observer
                )

                if owns_transaction:
                    await self.commit()
            except Exception as exc:
                if owns_transaction:
                    try:
                        await self.rollback()
                    except Exception as rollback_error:  # pragma: no cover
                        logger.debug("Rollback after pipeline failure failed: %s", rollback_error)
                if isinstance(exc, StackExecutionError):
                    raise
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

        if not connection_is_thin(self.connection):
            self._pipeline_support = False
            self._pipeline_support_reason = "thin_mode_required"
            return False

        connection_major = resolve_oracle_connection_major(self.connection, self._oracle_version_cache)
        if connection_major is not None:
            if connection_major >= PIPELINE_MIN_DATABASE_MAJOR:
                self._pipeline_support = True
                self._pipeline_support_reason = None
                return True
            self._pipeline_support = False
            self._pipeline_support_reason = "database_version"
            return False

        version_info = await self.data_dictionary.get_version(self)
        if version_info and version_info.major >= PIPELINE_MIN_DATABASE_MAJOR:
            self._pipeline_support = True
            self._pipeline_support_reason = None
            return True

        self._pipeline_support = False
        self._pipeline_support_reason = "database_version"
        return False


register_driver_profile("oracledb", driver_profile)


class _CompiledStackOperation(NamedTuple):
    statement: SQL
    sql: str
    parameters: Any
    method: str
    returns_rows: bool
    summary: str


def _arrow_schema_supports_direct_path(arrow_table: Any) -> bool:
    """Report whether every column maps to a type direct path load can ingest.

    This is an allow-list rather than a nested-type check. oracledb converts a
    limited set of Arrow types, and a conversion it cannot perform fails part
    way through the load, so anything unrecognized takes the insert path.

    Args:
        arrow_table: The Arrow table about to be ingested.

    Returns:
        True when every column type is known to be convertible.
    """
    ensure_pyarrow()

    for field in arrow_table.schema:
        data_type = field.type
        supported = (
            pa.types.is_integer(data_type)
            or (pa.types.is_floating(data_type) and not pa.types.is_float16(data_type))
            or pa.types.is_boolean(data_type)
            or pa.types.is_string(data_type)
            or pa.types.is_large_string(data_type)
            or pa.types.is_binary(data_type)
            or pa.types.is_large_binary(data_type)
            or pa.types.is_timestamp(data_type)
            or pa.types.is_decimal128(data_type)
        )
        if not supported:
            return False
    return True


def _resolve_direct_path_target(connection: Any, table: str) -> tuple[str, str]:
    parts = split_qualified_identifier(table, quote_chars='"', allow_bracket_quotes=False)
    if not parts:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    if len(parts) == 1:
        return connection.username, parts[0]
    return ".".join(parts[:-1]), parts[-1]


def _row_has_async_readable(row: Any) -> bool:
    if isinstance(row, dict):
        return any(is_async_readable(value) for value in row.values())
    try:
        values = row if isinstance(row, (list, tuple)) else tuple(row)
    except TypeError:
        values = (row,)
    return any(is_async_readable(value) for value in values)


def _managed_explain_statement(statement: "SQL", sql: str) -> "str | None":
    """Extract a SQLSpec-built Oracle plan request without parsing SQL text.

    ``Explain.build()`` marks its SQLGlot command expression. Raw caller SQL has no
    marker and stays on the ordinary one-statement path. The marked form has one exact
    generated prefix, so extracting the inner statement is a constant-prefix slice,
    not a second SQL parse.

    Args:
        statement: Prepared SQL object.
        sql: Compiled statement text.

    Returns:
        The inner statement, or None when SQLSpec does not own the plan flow.

    Raises:
        SQLSpecError: If a marked statement no longer has SQLSpec's generated shape.
    """
    expression = statement.raw_expression
    if expression is None or not expression.meta.get(ORACLE_MANAGED_EXPLAIN_META_KEY, False):
        return None

    if not sql.startswith(ORACLE_EXPLAIN_PREFIX) or len(sql) == len(ORACLE_EXPLAIN_PREFIX):
        msg = "SQLSpec-managed Oracle EXPLAIN statement lost its generated prefix"
        raise SQLSpecError(msg)
    return sql[len(ORACLE_EXPLAIN_PREFIX) :]


def _new_plan_statement_id() -> str:
    """Build an identifier that isolates one plan within the session plan table.

    Returns:
        Identifier of at most ``PLAN_STATEMENT_ID_MAX_LENGTH`` alphanumeric characters.
    """
    suffix_length = PLAN_STATEMENT_ID_MAX_LENGTH - len(PLAN_STATEMENT_ID_PREFIX)
    return f"{PLAN_STATEMENT_ID_PREFIX}{uuid4().hex[:suffix_length]}"


def _tagged_explain_plan_sql(statement_id: str, explained_sql: str) -> str:
    """Rewrite a plan request so its rows can be located and removed again.

    Args:
        statement_id: Identifier produced by ``_new_plan_statement_id``.
        explained_sql: Statement whose plan is wanted, with its bind placeholders intact.

    Returns:
        Plan request tagged with the statement id.
    """
    return f"EXPLAIN PLAN SET STATEMENT_ID = '{statement_id}' FOR {explained_sql}"


def _format_plan_display_sql(xplan_format: str = "TYPICAL") -> str:
    """Build a DBMS_XPLAN.DISPLAY query with a validated format string."""
    if "'" in xplan_format:
        msg = f"Invalid Oracle XPLAN format string: {xplan_format!r}"
        raise SQLSpecError(msg)
    return f"SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY(NULL, :statement_id, '{xplan_format}'))"


def _format_display_cursor_sql(xplan_format: str = "TYPICAL") -> str:
    """Build a DBMS_XPLAN.DISPLAY_CURSOR query with a validated format string."""
    if "'" in xplan_format:
        msg = f"Invalid Oracle XPLAN format string: {xplan_format!r}"
        raise SQLSpecError(msg)
    return f"SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(NULL, NULL, '{xplan_format}'))"


def _managed_explain_options(statement: "SQL") -> "tuple[bool, str]":
    """Extract Oracle display_cursor and xplan_format options from a managed Explain statement."""
    expression = statement.raw_expression
    if expression is None:
        return False, "TYPICAL"
    display_cursor = bool(expression.meta.get(ORACLE_EXPLAIN_DISPLAY_CURSOR_META_KEY, False))
    raw_format = expression.meta.get(ORACLE_EXPLAIN_XPLAN_FORMAT_META_KEY)
    xplan_format = str(raw_format) if isinstance(raw_format, str) and raw_format else "TYPICAL"
    return display_cursor, xplan_format


def _retry_arrow_without_fetch_lobs(exc: TypeError, fetch_kwargs: "dict[str, object]") -> "dict[str, object] | None":
    if "fetch_lobs" not in fetch_kwargs or "fetch_lobs" not in str(exc):
        return None
    retry_kwargs = dict(fetch_kwargs)
    retry_kwargs.pop("fetch_lobs", None)
    return retry_kwargs


def _extract_statement_binds(statement: str) -> "tuple[set[str], int]":
    """Extract named bind names (lowercased) and count of positional binds outside quotes and comments."""
    named_binds: set[str] = set()
    positional_count = 0
    n = len(statement)
    i = 0
    while i < n:
        ch = statement[i]
        if ch == "-" and i + 1 < n and statement[i + 1] == "-":
            end = statement.find("\n", i + 2)
            i = n if end == -1 else end + 1
            continue
        if ch == "/" and i + 1 < n and statement[i + 1] == "*":
            end = statement.find("*/", i + 2)
            i = n if end == -1 else end + 2
            continue
        if ch == "'":
            end = i + 1
            while end < n:
                if statement[end] == "'":
                    if end + 1 < n and statement[end + 1] == "'":
                        end += 2
                        continue
                    end += 1
                    break
                end += 1
            i = end
            continue
        if ch == '"':
            end = i + 1
            while end < n:
                if statement[end] == '"':
                    if end + 1 < n and statement[end + 1] == '"':
                        end += 2
                        continue
                    end += 1
                    break
                end += 1
            i = end
            continue
        if ch == ":" and (i == 0 or statement[i - 1] != ":"):
            start = i + 1
            if start < n and statement[start] != ":":
                end = start
                while end < n and (statement[end].isalnum() or statement[end] in {"_", "$", "#"}):
                    end += 1
                if end > start:
                    token = statement[start:end]
                    if token.isdigit():
                        positional_count += 1
                    else:
                        named_binds.add(token.lower())
                    i = end
                    continue
        i += 1
    return named_binds, positional_count


def _slice_script_parameters(statement: str, parameters: Any, pos_offset: int) -> "tuple[Any, int]":
    """Filter dictionary binds or slice positional binds for a single statement."""
    if not parameters:
        return {}, pos_offset
    named_binds, pos_count = _extract_statement_binds(statement)
    if isinstance(parameters, dict):
        filtered = {k: v for k, v in parameters.items() if str(k).lower() in named_binds}
        return filtered, pos_offset
    if isinstance(parameters, (list, tuple)):
        if pos_count > 0:
            sliced = list(parameters[pos_offset : pos_offset + pos_count])
            return sliced, pos_offset + pos_count
        return [], pos_offset
    return parameters, pos_offset
