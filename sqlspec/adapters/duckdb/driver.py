"""DuckDB driver implementation."""

import contextlib
from time import perf_counter
from typing import TYPE_CHECKING, Any, Literal, cast

from sqlglot import exp

from sqlspec.adapters.duckdb._typing import DuckDBCursor, DuckDBSessionContext
from sqlspec.adapters.duckdb._typing import duckdb_module as duckdb
from sqlspec.adapters.duckdb.core import (
    ARROW_MIN_ROWS,
    _build_storage_copy_sql,
    _build_storage_read_sql,
    _DuckDBStreamSource,
    _resolve_native_storage_target,
    _restore_uuid_columns,
    collect_rows,
    configure_duckdb_statement_profiling,
    create_duckdb_profile_path,
    create_mapped_exception,
    default_statement_config,
    description_has_arrow_divergent_types,
    driver_profile,
    format_identifier,
    normalize_execute_parameters,
    read_duckdb_profile_json,
    resolve_rowcount,
)
from sqlspec.adapters.duckdb.data_dictionary import DuckDBDataDictionary
from sqlspec.core import (
    SQL,
    ParameterStyle,
    StatementConfig,
    build_arrow_result_from_reader,
    build_arrow_result_from_table,
    get_cache_config,
    register_driver_profile,
)
from sqlspec.core.result import DMLResult
from sqlspec.driver import BaseSyncExceptionHandler, SyncDriverAdapterBase, SyncRowStream
from sqlspec.exceptions import SQLSpecError
from sqlspec.typing import import_optional
from sqlspec.utils.arrow_helpers import arrow_reader_with_deferred_close
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.text import quote_identifier
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from collections.abc import Sequence

    import pyarrow as pa

    from sqlspec.adapters.duckdb._typing import DuckDBConnection
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, SQLResult, Statement, StatementFilter
    from sqlspec.core.result._base import RowFormat
    from sqlspec.driver import ExecutionResult
    from sqlspec.driver._query_cache import CachedQuery
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.typing import ArrowReturnFormat, StatementParameters
else:
    pa = import_optional("pyarrow")

__all__ = ("DuckDBCursor", "DuckDBDriver", "DuckDBExceptionHandler", "DuckDBSessionContext")

logger = get_logger("sqlspec.adapters.duckdb")


class DuckDBExceptionHandler(BaseSyncExceptionHandler):
    """Context manager for handling DuckDB database exceptions.

    Uses exception type and message-based detection to map DuckDB errors
    to specific SQLSpec exceptions for better error handling.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __exit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        if exc_type is None:
            return False
        if issubclass(exc_type, duckdb.Error):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class DuckDBDriver(SyncDriverAdapterBase):
    """Synchronous DuckDB database driver.

    Provides SQL statement execution, transaction management, and result handling
    for DuckDB databases. Supports multiple parameter styles including QMARK,
    NUMERIC, and NAMED_DOLLAR formats.

    The driver handles script execution, batch operations, and integrates with
    the sqlspec.core modules for statement processing and caching.
    """

    __slots__ = ("_data_dictionary", "_transaction_active")
    dialect = "duckdb"

    def __init__(
        self,
        connection: "DuckDBConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )
        driver_features = dict(driver_features) if driver_features else {}

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: DuckDBDataDictionary | None = None
        self._transaction_active = False

    @property
    def data_dictionary(self) -> "DuckDBDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = DuckDBDataDictionary()
        return self._data_dictionary

    def dispatch_execute(self, cursor: "DuckDBConnection", statement: SQL) -> "ExecutionResult":
        """Execute single SQL statement with data handling.

        Executes a SQL statement with parameter binding and processes the results.
        Handles both data-returning queries and data modification operations.

        Args:
            cursor: DuckDB cursor object
            statement: SQL statement to execute

        Returns:
            ExecutionResult with execution metadata
        """
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        profiling_enabled = bool(self.driver_features.get("enable_statement_profiling", False))
        if profiling_enabled:
            self._ensure_statement_profiling(cursor)
        cursor.execute(sql, normalize_execute_parameters(prepared_parameters))

        is_select_like = statement.returns_rows() or self._should_force_select(statement, cursor)

        if is_select_like:
            data, column_names, row_count, row_format = self._materialize_select_rows(cursor, sql)
            if profiling_enabled:
                self._capture_statement_profile(sql)
            return self.create_execution_result(
                cursor,
                selected_data=data,
                column_names=column_names,
                data_row_count=row_count,
                is_select_result=True,
                row_format=row_format,
            )

        row_count = resolve_rowcount(cursor)
        if profiling_enabled:
            with contextlib.suppress(Exception):
                cursor.fetchall()
            self._capture_statement_profile(sql)

        return self.create_execution_result(cursor, rowcount_override=row_count)

    def dispatch_execute_many(self, cursor: "DuckDBConnection", statement: SQL) -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using batch processing.

        Uses DuckDB's Arrow bulk insert fast path for simple INSERT batches,
        falling back to executemany and calculating row counts for both data
        modification and query operations.

        Args:
            cursor: DuckDB cursor object
            statement: SQL statement with multiple parameter sets

        Returns:
            ExecutionResult with batch execution metadata
        """
        cached_statement, prepared_parameters = self._compiled_statement(statement, self.statement_config)
        sql = cached_statement.compiled_sql

        if prepared_parameters:
            parsed_expression = cached_statement.expression or statement._peek_expression()
            if isinstance(parsed_expression, exp.Insert) and not parsed_expression.args.get("returning"):
                parsed_expression.meta["compiled_sql"] = sql
                bulk_result = self._execute_bulk_insert_many(parsed_expression, prepared_parameters)
                if bulk_result is not None:
                    return self.create_execution_result(
                        cursor,
                        rowcount_override=bulk_result.rows_affected,
                        special_data={"dml_result": bulk_result},
                        is_many_result=True,
                    )
            parameter_sets = cast("list[Any]", prepared_parameters)
            cursor.executemany(sql, parameter_sets)

            row_count = len(parameter_sets) if statement.is_modifying_operation() else resolve_rowcount(cursor)
        else:
            row_count = 0

        return self.create_execution_result(cursor, rowcount_override=row_count, is_many_result=True)

    def build_statement_result(self, statement: SQL, execution_result: "ExecutionResult") -> "SQLResult":
        """Build statement result, returning DMLResult when produced by bulk insert."""
        if execution_result.special_data and isinstance(execution_result.special_data.get("dml_result"), DMLResult):
            statement._pooled = False
            return cast("DMLResult", execution_result.special_data["dml_result"])
        return super().build_statement_result(statement, execution_result)

    def dispatch_execute_script(self, cursor: "DuckDBConnection", statement: SQL) -> "ExecutionResult":
        """Execute SQL script with statement splitting and parameter handling.

        Parses multi-statement scripts and executes each statement sequentially
        with the provided parameters.

        Args:
            cursor: DuckDB cursor object
            statement: SQL statement with script content

        Returns:
            ExecutionResult with script execution metadata
        """
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        statement_count = len(statements)
        successful_count = 0
        last_result = None
        runtime = self._observability
        has_observers = runtime is not None and runtime.has_statement_observers
        profiling_enabled = bool(self.driver_features.get("enable_statement_profiling", False))
        if profiling_enabled:
            self._ensure_statement_profiling(cursor)

        for idx, stmt in enumerate(statements):
            stmt_started = perf_counter() if has_observers else 0.0
            last_result = cursor.execute(stmt, normalize_execute_parameters(prepared_parameters))
            if profiling_enabled and last_result is not None:
                with contextlib.suppress(Exception):
                    last_result.fetchall()
            stmt_profile = self._capture_statement_profile(stmt) if profiling_enabled else None
            if has_observers:
                stmt_duration = perf_counter() - stmt_started
                single_stmt = self._sub_statement(stmt, prepared_parameters)
                try:
                    stmt_operation = single_stmt.operation_type
                finally:
                    self._release_pooled_statement(single_stmt)
                self._emit_script_statement_event(
                    sql=stmt,
                    parameters=prepared_parameters,
                    operation=stmt_operation,
                    rows_affected=None,
                    duration_s=stmt_duration,
                    started_at=stmt_started,
                    profile=stmt_profile,
                    script_ordinal=idx,
                    script_statement_count=statement_count,
                )
            successful_count += 1

        self._pending_statement_profile = None
        return self.create_execution_result(
            last_result, statement_count=statement_count, successful_statements=successful_count, is_script_result=True
        )

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "SyncRowStream[dict[str, Any]] | None":
        """Return a native DuckDB row stream backed by Arrow record batches."""
        if not statement.returns_rows():
            return None
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        return SyncRowStream(_DuckDBStreamSource(self._open_stream_reader, sql, prepared_parameters, chunk_size))

    def begin(self) -> None:
        """Begin a database transaction."""
        try:
            self.connection.execute("BEGIN TRANSACTION")
        except duckdb.Error as e:
            raise create_mapped_exception(e) from e
        self._transaction_active = True

    def commit(self) -> None:
        """Commit the current transaction."""
        try:
            self.connection.commit()
        except duckdb.Error as e:
            raise create_mapped_exception(e) from e
        finally:
            self._transaction_active = False

    def rollback(self) -> None:
        """Rollback the current transaction."""
        try:
            self.connection.rollback()
        except duckdb.Error as e:
            raise create_mapped_exception(e) from e
        finally:
            self._transaction_active = False

    def create_savepoint(self, name: str) -> None:
        """Raise because DuckDB does not support savepoints.

        Raises:
            NotImplementedError: Always.
        """
        msg = "DuckDB does not support savepoints."
        raise NotImplementedError(msg)

    def release_savepoint(self, name: str) -> None:
        """Raise because DuckDB does not support savepoints.

        Raises:
            NotImplementedError: Always.
        """
        msg = "DuckDB does not support savepoints."
        raise NotImplementedError(msg)

    def rollback_to_savepoint(self, name: str) -> None:
        """Raise because DuckDB does not support savepoints.

        Raises:
            NotImplementedError: Always.
        """
        msg = "DuckDB does not support savepoints."
        raise NotImplementedError(msg)

    def set_migration_session_schema(self, schema: str) -> None:
        """Set DuckDB search_path for migration SQL."""
        self.connection.execute(f"SET search_path = {quote_identifier(schema)}")

    def has_schema(self, schema: str) -> bool:
        """Return whether a DuckDB schema exists."""
        result = self.connection.execute(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = ?", [schema]
        ).fetchone()
        return result is not None

    def with_cursor(self, connection: "DuckDBConnection") -> "DuckDBCursor":
        """Create context manager for DuckDB cursor.

        Args:
            connection: DuckDB connection instance

        Returns:
            DuckDBCursor context manager instance
        """
        return DuckDBCursor(connection)

    def handle_database_exceptions(self) -> "DuckDBExceptionHandler":
        """Handle database-specific exceptions and wrap them appropriately.

        Returns:
            Exception handler with deferred exception pattern for mypyc compatibility.
        """
        return DuckDBExceptionHandler()

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
        """Execute query and return results as Apache Arrow (DuckDB native path).

        DuckDB provides native Arrow support via cursor.arrow().
        This is the fastest path due to DuckDB's columnar architecture.

        Args:
            statement: SQL statement, string, or QueryBuilder
            *parameters: Query parameters or filters
            statement_config: Optional statement configuration override
            return_format: "table" for pyarrow.Table (default), "batch" for RecordBatch,
                "batches" for list of RecordBatch, "reader" for RecordBatchReader
            native_only: Ignored for DuckDB (always uses native path)
            batch_size: Batch size hint (for future streaming implementation)
            arrow_schema: Optional pyarrow.Schema for type casting
            **kwargs: Additional keyword arguments

        Returns:
            ArrowResult with native Arrow data
        """
        ensure_pyarrow()

        config = statement_config or self.statement_config
        prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)

        exc_handler = self.handle_database_exceptions()
        arrow_result: ArrowResult | None = None

        if return_format in {"reader", "batches"}:
            stream_cursor, close_cursor = self._acquire_stream_cursor()
            try:
                with exc_handler:
                    sql, driver_params = self._compiled_sql(prepared_statement, config)
                    stream_cursor.execute(sql, driver_params or ())
                    arrow_reader = (
                        stream_cursor.to_arrow_reader(batch_size)
                        if batch_size is not None
                        else stream_cursor.to_arrow_reader()
                    )
                    if close_cursor:
                        arrow_reader = arrow_reader_with_deferred_close(arrow_reader, stream_cursor.close)
                    arrow_result = build_arrow_result_from_reader(
                        prepared_statement,
                        arrow_reader,
                        return_format=return_format,
                        batch_size=batch_size,
                        arrow_schema=arrow_schema,
                    )
                self._check_pending_exception(exc_handler)
            except BaseException:
                if close_cursor:
                    with contextlib.suppress(Exception):
                        stream_cursor.close()
                raise
        else:
            with self.with_cursor(self.connection) as cursor, exc_handler:
                sql, driver_params = self._compiled_sql(prepared_statement, config)
                cursor.execute(sql, driver_params or ())
                arrow_table = cursor.to_arrow_table()
                arrow_result = build_arrow_result_from_table(
                    prepared_statement,
                    arrow_table,
                    return_format=return_format,
                    batch_size=batch_size,
                    arrow_schema=arrow_schema,
                )
            self._check_pending_exception(exc_handler)

        if arrow_result is None:
            msg = "Unreachable"
            raise RuntimeError(msg)  # pragma: no cover

        return arrow_result

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
        """Persist a query with native object-store COPY when settings are compatible."""

        self._require_capability("arrow_export_enabled")
        sync_pipeline = self._storage_pipeline()
        format_choice = format_hint or "parquet"
        if (
            self.storage_pipeline_factory is None
            and format_choice in {"parquet", "csv"}
            and not {"arrow_schema", "return_format", "batch_size", "native_only"}.intersection(kwargs)
        ):
            target = _resolve_native_storage_target(sync_pipeline, destination, self.driver_features, write=True)
            if target is not None:
                config = statement_config or self.statement_config
                config = config.replace(
                    parameter_config=config.parameter_config.replace(
                        default_execution_parameter_style=ParameterStyle.QMARK
                    )
                )
                prepared = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
                sql, driver_params = self._compiled_sql(prepared, config)
                copy_sql = _build_storage_copy_sql(sql, format_choice)
                if copy_sql is not None:
                    return self._native_storage_job(
                        copy_sql,
                        [target.uri, *cast("Sequence[Any]", driver_params or ())],
                        destination=str(destination),
                        format_choice=format_choice,
                        operation="write",
                        partitioner=partitioner,
                        telemetry=telemetry,
                    )
        arrow_result = self.select_to_arrow(statement, *parameters, statement_config=statement_config, **kwargs)
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
    ) -> "StorageBridgeJob":
        """Load Arrow data into DuckDB using temporary table registration."""

        self._require_capability("arrow_import_enabled")
        ensure_pyarrow()

        source_data = source.get_data() if hasattr(source, "get_data") else source
        arrow_table = None
        arrow_source: object
        if isinstance(source_data, pa.RecordBatchReader):
            arrow_source = source_data
        else:
            arrow_table = self._coerce_arrow_table(source_data)
            arrow_source = arrow_table
        table_name = format_identifier(table)
        temp_view = f"_sqlspec_arrow_{uuid4().hex}"
        inserted_rows = 0
        manage_transaction = overwrite and not self._connection_in_transaction()
        exc_handler = self.handle_database_exceptions()
        with exc_handler:
            if manage_transaction:
                self.connection.execute("BEGIN TRANSACTION")
            try:
                if overwrite:
                    self.connection.execute(f"TRUNCATE TABLE {table_name}")
                self.connection.register(temp_view, arrow_source)
                try:
                    insert_result = self.connection.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_view}")
                    inserted_rows = _resolve_duckdb_inserted_rows(insert_result)
                finally:
                    with contextlib.suppress(Exception):
                        self.connection.unregister(temp_view)
                if manage_transaction:
                    self.connection.commit()
            except BaseException:
                if manage_transaction:
                    with contextlib.suppress(Exception):
                        self.connection.rollback()
                raise
        self._check_pending_exception(exc_handler)

        if isinstance(source_data, pa.RecordBatchReader):
            telemetry_payload: StorageTelemetry = {
                "rows_processed": inserted_rows,
                "bytes_processed": 0,
                "format": "arrow",
            }
        else:
            if arrow_table is None:
                msg = "DuckDB Arrow load did not resolve an Arrow table."
                raise SQLSpecError(msg)
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
    ) -> "StorageBridgeJob":
        """Read an artifact from storage and load it into DuckDB."""

        self._require_capability("arrow_import_enabled")
        if not overwrite and file_format == "parquet" and self.storage_pipeline_factory is None:
            pipeline = self._storage_pipeline()
            target = _resolve_native_storage_target(pipeline, source, self.driver_features, write=False)
            if target is not None:
                read_sql = _build_storage_read_sql(table, target.uri, file_format)
                if read_sql is not None:
                    return self._native_storage_job(
                        read_sql,
                        [target.uri],
                        destination=table,
                        format_choice=file_format,
                        operation="read",
                        partitioner=partitioner,
                    )
        arrow_table, inbound = self._read_storage_arrow(source, file_format=file_format)
        return self.load_from_arrow(table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound)

    def collect_rows(self, cursor: "DuckDBConnection", fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        """Collect DuckDB rows for the direct execution path."""
        data, column_names = collect_rows(cast("list[Any] | None", fetched), cursor.description)
        return data, column_names, len(data)

    def resolve_rowcount(self, cursor: "DuckDBConnection") -> int:
        """Resolve rowcount from DuckDB cursor for the direct execution path."""
        return resolve_rowcount(cursor)

    def _execute_cache_hit_with_cursor(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: "CachedQuery"
    ) -> "SQLResult":
        """Execute a cached query using DuckDB's unified row materializer."""
        profiling_enabled = bool(self.driver_features.get("enable_statement_profiling", False))
        with self.with_cursor(self.connection) as cursor:
            if profiling_enabled:
                self._ensure_statement_profiling(cursor)
            wire_sql = self._format_wire_sql(cached.compiled_sql)
            cursor.execute(wire_sql, normalize_execute_parameters(params))
            if cached.operation_profile.returns_rows:
                data, column_names, row_count, row_format = self._materialize_select_rows(cursor, cached.compiled_sql)
                if profiling_enabled:
                    self._capture_statement_profile(wire_sql)
                return self.build_cached_select_result(
                    sql,
                    params,
                    cached,
                    selected_data=data,
                    column_names=column_names,
                    data_row_count=row_count,
                    row_format=row_format,
                    compiled_sql=wire_sql,
                )
            affected_rows = resolve_rowcount(cursor)
            if profiling_enabled:
                with contextlib.suppress(Exception):
                    cursor.fetchall()
                self._capture_statement_profile(wire_sql)
            return DMLResult(cached.operation_type, affected_rows)

    def _ensure_statement_profiling(self, cursor: Any) -> None:
        """Ensure per-connection DuckDB JSON statement profiling is configured."""
        if self.driver_features.get("_duckdb_profile_path"):
            return
        profile_path = create_duckdb_profile_path()
        configure_duckdb_statement_profiling(cursor, profile_path)
        self.driver_features["_duckdb_profile_path"] = str(profile_path)

    def _capture_statement_profile(self, executed_sql: str) -> "dict[str, Any] | None":
        """Read the latest DuckDB JSON profile and stage it for StatementEvent emission."""
        profile_path = self.driver_features.get("_duckdb_profile_path")
        profile = read_duckdb_profile_json(profile_path, executed_sql)
        self._pending_statement_profile = profile
        return profile

    def _materialize_select_rows(self, cursor: Any, compiled_sql: str) -> "tuple[list[Any], list[str], int, RowFormat]":
        """Materialize SELECT rows using DuckDB's native Python type contract."""
        description = cursor.description
        hint = self._cache_holder.get_row_count_hint(compiled_sql)
        if (
            (hint is not None and hint < ARROW_MIN_ROWS)
            or description_has_arrow_divergent_types(description)
            or not hasattr(cursor, "to_arrow_table")
        ):
            fetched_data = cursor.fetchall()
            data, column_names = collect_rows(fetched_data, description)
            row_count = len(data)
            self._cache_holder.set_row_count_hint(compiled_sql, row_count)
            return data, column_names, row_count, "tuple"

        arrow_table = cursor.to_arrow_table()
        data = arrow_table.to_pylist()
        _restore_uuid_columns(data, description)
        column_names = list(arrow_table.column_names)
        row_count = len(data)
        self._cache_holder.set_row_count_hint(compiled_sql, row_count)
        return data, column_names, row_count, "dict"

    def _native_storage_job(
        self,
        sql: str,
        parameters: "list[Any]",
        *,
        destination: str,
        format_choice: "StorageFormat",
        operation: "Literal['read', 'write']",
        partitioner: "dict[str, object] | None",
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        runtime = self.observability
        span = runtime.start_storage_span(operation, destination=destination, format_label=format_choice)
        start = perf_counter()
        try:
            rows = self._execute_storage_sql(sql, parameters)
            path = destination
            if operation == "write":
                path = destination.partition("://")[2]
                if destination.startswith("alias://"):
                    path = path.partition("/")[2].strip().lstrip("/")
            payload: StorageTelemetry = {
                "destination": path,
                "rows_processed": rows,
                "duration_s": perf_counter() - start,
                "format": format_choice,
                "backend": "duckdb",
            }
            payload = runtime.annotate_storage_telemetry(payload)
        except Exception as exc:
            runtime.end_storage_span(span, error=exc)
            raise
        runtime.end_storage_span(span, telemetry=payload)
        self._attach_partition_telemetry(payload, partitioner)
        return self._storage_job(payload, telemetry)

    def _execute_storage_sql(self, sql: str, parameters: "list[Any]") -> int:
        handler = self.handle_database_exceptions()
        result = None
        with self.with_cursor(self.connection) as cursor, handler:
            cursor.execute(sql, parameters)
            result = cursor.fetchone()
        self._check_pending_exception(handler)
        if result is None:
            msg = "DuckDB storage transfer did not report its row count."
            raise SQLSpecError(msg)
        return int(result[0])

    def _execute_bulk_insert_many(
        self, expression: exp.Insert, prepared_parameters: Any, compiled_sql: str = ""
    ) -> "DMLResult | None":
        """Execute a batch INSERT via Arrow registration when the payload is simple."""
        if not isinstance(prepared_parameters, list) or not prepared_parameters:
            return None
        if not isinstance(expression.this, exp.Schema):
            return None

        if not _is_plain_values_insert(expression, len(expression.this.expressions)):
            return None
        if not isinstance(prepared_parameters[0], (list, tuple)):
            return None

        table_expr = expression.this.this
        if not isinstance(table_expr, exp.Table):
            return None

        if table_expr.alias:
            return None

        rows = prepared_parameters
        column_names = [column.name for column in expression.this.expressions]
        arrow_table = self._build_arrow_table(rows, column_names)
        if arrow_table is None:
            return None

        cache_key = compiled_sql or (str(expression._meta.get("compiled_sql", "")) if expression._meta else "")
        cached_sql_pair = self._cache_holder.get_bulk_insert_sql(cache_key) if cache_key else None
        if cached_sql_pair is not None:
            target_table, column_sql = cached_sql_pair
        else:
            target_table = table_expr.sql(dialect="duckdb")
            column_sql = ", ".join(column.sql(dialect="duckdb") for column in expression.this.expressions)
            if cache_key:
                self._cache_holder.set_bulk_insert_sql(cache_key, (target_table, column_sql))
        temp_view = f"_sqlspec_batch_{uuid4().hex}"
        exc_handler = self.handle_database_exceptions()
        with exc_handler:
            self.connection.register(temp_view, arrow_table)
            try:
                self.connection.execute(f"INSERT INTO {target_table} ({column_sql}) SELECT * FROM {temp_view}")
            finally:
                with contextlib.suppress(Exception):
                    self.connection.unregister(temp_view)
        self._check_pending_exception(exc_handler)

        return DMLResult("INSERT", len(rows))

    @staticmethod
    def _build_arrow_table(rows: "list[Any]", column_names: "list[str]") -> Any | None:
        """Build a pyarrow table from batch rows when they share a stable shape."""
        if not rows or pa is None:
            return None
        first_row = rows[0]

        if isinstance(first_row, (list, tuple)):
            values = list(first_row)
            if not column_names:
                column_names = [f"col_{index}" for index in range(len(values))]
            if any(not isinstance(row, (list, tuple)) or len(row) != len(column_names) for row in rows):
                return None
            try:
                return pa.Table.from_arrays([pa.array(col) for col in zip(*rows, strict=True)], names=column_names)
            except Exception:
                return None

        return None

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        DuckDB does not expose native transaction state, so it is tracked via a
        flag toggled in begin/commit/rollback.

        Returns:
            True when a transaction is active.
        """
        return self._transaction_active

    def _acquire_stream_cursor(self) -> "tuple[Any, bool]":
        """Acquire a dedicated cursor for Arrow streaming when not in an active transaction."""
        if not self._connection_in_transaction():
            cursor_factory = getattr(self.connection, "cursor", None)
            if callable(cursor_factory):
                stream_cursor = cursor_factory()
                if stream_cursor is not None and stream_cursor is not self.connection:
                    return stream_cursor, True
        return self.connection, False

    def _open_stream_reader(self, sql: str, parameters: Any, chunk_size: int) -> "tuple[Any, list[Any] | None]":
        """Open a native Arrow reader through DuckDB's exception boundary."""
        ensure_pyarrow()
        handler = self.handle_database_exceptions()
        description: list[Any] | None = None
        reader: Any | None = None
        stream_cursor, close_cursor = self._acquire_stream_cursor()
        try:
            with handler:
                result = stream_cursor.execute(sql, normalize_execute_parameters(parameters))
                description = result.description
                reader = result.to_arrow_reader(chunk_size)
            self._check_pending_exception(handler)
            reader = _ensure_arrow_reader(reader)
            if close_cursor:
                reader = arrow_reader_with_deferred_close(reader, stream_cursor.close)
        except BaseException:
            if close_cursor:
                with contextlib.suppress(Exception):
                    stream_cursor.close()
            raise
        return reader, description


register_driver_profile("duckdb", driver_profile)


def _ensure_arrow_reader(reader: Any) -> Any:
    if reader is None:
        msg = "DuckDB did not return an Arrow record batch reader."
        raise SQLSpecError(msg)
    return reader


def _is_plain_values_insert(expression: exp.Insert, column_count: int) -> bool:
    if any(expression.args.get(key) for key in ("conflict", "alternative", "with_")):
        return False
    values = expression.expression
    if not isinstance(values, exp.Values) or len(values.expressions) != 1:
        return False
    row = values.expressions[0]
    if not isinstance(row, exp.Tuple) or not column_count or len(row.expressions) != column_count:
        return False
    for index, item in enumerate(row.expressions, 1):
        if isinstance(item, exp.Placeholder):
            if item.name not in ("", "?", str(index)):
                return False
        elif not isinstance(item, exp.Parameter) or item.name != str(index):
            return False
    return True


def _resolve_duckdb_inserted_rows(result: object) -> int:
    fetchall = getattr(result, "fetchall", None)
    if not callable(fetchall):
        return 0
    try:
        rows = fetchall()
    except Exception:
        return 0
    if not isinstance(rows, list) or not rows:
        return 0
    first_row = rows[0]
    if isinstance(first_row, (tuple, list)) and first_row and isinstance(first_row[0], int):
        return max(first_row[0], 0)
    if isinstance(first_row, int):
        return max(first_row, 0)
    return 0
