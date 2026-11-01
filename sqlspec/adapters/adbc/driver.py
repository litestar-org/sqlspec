"""ADBC driver implementation for Arrow Database Connectivity.

Provides database connectivity through ADBC with support for multiple
database dialects, parameter style conversion, and transaction management.
"""

import contextlib
import sys
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Any, Literal, cast, final

from sqlspec.adapters.adbc._typing import AdbcCursor, AdbcNativeError, AdbcSessionContext
from sqlspec.adapters.adbc.core import (
    _transform_duckdb_copy_parameters,
    apply_driver_features,
    cleanup_duckdb_profile_path,
    collect_rows,
    configure_adbc_duckdb_profiling,
    create_duckdb_profile_path,
    create_mapped_exception,
    detect_dialect,
    driver_profile,
    enforce_strict_type_coercion,
    get_statement_config,
    handle_postgres_rollback,
    is_postgres_dialect,
    normalize_column_names,
    normalize_postgres_empty_parameters,
    normalize_script_rowcount,
    prepare_batch_with_casts,
    prepare_postgres_parameters,
    prepare_postgres_uuid_bindings,
    read_duckdb_profile_json,
    resolve_column_names,
    resolve_dialect_name,
    resolve_many_rowcount,
    resolve_parameter_casts,
    resolve_rowcount,
)
from sqlspec.adapters.adbc.data_dictionary import AdbcDataDictionary
from sqlspec.core import (
    SQL,
    StatementConfig,
    build_arrow_result_from_reader,
    build_arrow_result_from_table,
    get_cache_config,
    register_driver_profile,
)
from sqlspec.core.result import DMLResult
from sqlspec.driver import BaseSyncExceptionHandler, SyncDriverAdapterBase, SyncRowStream, validate_savepoint_name
from sqlspec.exceptions import DatabaseConnectionError, SQLSpecError
from sqlspec.utils.arrow_helpers import arrow_reader_with_deferred_close, arrow_table_to_pylist
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_pyarrow, import_optional
from sqlspec.utils.serializers import to_json
from sqlspec.utils.text import normalize_identifier, quote_identifier, split_qualified_identifier

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from sqlspec.adapters.adbc._typing import AdbcConnection, AdbcRawCursor
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, SQLResult, Statement, StatementFilter
    from sqlspec.driver import CachedQuery, ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.typing import ArrowReturnFormat, StatementParameters

__all__ = ("AdbcCursor", "AdbcDriver", "AdbcExceptionHandler", "AdbcSessionContext")

logger = get_logger("sqlspec.adapters.adbc")
_DIALECTS_WITHOUT_SAVEPOINTS = frozenset({"bigquery", "duckdb", "snowflake"})

_MULTI_ROW_BIND_UNSUPPORTED = "Binding multiple rows at once is not supported"
_SCHEMA_QUALIFIED_PARTS = 2
_CATALOG_QUALIFIED_PARTS = 3


@final
class AdbcExceptionHandler(BaseSyncExceptionHandler):
    """Context manager for handling ADBC database exceptions.

    Captures underlying ADBC driver errors and exposes mapped SQLSpec exceptions
    on ``pending_exception``.
    """

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        if exc_type is None:
            return False
        if issubclass(exc_type, AdbcNativeError):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class AdbcSelectStreamSource:
    """Native ADBC chunk source backed by a ``RecordBatchReader``."""

    __slots__ = ("_chunk_size", "_cursor_manager", "_driver", "_parameters", "_reader", "_remainder", "_sql")

    def __init__(self, driver: "AdbcDriver", sql: str, parameters: Any, chunk_size: int = 1000) -> None:
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._cursor_manager: AdbcCursor | None = None
        self._reader: Any = None
        self._remainder: list[dict[str, Any]] = []

    def start(self) -> None:
        cursor_manager = self._driver.with_cursor(self._driver.connection)
        cursor: Any = None
        reader: Any = None
        try:
            cursor = cursor_manager.__enter__()
            handler = self._driver.handle_database_exceptions()
            if sys.exc_info()[1] is None:
                with handler:
                    execute_parameters = normalize_postgres_empty_parameters(
                        self._driver._dialect_name, self._parameters
                    )
                    cursor.execute(self._sql, parameters=execute_parameters)
                    reader = _fetch_record_batch(cursor)
            else:

                def _stream_op() -> Any:
                    execute_parameters = normalize_postgres_empty_parameters(
                        self._driver._dialect_name, self._parameters
                    )
                    cursor.execute(self._sql, parameters=execute_parameters)
                    return _fetch_record_batch(cursor)

                reader = self._driver._run_with_exception_handler(handler, _stream_op)
            self._driver._check_pending_exception(handler)
        except BaseException:
            if cursor is not None:
                handle_postgres_rollback(self._driver._dialect_name, cursor, logger)
            with contextlib.suppress(Exception):
                cursor_manager.__exit__(None, None, None)
            raise

        self._cursor_manager = cursor_manager
        self._reader = iter(reader)

    def fetch_chunk(self) -> "list[dict[str, Any]]":
        chunk_size = self._chunk_size
        results: list[dict[str, Any]] = []
        if self._remainder:
            if len(self._remainder) >= chunk_size:
                chunk = self._remainder[:chunk_size]
                self._remainder = self._remainder[chunk_size:]
                return chunk
            results.extend(self._remainder)
            self._remainder = []

        reader = self._reader
        if reader is None:
            return results

        while len(results) < chunk_size:
            try:
                batch = next(reader)
            except StopIteration:
                self._reader = None
                break

            if bool(self._driver.driver_features.get("enable_lowercase_column_names", False)) and hasattr(
                batch, "schema"
            ):
                batch_names = list(batch.schema.names)
                normalized_names = normalize_column_names(batch_names, self._driver.driver_features)
                if normalized_names != batch_names and hasattr(batch, "rename_columns"):
                    batch = batch.rename_columns(normalized_names)

            rows = arrow_table_to_pylist(
                batch,
                decode_arrow_extension_types=bool(
                    self._driver.driver_features.get("enable_arrow_extension_types", True)
                ),
            )
            needed = chunk_size - len(results)
            if len(rows) <= needed:
                results.extend(rows)
            else:
                results.extend(rows[:needed])
                self._remainder = rows[needed:]
                break

        return results

    def close(self, error: bool = False) -> None:
        self._reader = None
        cursor_manager = self._cursor_manager
        self._cursor_manager = None
        if cursor_manager is not None:
            with contextlib.suppress(Exception):
                cursor_manager.__exit__(None, None, None)


@final
class AdbcDriver(SyncDriverAdapterBase):
    """ADBC driver for Arrow Database Connectivity.

    Provides database connectivity through ADBC with support for multiple
    database dialects, parameter style conversion, and transaction management.
    """

    __slots__ = (
        "_column_name_cache",
        "_data_dictionary",
        "_detected_dialect",
        "_dialect_name",
        "_is_flightsql",
        "_is_postgres",
        "_json_serializer",
        "_previous_autocommit",
        "_transaction_active",
        "dialect",
    )

    def __init__(
        self,
        connection: "AdbcConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
        *,
        dialect: "str | None" = None,
    ) -> None:
        configured_dialect = resolve_dialect_name(statement_config.dialect) if statement_config is not None else None
        fallback_dialect = dialect or configured_dialect
        self._detected_dialect = detect_dialect(connection, logger, fallback_dialect=fallback_dialect)
        self._is_flightsql = self._detect_flightsql_connection(connection)

        if statement_config is None:
            base_config = get_statement_config(self._detected_dialect)
            statement_config = base_config.replace(enable_caching=get_cache_config().compiled_cache_enabled)
        elif (
            dialect is None
            and configured_dialect
            and self._detected_dialect != configured_dialect
            and not (self._detected_dialect == "postgres" and is_postgres_dialect(configured_dialect))
        ):
            base_config = get_statement_config(self._detected_dialect)
            statement_config = base_config.replace(enable_caching=statement_config.enable_caching)

        explicit_lowercase = bool(driver_features and "enable_lowercase_column_names" in driver_features)
        statement_config, normalized_features = apply_driver_features(statement_config, driver_features)
        if not explicit_lowercase and (dialect or self._detected_dialect) in {"oracle", "db2", "snowflake"}:
            normalized_features["enable_lowercase_column_names"] = True
        super().__init__(connection=connection, statement_config=statement_config, driver_features=normalized_features)
        self.dialect = statement_config.dialect
        self._dialect_name = dialect or resolve_dialect_name(self.dialect)
        self._is_postgres = is_postgres_dialect(self._dialect_name)
        self._json_serializer = cast("Callable[[Any], str]", self.driver_features.get("json_serializer", to_json))
        self._data_dictionary: AdbcDataDictionary | None = None
        self._column_name_cache: dict[int, tuple[Any, list[str]]] = {}
        self._transaction_active = False
        self._previous_autocommit: bool | None = None

    @property
    def data_dictionary(self) -> "AdbcDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = AdbcDataDictionary()
        return self._data_dictionary

    def dispatch_execute(self, cursor: "AdbcRawCursor", statement: SQL) -> "ExecutionResult":
        """Execute single SQL statement.

        Args:
            cursor: Database cursor
            statement: SQL statement to execute

        Returns:
            Execution result with data or row count
        """
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        self._ensure_statement_profiling()
        use_db2_update = False

        try:
            execute_parameters = normalize_postgres_empty_parameters(self._dialect_name, prepared_parameters)
            use_db2_update = (
                self._dialect_name == "db2"
                and not statement.returns_rows()
                and statement.operation_type != "COMMAND"
                and bool(execute_parameters)
            )
            if use_db2_update:
                self._execute_db2_non_query(cursor, sql, execute_parameters)
            else:
                cursor.execute(sql, parameters=execute_parameters)

        except Exception:
            handle_postgres_rollback(self._dialect_name, cursor, logger)
            raise

        is_select_like = (
            False if use_db2_update else (statement.returns_rows() or self._should_force_select(statement, cursor))
        )

        if is_select_like:
            arrow_table = cursor.fetch_arrow_table()
            self._capture_statement_profile(sql)
            raw_column_names = list(arrow_table.column_names)
            column_names = normalize_column_names(raw_column_names, self.driver_features)
            if column_names != raw_column_names and hasattr(arrow_table, "rename_columns"):
                arrow_table = arrow_table.rename_columns(column_names)
            data = arrow_table_to_pylist(
                arrow_table,
                decode_arrow_extension_types=bool(self.driver_features.get("enable_arrow_extension_types", True)),
            )
            return self.create_execution_result(
                cursor,
                selected_data=data,
                column_names=column_names,
                data_row_count=len(data),
                is_select_result=True,
                row_format="dict",
            )

        row_count = self._resolve_count_result_rowcount(cursor, fallback=resolve_rowcount(cursor))
        self._capture_statement_profile(sql)
        return self.create_execution_result(cursor, rowcount_override=row_count)

    def dispatch_execute_many(self, cursor: "AdbcRawCursor", statement: SQL) -> "ExecutionResult":
        """Execute SQL with multiple parameter sets.

        Args:
            cursor: Database cursor
            statement: SQL statement to execute

        Returns:
            Execution result with row counts
        """
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)

        try:
            if not prepared_parameters:
                row_count = 0
            elif isinstance(prepared_parameters, (list, tuple)) and prepared_parameters:
                parameter_count = len(prepared_parameters)
                if self._is_flightsql:
                    row_count = 0
                    for row in prepared_parameters:
                        cursor.execute(sql, parameters=row)
                        row_count += self._resolve_count_result_rowcount(cursor, fallback=1)
                else:
                    try:
                        cursor.executemany(sql, prepared_parameters)
                    except Exception as exc:
                        if _MULTI_ROW_BIND_UNSUPPORTED not in str(exc):
                            raise
                        row_count = 0
                        for row in prepared_parameters:
                            cursor.execute(sql, parameters=row)
                            row_count += self._resolve_count_result_rowcount(cursor, fallback=1)
                    else:
                        row_count = self._resolve_count_result_rowcount(
                            cursor,
                            fallback=resolve_many_rowcount(cursor, prepared_parameters, fallback_count=parameter_count),
                        )
            elif self._is_flightsql:
                cursor.execute(sql, parameters=prepared_parameters)
                row_count = self._resolve_count_result_rowcount(cursor, fallback=1)
            else:
                cursor.executemany(sql, prepared_parameters)
                row_count = self._resolve_count_result_rowcount(cursor, fallback=resolve_rowcount(cursor))

        except Exception:
            handle_postgres_rollback(self._dialect_name, cursor, logger)
            raise

        return self.create_execution_result(cursor, rowcount_override=row_count, is_many_result=True)

    def dispatch_execute_script(self, cursor: "AdbcRawCursor", statement: "SQL") -> "ExecutionResult":
        """Execute SQL script containing multiple statements.

        Args:
            cursor: Database cursor
            statement: SQL script to execute

        Returns:
            Execution result with statement counts
        """
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)

        statements = self.split_script_statements(sql, self.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_rowcount = 0
        total_statements = len(statements)
        runtime = self._observability
        has_observers = runtime is not None and runtime.has_statement_observers
        profiling_enabled = self._dialect_name == "duckdb" and bool(
            self.driver_features.get("enable_statement_profiling", False)
        )
        if profiling_enabled:
            self._ensure_statement_profiling()
        try:
            for idx, stmt in enumerate(statements):
                stmt_started = perf_counter() if has_observers else 0.0
                stmt_params: Any = ()
                if prepared_parameters and (
                    total_statements == 1 or any(marker in stmt for marker in ("?", "$", "%", "@", ":"))
                ):
                    stmt_params = normalize_postgres_empty_parameters(self._dialect_name, prepared_parameters)
                    if (
                        self._dialect_name == "db2"
                        and stmt_params
                        and self._is_db2_non_query_statement(
                            stmt, parent_statement=statement, total_statements=total_statements
                        )
                    ):
                        self._execute_db2_non_query(cursor, stmt, stmt_params)
                    else:
                        cursor.execute(stmt, parameters=stmt_params)
                else:
                    cursor.execute(stmt)
                successful_count += 1
                stmt_rowcount = self._resolve_count_result_rowcount(
                    cursor, fallback=normalize_script_rowcount(last_rowcount, cursor)
                )
                last_rowcount = stmt_rowcount
                stmt_profile = self._capture_statement_profile(stmt) if profiling_enabled else None
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
                        rows_affected=stmt_rowcount,
                        duration_s=stmt_duration,
                        started_at=stmt_started,
                        profile=stmt_profile,
                        script_ordinal=idx,
                        script_statement_count=total_statements,
                    )
        except Exception:
            handle_postgres_rollback(self._dialect_name, cursor, logger)
            raise

        self._pending_statement_profile = None
        return self.create_execution_result(
            cursor,
            statement_count=total_statements,
            successful_statements=successful_count,
            rowcount_override=last_rowcount,
            is_script_result=True,
        )

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "SyncRowStream[dict[str, Any]] | None":
        """Return a native ADBC stream backed by ``fetch_record_batch()``."""
        if not statement.returns_rows():
            return None
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        return SyncRowStream(AdbcSelectStreamSource(self, sql, prepared_parameters, chunk_size=chunk_size))

    def begin(self) -> None:
        """Begin database transaction.

        ADBC connections operate with autocommit disabled and manage the
        active transaction internally, so no explicit statement is issued.
        """
        if hasattr(self.connection, "autocommit"):
            if self._previous_autocommit is None:
                self._previous_autocommit = bool(self.connection.autocommit)
            if self.connection.autocommit:
                self.connection.autocommit = False
        self._transaction_active = True

    def commit(self) -> None:
        """Commit database transaction."""
        try:
            self.connection.commit()
        except AdbcNativeError as e:
            raise create_mapped_exception(e) from e
        finally:
            self._transaction_active = False
            if self._previous_autocommit is not None:
                if hasattr(self.connection, "autocommit"):
                    self.connection.autocommit = self._previous_autocommit
                self._previous_autocommit = None

    def rollback(self) -> None:
        """Rollback database transaction."""
        try:
            self.connection.rollback()
        except AdbcNativeError as e:
            raise create_mapped_exception(e) from e
        finally:
            self._transaction_active = False
            if self._previous_autocommit is not None:
                if hasattr(self.connection, "autocommit"):
                    self.connection.autocommit = self._previous_autocommit
                self._previous_autocommit = None

    def create_savepoint(self, name: str) -> None:
        """Create a savepoint within the current transaction.

        Raises:
            NotImplementedError: If the connected database does not support savepoints.
        """
        self._require_savepoints()
        safe_name = validate_savepoint_name(name)
        dialect_name = resolve_dialect_name(self.dialect)
        if dialect_name == "db2":
            self.execute_script(f"SAVEPOINT {safe_name} ON ROLLBACK RETAIN CURSORS")
            return
        if dialect_name in {"mssql", "tsql"}:
            self.execute_script(f"SAVE TRANSACTION {safe_name}")
            return
        super().create_savepoint(safe_name)

    def release_savepoint(self, name: str) -> None:
        """Release a previously created savepoint.

        Raises:
            NotImplementedError: If the connected database does not support savepoints.
        """
        self._require_savepoints()
        safe_name = validate_savepoint_name(name)
        if resolve_dialect_name(self.dialect) in {"oracle", "mssql", "tsql"}:
            return
        super().release_savepoint(safe_name)

    def rollback_to_savepoint(self, name: str) -> None:
        """Roll back the current transaction to a previously created savepoint.

        Raises:
            NotImplementedError: If the connected database does not support savepoints.
        """
        self._require_savepoints()
        safe_name = validate_savepoint_name(name)
        if resolve_dialect_name(self.dialect) in {"mssql", "tsql"}:
            self.execute_script(f"ROLLBACK TRANSACTION {safe_name}")
            return
        super().rollback_to_savepoint(safe_name)

    def set_migration_session_schema(self, schema: str) -> None:
        """Set the PostgreSQL search path for migration SQL when using ADBC PostgreSQL."""
        if not self._is_postgres:
            super().set_migration_session_schema(schema)
            return
        normalized_schema = normalize_identifier(schema, "postgres")
        quoted_schema = quote_identifier(normalized_schema)
        with self.with_cursor(self.connection) as cursor:
            cursor.execute(f'SET search_path TO {quoted_schema}, "$user", public')

    def set_migration_non_transactional_schema(self, schema: str) -> None:
        """Set the PostgreSQL search path for non-transactional migration SQL."""
        self.set_migration_session_schema(schema)

    def reset_migration_session_schema(self) -> None:
        """Reset PostgreSQL search path after non-transactional migration SQL."""
        if not self._is_postgres:
            super().reset_migration_session_schema()
            return
        with self.with_cursor(self.connection) as cursor:
            cursor.execute("RESET search_path")

    def has_schema(self, schema: str) -> bool:
        """Return whether a PostgreSQL schema exists when using ADBC PostgreSQL."""
        if not self._is_postgres:
            return super().has_schema(schema)
        normalized_schema = normalize_identifier(schema, "postgres")
        with self.with_cursor(self.connection) as cursor:
            cursor.execute(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = $1", parameters=[normalized_schema]
            )
            return cursor.fetchone() is not None

    def with_cursor(self, connection: "AdbcConnection") -> "AdbcCursor":
        """Create context manager for cursor.

        Args:
            connection: Database connection

        Returns:
            Cursor context manager
        """
        return AdbcCursor(connection)

    def handle_database_exceptions(self) -> "AdbcExceptionHandler":
        """Handle database-specific exceptions and wrap them appropriately.

        Returns:
            Exception handler context manager
        """
        return AdbcExceptionHandler()

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
        """Execute query and return results as Apache Arrow (ADBC native path).

        ADBC provides zero-copy Arrow support via cursor.fetch_arrow_table().
        This is 5-10x faster than the conversion path for large datasets.

        Args:
            statement: SQL statement, string, or QueryBuilder
            *parameters: Query parameters or filters
            statement_config: Optional statement configuration override
            return_format: "table" for pyarrow.Table (default), "batch" for RecordBatch,
                "batches" for list of RecordBatch, "reader" for RecordBatchReader
            native_only: Ignored for ADBC (always uses native path)
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
            cursor_manager = self.with_cursor(self.connection)
            cursor = None
            reader: Any = None
            with exc_handler:
                cursor = cursor_manager.__enter__()
                try:
                    sql, driver_params = self._compiled_sql(prepared_statement, config)
                    execute_parameters = normalize_postgres_empty_parameters(self._dialect_name, driver_params)
                    cursor.execute(sql, parameters=execute_parameters)
                    fetch_record_batch = getattr(cursor, "fetch_record_batch", None)
                    if fetch_record_batch is None:
                        arrow_table = cursor.fetch_arrow_table()
                        cursor_manager.__exit__(None, None, None)
                        cursor = None
                        arrow_result = build_arrow_result_from_table(
                            prepared_statement,
                            arrow_table,
                            return_format=return_format,
                            batch_size=batch_size,
                            arrow_schema=arrow_schema,
                        )
                    else:
                        reader = fetch_record_batch()
                except Exception:
                    handle_postgres_rollback(self._dialect_name, cursor, logger)
                    cursor_manager.__exit__(None, None, None)
                    cursor = None
                    raise

            if exc_handler.pending_exception is not None:
                if cursor is not None:
                    with contextlib.suppress(Exception):
                        cursor_manager.__exit__(None, None, None)
                self._check_pending_exception(exc_handler)
            if arrow_result is not None:
                return arrow_result
            if cursor is None or reader is None:
                if cursor is not None:
                    with contextlib.suppress(Exception):
                        cursor_manager.__exit__(None, None, None)
                msg = "ADBC did not return an Arrow reader."
                raise SQLSpecError(msg)
            if bool(self.driver_features.get("enable_lowercase_column_names", False)) and hasattr(reader, "schema"):
                raw_names = list(reader.schema.names)
                normalized_names = normalize_column_names(raw_names, self.driver_features)
                pa = import_optional("pyarrow")
                if normalized_names != raw_names and pa is not None:
                    new_schema = pa.schema([
                        field.with_name(name) for field, name in zip(reader.schema, normalized_names, strict=True)
                    ])
                    source_reader = reader

                    def _lower_batches() -> Any:
                        for batch in source_reader:
                            yield batch.rename_columns(normalized_names)

                    reader = pa.RecordBatchReader.from_batches(new_schema, _lower_batches())
            try:
                return build_arrow_result_from_reader(
                    prepared_statement,
                    arrow_reader_with_deferred_close(reader, cursor.close),
                    return_format=return_format,
                    batch_size=batch_size,
                    arrow_schema=arrow_schema,
                )
            except BaseException:
                with contextlib.suppress(Exception):
                    cursor_manager.__exit__(None, None, None)
                raise

        with self.with_cursor(self.connection) as cursor, exc_handler:
            if cursor is None:
                msg = "Failed to create cursor"
                raise DatabaseConnectionError(msg)

            sql, driver_params = self._compiled_sql(prepared_statement, config)
            execute_parameters = normalize_postgres_empty_parameters(self._dialect_name, driver_params)
            try:
                cursor.execute(sql, parameters=execute_parameters)
                arrow_table = cursor.fetch_arrow_table()
            except Exception:
                handle_postgres_rollback(self._dialect_name, cursor, logger)
                raise

            if bool(self.driver_features.get("enable_lowercase_column_names", False)) and hasattr(
                arrow_table, "column_names"
            ):
                raw_column_names = list(arrow_table.column_names)
                normalized_column_names = normalize_column_names(raw_column_names, self.driver_features)
                if normalized_column_names != raw_column_names and hasattr(arrow_table, "rename_columns"):
                    arrow_table = arrow_table.rename_columns(normalized_column_names)

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
        """Stream query results to storage via the Arrow fast path."""

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
        stage_table: "bool | str | None" = None,
        conflict_keys: "Sequence[str] | None" = None,
        on_conflict: Literal["update", "nothing"] = "update",
        exclude_update_columns: "Sequence[str] | None" = None,
        drop_stage_table: bool = True,
        drop_indexes: bool = False,
        concurrent_indexes: bool = False,
        type_casts: "Mapping[str, str] | None" = None,
    ) -> "StorageBridgeJob":
        """Ingest an Arrow payload directly through the ADBC cursor."""

        self._require_capability("arrow_import_enabled")
        if stage_table or conflict_keys or drop_indexes:
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
                drop_indexes=drop_indexes,
                concurrent_indexes=concurrent_indexes,
                type_casts=type_casts,
            )
        ensure_pyarrow()

        source_data: Any = source.get_data() if hasattr(source, "get_data") else source
        ingest_mode: Literal["append", "create", "replace", "create_append"]
        ingest_mode = "replace" if overwrite else "create_append"
        target_table, ingest_kwargs = _parse_adbc_ingest_target(table, mode=ingest_mode)

        if hasattr(source_data, "read_next_batch"):
            row_count = 0
            exc_handler = self.handle_database_exceptions()
            with self.with_cursor(self.connection) as cursor, exc_handler:
                try:
                    ingest_result = cursor.adbc_ingest(target_table, source_data, **ingest_kwargs)
                except Exception:
                    handle_postgres_rollback(self._dialect_name, cursor, logger)
                    raise
                if isinstance(ingest_result, int) and ingest_result > 0:
                    row_count = ingest_result

            self._check_pending_exception(exc_handler)
            telemetry_payload: StorageTelemetry = {
                "rows_processed": row_count,
                "bytes_processed": 0,
                "format": "arrow",
                "destination": table,
            }
            self._attach_partition_telemetry(telemetry_payload, partitioner)
            return self._storage_job(telemetry_payload, telemetry)

        arrow_table = self._coerce_arrow_table(source_data)
        exc_handler = self.handle_database_exceptions()
        with self.with_cursor(self.connection) as cursor, exc_handler:
            try:
                cursor.adbc_ingest(target_table, arrow_table, **ingest_kwargs)
            except Exception:
                handle_postgres_rollback(self._dialect_name, cursor, logger)
                raise

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
        **kwargs: Any,
    ) -> "StorageBridgeJob":
        """Read an artifact from storage and ingest it via ADBC."""

        return super().load_from_storage(
            table, source, file_format=file_format, partitioner=partitioner, overwrite=overwrite, **kwargs
        )

    def collect_rows(self, cursor: "AdbcRawCursor", fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        """Collect ADBC rows for the direct execution path."""
        column_names = self._resolve_column_names(cursor.description)
        data, column_names = collect_rows(
            cast("list[Any] | None", fetched),
            cursor.description,
            column_names=column_names,
            driver_features=self.driver_features,
        )
        return data, column_names, len(data)

    def resolve_rowcount(self, cursor: "AdbcRawCursor") -> int:
        """Resolve rowcount from ADBC cursor for the direct execution path."""
        return resolve_rowcount(cursor)

    def prepare_driver_parameters(
        self,
        parameters: Any,
        statement_config: "StatementConfig",
        is_many: bool = False,
        prepared_statement: Any | None = None,
    ) -> Any:
        """Prepare parameters with cast-aware type coercion for ADBC.

        For PostgreSQL, applies cast-aware parameter processing using metadata from the compiled statement.
        This allows proper handling of JSONB casts and other type conversions.
        Respects driver_features['enable_cast_detection'] configuration.

        Args:
            parameters: Parameters in any format
            statement_config: Statement configuration
            is_many: Whether this is for execute_many operation
            prepared_statement: Prepared statement containing the original SQL statement

        Returns:
            Parameters with cast-aware type coercion applied
        """
        if self.driver_features.get("enable_strict_type_coercion", False):
            enforce_strict_type_coercion(parameters, is_many=is_many)

        enable_cast_detection = self.driver_features.get("enable_cast_detection", True)
        if enable_cast_detection and prepared_statement and self._is_postgres:
            prepared_parameters = super().prepare_driver_parameters(
                parameters, statement_config, is_many, prepared_statement
            )
            parameter_casts = resolve_parameter_casts(prepared_statement)
            if is_many:
                if not parameter_casts or not isinstance(prepared_parameters, (list, tuple)):
                    return prepared_parameters
                return prepare_batch_with_casts(
                    prepared_parameters,
                    parameter_casts,
                    statement_config,
                    dialect=self._dialect_name,
                    json_serializer=self._json_serializer,
                )
            return prepare_postgres_parameters(
                prepared_parameters,
                parameter_casts,
                statement_config,
                dialect=self._dialect_name,
                json_serializer=self._json_serializer,
            )

        return super().prepare_driver_parameters(parameters, statement_config, is_many, prepared_statement)

    def _compiled_sql(
        self, statement: "SQL", statement_config: "StatementConfig", flatten_single_parameters: bool = False
    ) -> "tuple[str, object]":
        if (
            self._dialect_name == "duckdb"
            and statement_config.parameter_config.ast_transformer is None
            and statement.raw_sql.lstrip().upper().startswith(("COPY", "INSERT"))
        ):
            param_config = statement_config.parameter_config.replace(ast_transformer=_transform_duckdb_copy_parameters)
            statement_config = statement_config.replace(parameter_config=param_config)
        compiled_sql, prepared_parameters = super()._compiled_sql(
            statement, statement_config, flatten_single_parameters=flatten_single_parameters
        )
        return prepare_postgres_uuid_bindings(
            compiled_sql, prepared_parameters, is_many=statement.is_many, dialect=self._dialect_name
        )

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        ADBC does not expose reliable native transaction state, so it is tracked
        via a flag toggled in begin/commit/rollback.

        Returns:
            True when a transaction is active.
        """
        return self._transaction_active

    def _require_savepoints(self) -> None:
        if self._dialect_name in _DIALECTS_WITHOUT_SAVEPOINTS:
            msg = f"ADBC {self._dialect_name} connections do not support savepoints."
            raise NotImplementedError(msg)

    @staticmethod
    def _detect_flightsql_connection(connection: "AdbcConnection") -> bool:
        try:
            driver_info = connection.adbc_get_info()
        except Exception:
            return False
        driver_name = str(driver_info.get("driver_name", "")).lower()
        return "flight sql" in driver_name or "flightsql" in driver_name

    def _resolve_column_names(self, description: Any) -> list[str]:
        return resolve_column_names(description, self._column_name_cache, self.driver_features)

    def _ensure_statement_profiling(self) -> "Path | None":
        if self._dialect_name != "duckdb" or not bool(self.driver_features.get("enable_statement_profiling", False)):
            return None
        raw_path = self.driver_features.get("_duckdb_profile_path")
        if isinstance(raw_path, Path):
            return raw_path
        if isinstance(raw_path, str) and raw_path:
            return Path(raw_path)
        profile_path = create_duckdb_profile_path()
        try:
            configure_adbc_duckdb_profiling(self.connection, profile_path)
        except Exception:
            cleanup_duckdb_profile_path(profile_path)
            return None
        self.driver_features["_duckdb_profile_path"] = str(profile_path)
        return profile_path

    def _capture_statement_profile(self, executed_sql: str) -> "dict[str, Any] | None":
        if self._dialect_name != "duckdb" or not bool(self.driver_features.get("enable_statement_profiling", False)):
            return None
        profile_path = self._ensure_statement_profiling()
        if profile_path is None:
            return None
        profile = read_duckdb_profile_json(profile_path, executed_sql)
        self._pending_statement_profile = profile
        return profile

    @staticmethod
    def _resolve_count_result_rowcount(cursor: "AdbcRawCursor", *, fallback: int) -> int:
        """Consume ADBC count result streams and return their row count."""
        if not cursor.description:
            return fallback
        try:
            rows = cursor.fetchall()
        except Exception:
            return fallback
        if not rows:
            return fallback

        first_row = rows[0]
        if isinstance(first_row, dict):
            first_value = next(iter(first_row.values()), None)
        elif isinstance(first_row, (tuple, list)) and first_row:
            first_value = first_row[0]
        else:
            first_value = first_row

        if isinstance(first_value, int):
            return first_value
        return fallback

    def _execute_cache_hit_with_cursor(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: "CachedQuery"
    ) -> "SQLResult":
        """Execute a cached query, routing parameterized Db2 non-queries through execute_update."""
        if self._dialect_name == "db2" and not cached.operation_profile.returns_rows and params:
            with self.with_cursor(self.connection) as cursor:
                wire_sql = self._format_wire_sql(cached.compiled_sql)
                self._execute_db2_non_query(cursor, wire_sql, params)
                affected_rows = self._resolve_count_result_rowcount(cursor, fallback=resolve_rowcount(cursor))
                return DMLResult(cached.operation_type, affected_rows)
        return super()._execute_cache_hit_with_cursor(sql, params, cached)

    def _is_db2_non_query_statement(self, sql: str, *, parent_statement: "SQL", total_statements: int) -> bool:
        """Return whether a script statement is a non-row-returning Db2 operation."""
        if total_statements == 1:
            return not parent_statement.returns_rows() and parent_statement.operation_type != "COMMAND"
        statement = SQL(sql, statement_config=self.statement_config)
        return not statement.returns_rows() and statement.operation_type != "COMMAND"

    @staticmethod
    def _execute_db2_non_query(cursor: "AdbcRawCursor", sql: str, execute_parameters: Any) -> None:
        """Execute a parameterized Db2 non-query statement via ADBC execute_update."""
        if hasattr(execute_parameters, "__arrow_c_array__") or hasattr(execute_parameters, "__arrow_c_stream__"):
            cursor.executemany(sql, execute_parameters)
            return
        cursor.executemany(sql, [execute_parameters])


def _parse_adbc_ingest_target(
    table: str, *, mode: Literal["append", "create", "replace", "create_append"]
) -> "tuple[str, dict[str, Any]]":
    kwargs: dict[str, Any] = {"mode": mode}
    if "." not in table:
        return table, kwargs
    parts = split_qualified_identifier(table, quote_chars='"')
    if len(parts) == _SCHEMA_QUALIFIED_PARTS:
        kwargs["db_schema_name"] = parts[0]
        return parts[1], kwargs
    if len(parts) >= _CATALOG_QUALIFIED_PARTS:
        kwargs["catalog_name"] = parts[0]
        kwargs["db_schema_name"] = parts[1]
        return parts[-1], kwargs
    return table, kwargs


def _fetch_record_batch(cursor: Any) -> Any:
    fetch_record_batch = getattr(cursor, "fetch_record_batch", None)
    if fetch_record_batch is None:
        msg = "ADBC cursor does not expose fetch_record_batch() for native row streaming."
        raise SQLSpecError(msg)
    reader = fetch_record_batch()
    if reader is None:
        msg = "ADBC did not return a record batch reader."
        raise SQLSpecError(msg)
    return reader


register_driver_profile("adbc", driver_profile)
