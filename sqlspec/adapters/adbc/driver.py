"""ADBC driver implementation for Arrow Database Connectivity.

Provides database connectivity through ADBC with support for multiple
database dialects, parameter style conversion, and transaction management.
"""

from typing import TYPE_CHECKING, Any, Literal, cast

from typing_extensions import final

from sqlspec.adapters.adbc._typing import AdbcCursor, AdbcSessionContext
from sqlspec.adapters.adbc.core import (
    _prepare_batch_with_casts,
    collect_rows,
    create_mapped_exception,
    detect_dialect,
    driver_profile,
    get_statement_config,
    handle_postgres_rollback,
    is_postgres_dialect,
    normalize_postgres_empty_parameters,
    normalize_script_rowcount,
    prepare_postgres_parameters,
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
from sqlspec.driver import BaseSyncExceptionHandler, SyncDriverAdapterBase
from sqlspec.exceptions import DatabaseConnectionError, SQLSpecError
from sqlspec.utils.arrow_helpers import arrow_reader_with_deferred_close
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.serializers import to_json
from sqlspec.utils.text import normalize_identifier, quote_identifier

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.adapters.adbc._typing import AdbcConnection, AdbcRawCursor
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, Statement, StatementFilter
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.typing import ArrowReturnFormat, StatementParameters

__all__ = ("AdbcCursor", "AdbcDriver", "AdbcExceptionHandler", "AdbcSessionContext")

logger = get_logger("sqlspec.adapters.adbc")

_MULTI_ROW_BIND_UNSUPPORTED = "Binding multiple rows at once is not supported"


@final
class AdbcExceptionHandler(BaseSyncExceptionHandler):
    """Context manager for handling ADBC database exceptions.

    ADBC propagates underlying database errors. Exception mapping
    depends on the specific ADBC driver being used.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __exit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        if exc_type is None:
            return False
        self.pending_exception = create_mapped_exception(exc_val)
        return True


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
        self._detected_dialect = detect_dialect(connection, logger, fallback_dialect=dialect)
        self._is_flightsql = self._detect_flightsql_connection(connection)

        if statement_config is None:
            base_config = get_statement_config(self._detected_dialect)
            statement_config = base_config.replace(enable_caching=get_cache_config().compiled_cache_enabled)

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self.dialect = statement_config.dialect
        self._dialect_name = resolve_dialect_name(self.dialect)
        self._is_postgres = is_postgres_dialect(self._dialect_name)
        self._json_serializer = cast("Callable[[Any], str]", self.driver_features.get("json_serializer", to_json))
        self._data_dictionary: AdbcDataDictionary | None = None
        self._column_name_cache: dict[int, tuple[Any, list[str]]] = {}

    # ─────────────────────────────────────────────────────────────────────────────
    # CORE DISPATCH METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def dispatch_execute(self, cursor: "AdbcRawCursor", statement: SQL) -> "ExecutionResult":
        """Execute single SQL statement.

        Args:
            cursor: Database cursor
            statement: SQL statement to execute

        Returns:
            Execution result with data or row count
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        try:
            execute_parameters = normalize_postgres_empty_parameters(self._dialect_name, prepared_parameters)
            cursor.execute(sql, parameters=execute_parameters)

        except Exception:
            handle_postgres_rollback(self._dialect_name, cursor, logger)
            raise

        is_select_like = statement.returns_rows() or self._should_force_select(statement, cursor)

        if is_select_like:
            fetched_data = cursor.fetchall()
            column_names = self._resolve_column_names(cursor.description)
            data, column_names = collect_rows(
                cast("list[Any] | None", fetched_data), cursor.description, column_names=column_names
            )
            row_format = "dict" if data and isinstance(data[0], dict) else "tuple"
            return self.create_execution_result(
                cursor,
                selected_data=data,
                column_names=column_names,
                data_row_count=len(data),
                is_select_result=True,
                row_format=row_format,
            )

        row_count = self._resolve_count_result_rowcount(cursor, fallback=resolve_rowcount(cursor))
        return self.create_execution_result(cursor, rowcount_override=row_count)

    def dispatch_execute_many(self, cursor: "AdbcRawCursor", statement: SQL) -> "ExecutionResult":
        """Execute SQL with multiple parameter sets.

        Args:
            cursor: Database cursor
            statement: SQL statement to execute

        Returns:
            Execution result with row counts
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        try:
            if not prepared_parameters:
                cursor._rowcount = 0  # pyright: ignore[reportPrivateUsage]
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
        prepared_parameters: Any | None = None
        if statement.is_script:
            sql = statement.raw_sql
        else:
            sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        statements = self.split_script_statements(sql, self.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_rowcount = 0
        try:
            for stmt in statements:
                if prepared_parameters:
                    postgres_compatible_params = normalize_postgres_empty_parameters(
                        self._dialect_name, prepared_parameters
                    )
                    cursor.execute(stmt, parameters=postgres_compatible_params)
                else:
                    cursor.execute(stmt)
                successful_count += 1
                last_rowcount = self._resolve_count_result_rowcount(
                    cursor, fallback=normalize_script_rowcount(last_rowcount, cursor)
                )
        except Exception:
            handle_postgres_rollback(self._dialect_name, cursor, logger)
            raise

        return self.create_execution_result(
            cursor,
            statement_count=len(statements),
            successful_statements=successful_count,
            rowcount_override=last_rowcount,
            is_script_result=True,
        )

    # ─────────────────────────────────────────────────────────────────────────────
    # TRANSACTION MANAGEMENT
    # ─────────────────────────────────────────────────────────────────────────────

    def begin(self) -> None:
        """Begin database transaction.

        ADBC connections operate with autocommit disabled and manage the
        active transaction internally, so no explicit statement is issued.
        """

    def commit(self) -> None:
        """Commit database transaction."""
        try:
            self.connection.commit()
        except Exception as e:
            msg = f"Failed to commit transaction: {e}"
            raise SQLSpecError(msg) from e

    def rollback(self) -> None:
        """Rollback database transaction."""
        try:
            self.connection.rollback()
        except Exception as e:
            msg = f"Failed to rollback transaction: {e}"
            raise SQLSpecError(msg) from e

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

        # Prepare statement
        config = statement_config or self.statement_config
        prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)

        exc_handler = self.handle_database_exceptions()
        arrow_result: ArrowResult | None = None

        if return_format in {"reader", "batches"}:
            cursor_manager = self.with_cursor(self.connection)
            cursor = None
            reader: object | None = None
            with exc_handler:
                cursor = cursor_manager.__enter__()
                try:
                    sql, driver_params = self._get_compiled_sql(prepared_statement, config)
                    cursor.execute(sql, driver_params or ())
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
                    cursor_manager.__exit__(None, None, None)
                    cursor = None
                    raise

            if exc_handler.pending_exception is not None:
                raise exc_handler.pending_exception from None
            if arrow_result is not None:
                return arrow_result
            if cursor is None or reader is None:
                msg = "ADBC did not return an Arrow reader."
                raise SQLSpecError(msg)
            return build_arrow_result_from_reader(
                prepared_statement,
                arrow_reader_with_deferred_close(reader, cursor.close),
                return_format=return_format,
                batch_size=batch_size,
                arrow_schema=arrow_schema,
            )

        # Use ADBC cursor for native Arrow
        with self.with_cursor(self.connection) as cursor, exc_handler:
            if cursor is None:
                msg = "Failed to create cursor"
                raise DatabaseConnectionError(msg)

            # Get compiled SQL and parameters
            sql, driver_params = self._get_compiled_sql(prepared_statement, config)

            # Execute query
            cursor.execute(sql, driver_params or ())

            # Fetch as Arrow table (zero-copy!)
            arrow_table = cursor.fetch_arrow_table()

            arrow_result = build_arrow_result_from_table(
                prepared_statement,
                arrow_table,
                return_format=return_format,
                batch_size=batch_size,
                arrow_schema=arrow_schema,
            )

        if exc_handler.pending_exception is not None:
            raise exc_handler.pending_exception from None

        if arrow_result is None:
            msg = "Unreachable"
            raise RuntimeError(msg)  # pragma: no cover

        return arrow_result

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
        """Stream query results to storage via the Arrow fast path."""

        _ = kwargs
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
        """Ingest an Arrow payload directly through the ADBC cursor."""

        self._require_capability("arrow_import_enabled")
        ensure_pyarrow()
        import pyarrow as pa

        source_data = source.get_data() if hasattr(source, "get_data") else source
        ingest_mode: Literal["append", "create", "replace", "create_append"]
        ingest_mode = "replace" if overwrite else "create_append"
        if isinstance(source_data, pa.RecordBatchReader):
            row_count = 0
            exc_handler = self.handle_database_exceptions()
            with self.with_cursor(self.connection) as cursor, exc_handler:
                ingest_result = cursor.adbc_ingest(table, source_data, mode=ingest_mode)
                if isinstance(ingest_result, int) and ingest_result > 0:
                    row_count = ingest_result

            if exc_handler.pending_exception is not None:
                raise exc_handler.pending_exception from None
            telemetry_payload: StorageTelemetry = {
                "rows_processed": row_count,
                "bytes_processed": 0,
                "format": "arrow",
                "destination": table,
            }
            self._attach_partition_telemetry(telemetry_payload, partitioner)
            return self._create_storage_job(telemetry_payload, telemetry)

        arrow_table = self._coerce_arrow_table(source_data)
        exc_handler = self.handle_database_exceptions()
        with self.with_cursor(self.connection) as cursor, exc_handler:
            cursor.adbc_ingest(table, arrow_table, mode=ingest_mode)

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
        """Read an artifact from storage and ingest it via ADBC."""

        arrow_table, inbound = self._read_arrow_from_storage_sync(source, file_format=file_format)
        return self.load_from_arrow(table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound)

    # ─────────────────────────────────────────────────────────────────────────────
    # UTILITY METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    @property
    def data_dictionary(self) -> "AdbcDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = AdbcDataDictionary()
        return self._data_dictionary

    # ─────────────────────────────────────────────────────────────────────────────
    # PRIVATE/INTERNAL METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def collect_rows(self, cursor: "AdbcRawCursor", fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        """Collect ADBC rows for the direct execution path."""
        column_names = self._resolve_column_names(cursor.description)
        data, column_names = collect_rows(
            cast("list[Any] | None", fetched), cursor.description, column_names=column_names
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
        enable_cast_detection = self.driver_features.get("enable_cast_detection", True)
        if enable_cast_detection and prepared_statement and self._is_postgres:
            prepared_parameters = super().prepare_driver_parameters(
                parameters, statement_config, is_many, prepared_statement
            )
            parameter_casts = resolve_parameter_casts(prepared_statement)
            if is_many:
                if not parameter_casts or not isinstance(prepared_parameters, (list, tuple)):
                    return prepared_parameters
                return _prepare_batch_with_casts(
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

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        ADBC uses explicit BEGIN and does not expose reliable transaction state.

        Returns:
            False - ADBC requires explicit transaction management.
        """
        return False

    @staticmethod
    def _detect_flightsql_connection(connection: "AdbcConnection") -> bool:
        try:
            driver_info = connection.adbc_get_info()
        except Exception:
            return False
        driver_name = str(driver_info.get("driver_name", "")).lower()
        return "flight sql" in driver_name or "flightsql" in driver_name

    def _resolve_column_names(self, description: Any) -> list[str]:
        return resolve_column_names(description, self._column_name_cache)

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


register_driver_profile("adbc", driver_profile)
