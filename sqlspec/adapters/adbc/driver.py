"""ADBC driver implementation for Arrow Database Connectivity.

Provides database connectivity through ADBC with support for multiple
database dialects, parameter style conversion, and transaction management.
"""

import contextlib
from typing import TYPE_CHECKING, Any, Literal, cast

from sqlspec.adapters.adbc._typing import AdbcSessionContext
from sqlspec.adapters.adbc.core import (
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
    resolve_dialect_name,
    resolve_parameter_casts,
    resolve_rowcount,
)
from sqlspec.adapters.adbc.data_dictionary import AdbcDataDictionary
from sqlspec.core import SQL, StatementConfig, build_arrow_result_from_table, get_cache_config, register_driver_profile
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.exceptions import DatabaseConnectionError, SQLSpecError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from collections.abc import Callable

    from adbc_driver_manager.dbapi import Cursor

    from sqlspec.adapters.adbc._typing import AdbcConnection
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, Statement, StatementFilter
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.typing import ArrowReturnFormat, StatementParameters

__all__ = ("AdbcCursor", "AdbcDriver", "AdbcExceptionHandler", "AdbcSessionContext")

logger = get_logger("sqlspec.adapters.adbc")


class AdbcCursor:
    """Context manager for cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "AdbcConnection") -> None:
        self.connection = connection
        self.cursor: Cursor | None = None

    def __enter__(self) -> "Cursor":
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                self.cursor.close()  # type: ignore[no-untyped-call]


class AdbcExceptionHandler:
    """Context manager for handling ADBC database exceptions.

    ADBC propagates underlying database errors. Exception mapping
    depends on the specific ADBC driver being used.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __exit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    def __enter__(self) -> "AdbcExceptionHandler":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        _ = exc_tb
        if exc_type is None:
            return False
        self.pending_exception = create_mapped_exception(exc_val)
        return True


class AdbcDriver(SyncDriverAdapterBase):
    """ADBC driver for Arrow Database Connectivity.

    Provides database connectivity through ADBC with support for multiple
    database dialects, parameter style conversion, and transaction management.
    """

    __slots__ = (
        "_data_dictionary",
        "_detected_dialect",
        "_dialect_name",
        "_is_postgres",
        "_json_serializer",
        "dialect",
    )

    def __init__(
        self,
        connection: "AdbcConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        self._detected_dialect = detect_dialect(connection, logger)

        if statement_config is None:
            base_config = get_statement_config(self._detected_dialect)
            statement_config = base_config.replace(enable_caching=get_cache_config().compiled_cache_enabled)

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self.dialect = statement_config.dialect
        self._dialect_name = resolve_dialect_name(self.dialect)
        self._is_postgres = is_postgres_dialect(self._dialect_name)
        self._json_serializer = cast("Callable[[Any], str]", self.driver_features.get("json_serializer", to_json))
        self._data_dictionary: AdbcDataDictionary | None = None

    # ─────────────────────────────────────────────────────────────────────────────
    # CORE DISPATCH METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def dispatch_execute(self, cursor: "Cursor", statement: SQL) -> "ExecutionResult":
        """Execute single SQL statement.

        Args:
            cursor: Database cursor
            statement: SQL statement to execute

        Returns:
            Execution result with data or row count
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        parameter_casts = resolve_parameter_casts(statement)

        try:
            if self._is_postgres:
                formatted_params = prepare_postgres_parameters(
                    prepared_parameters,
                    parameter_casts,
                    self.statement_config,
                    dialect=self._dialect_name,
                    json_serializer=self._json_serializer,
                )
                cursor.execute(sql, parameters=formatted_params)
            else:
                postgres_compatible_params = normalize_postgres_empty_parameters(
                    self._dialect_name, prepared_parameters
                )
                cursor.execute(sql, parameters=postgres_compatible_params)

        except Exception:
            handle_postgres_rollback(self._dialect_name, cursor, logger)
            raise

        is_select_like = statement.returns_rows() or self._should_force_select(statement, cursor)

        if is_select_like:
            fetched_data = cursor.fetchall()
            dict_data, column_names = collect_rows(cast("list[Any] | None", fetched_data), cursor.description)
            return self.create_execution_result(
                cursor,
                selected_data=dict_data,
                column_names=column_names,
                data_row_count=len(dict_data),
                is_select_result=True,
            )

        row_count = resolve_rowcount(cursor)
        return self.create_execution_result(cursor, rowcount_override=row_count)

    def dispatch_execute_many(self, cursor: "Cursor", statement: SQL) -> "ExecutionResult":
        """Execute SQL with multiple parameter sets.

        Args:
            cursor: Database cursor
            statement: SQL statement to execute

        Returns:
            Execution result with row counts
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        parameter_casts = resolve_parameter_casts(statement)

        try:
            if not prepared_parameters:
                cursor._rowcount = 0  # pyright: ignore[reportPrivateUsage]
                row_count = 0
            elif isinstance(prepared_parameters, (list, tuple)) and prepared_parameters:
                processed_params = []
                for param_set in prepared_parameters:
                    if self._is_postgres:
                        # For postgres, always use cast-aware parameter preparation
                        formatted_params = prepare_postgres_parameters(
                            param_set,
                            parameter_casts,
                            self.statement_config,
                            dialect=self._dialect_name,
                            json_serializer=self._json_serializer,
                        )
                    else:
                        postgres_compatible = normalize_postgres_empty_parameters(self._dialect_name, param_set)
                        formatted_params = self.prepare_driver_parameters(
                            postgres_compatible, self.statement_config, is_many=False
                        )
                    processed_params.append(formatted_params)

                cursor.executemany(sql, processed_params)
                row_count = resolve_rowcount(cursor)
            else:
                cursor.executemany(sql, prepared_parameters)
                row_count = resolve_rowcount(cursor)

        except Exception:
            handle_postgres_rollback(self._dialect_name, cursor, logger)
            raise

        return self.create_execution_result(cursor, rowcount_override=row_count, is_many_result=True)

    def dispatch_execute_script(self, cursor: "Cursor", statement: "SQL") -> "ExecutionResult":
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
                last_rowcount = normalize_script_rowcount(last_rowcount, cursor)
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
        """Begin database transaction."""
        try:
            with self.with_cursor(self.connection) as cursor:
                cursor.execute("BEGIN")
        except Exception as e:
            msg = f"Failed to begin transaction: {e}"
            raise SQLSpecError(msg) from e

    def commit(self) -> None:
        """Commit database transaction."""
        try:
            with self.with_cursor(self.connection) as cursor:
                cursor.execute("COMMIT")
        except Exception as e:
            msg = f"Failed to commit transaction: {e}"
            raise SQLSpecError(msg) from e

    def rollback(self) -> None:
        """Rollback database transaction."""
        try:
            with self.with_cursor(self.connection) as cursor:
                cursor.execute("ROLLBACK")
        except Exception as e:
            msg = f"Failed to rollback transaction: {e}"
            raise SQLSpecError(msg) from e

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

        Example:
            >>> result = driver.select_to_arrow(
            ...     "SELECT * FROM users WHERE age > $1", 18
            ... )
            >>> df = result.to_pandas()  # Fast zero-copy conversion
        """
        ensure_pyarrow()

        # Prepare statement
        config = statement_config or self.statement_config
        prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)

        # Use ADBC cursor for native Arrow
        with self.with_cursor(self.connection) as cursor, self.handle_database_exceptions():
            if cursor is None:
                msg = "Failed to create cursor"
                raise DatabaseConnectionError(msg)

            # Get compiled SQL and parameters
            sql, driver_params = self._get_compiled_sql(prepared_statement, config)

            # Execute query
            cursor.execute(sql, driver_params or ())

            # Fetch as Arrow table (zero-copy!)
            arrow_table = cursor.fetch_arrow_table()

            return build_arrow_result_from_table(
                prepared_statement,
                arrow_table,
                return_format=return_format,
                batch_size=batch_size,
                arrow_schema=arrow_schema,
            )
        msg = "Unreachable"
        raise RuntimeError(msg)  # pragma: no cover

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
        arrow_table = self._coerce_arrow_table(source)
        ingest_mode: Literal["append", "create", "replace", "create_append"]
        ingest_mode = "replace" if overwrite else "create_append"
        with self.with_cursor(self.connection) as cursor, self.handle_database_exceptions():
            cursor.adbc_ingest(table, arrow_table, mode=ingest_mode)
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

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        ADBC uses explicit BEGIN and does not expose reliable transaction state.

        Returns:
            False - ADBC requires explicit transaction management.
        """
        return False

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
        if enable_cast_detection and prepared_statement and self._is_postgres and not is_many:
            parameter_casts = resolve_parameter_casts(prepared_statement)
            return prepare_postgres_parameters(
                parameters,
                parameter_casts,
                statement_config,
                dialect=self._dialect_name,
                json_serializer=self._json_serializer,
            )

        return super().prepare_driver_parameters(parameters, statement_config, is_many, prepared_statement)


register_driver_profile("adbc", driver_profile)
