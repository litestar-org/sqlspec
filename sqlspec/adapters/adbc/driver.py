"""ADBC driver implementation for Arrow Database Connectivity.

Provides database connectivity through ADBC with support for multiple
database dialects, parameter style conversion, and transaction management.
"""

import contextlib
from typing import TYPE_CHECKING, Any, Literal, cast

from sqlspec.adapters.adbc._typing import AdbcSessionContext
from sqlspec.adapters.adbc.core import (
    build_adbc_profile,
    collect_adbc_rows,
    detect_adbc_dialect,
    get_adbc_statement_config,
    handle_postgres_rollback,
    normalize_postgres_empty_parameters,
    prepare_adbc_parameters_with_casts,
    resolve_adbc_parameter_casts,
)
from sqlspec.adapters.adbc.data_dictionary import AdbcDataDictionary
from sqlspec.core import SQL, StatementConfig, create_arrow_result, get_cache_config, register_driver_profile
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    SQLParsingError,
    SQLSpecError,
    TransactionError,
    UniqueViolationError,
)
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.serializers import to_json
from sqlspec.utils.type_guards import has_sqlstate

if TYPE_CHECKING:
    from collections.abc import Callable

    from adbc_driver_manager.dbapi import Cursor

    from sqlspec.adapters.adbc._typing import AdbcConnection
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, Statement, StatementFilter
    from sqlspec.driver import ExecutionResult
    from sqlspec.driver._sync import SyncDataDictionaryBase
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.typing import ArrowReturnFormat, StatementParameters

__all__ = ("AdbcCursor", "AdbcDriver", "AdbcExceptionHandler", "AdbcSessionContext", "get_adbc_statement_config")

logger = get_logger("adapters.adbc")


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
        try:
            self._map_adbc_exception(exc_val)
        except Exception as mapped:
            self.pending_exception = mapped
            return True
        return False

    def _map_adbc_exception(self, e: Any) -> None:
        """Map ADBC exception to SQLSpec exception.

        ADBC drivers may expose SQLSTATE codes or driver-specific codes.

        Args:
            e: ADBC exception instance
        """
        sqlstate = e.sqlstate if has_sqlstate(e) and e.sqlstate is not None else None

        if sqlstate:
            self._map_sqlstate_exception(e, sqlstate)
        else:
            self._map_message_based_exception(e)

    def _map_sqlstate_exception(self, e: Any, sqlstate: str) -> None:
        """Map SQLSTATE code to exception.

        Args:
            e: Exception instance
            sqlstate: SQLSTATE error code
        """
        if sqlstate == "23505":
            self._raise_unique_violation(e)
        elif sqlstate == "23503":
            self._raise_foreign_key_violation(e)
        elif sqlstate == "23502":
            self._raise_not_null_violation(e)
        elif sqlstate == "23514":
            self._raise_check_violation(e)
        elif sqlstate.startswith("23"):
            self._raise_integrity_error(e)
        elif sqlstate.startswith("42"):
            self._raise_parsing_error(e)
        elif sqlstate.startswith("08"):
            self._raise_connection_error(e)
        elif sqlstate.startswith("40"):
            self._raise_transaction_error(e)
        elif sqlstate.startswith("22"):
            self._raise_data_error(e)
        else:
            self._raise_generic_error(e)

    def _map_message_based_exception(self, e: Any) -> None:
        """Map exception using message-based detection.

        Args:
            e: Exception instance
        """
        error_msg = str(e).lower()

        if "unique" in error_msg or "duplicate" in error_msg:
            self._raise_unique_violation(e)
        elif "foreign key" in error_msg:
            self._raise_foreign_key_violation(e)
        elif "not null" in error_msg or "null value" in error_msg:
            self._raise_not_null_violation(e)
        elif "check constraint" in error_msg:
            self._raise_check_violation(e)
        elif "constraint" in error_msg:
            self._raise_integrity_error(e)
        elif "syntax" in error_msg:
            self._raise_parsing_error(e)
        elif "connection" in error_msg or "connect" in error_msg:
            self._raise_connection_error(e)
        else:
            self._raise_generic_error(e)

    def _raise_unique_violation(self, e: Any) -> None:
        msg = f"ADBC unique constraint violation: {e}"
        raise UniqueViolationError(msg) from e

    def _raise_foreign_key_violation(self, e: Any) -> None:
        msg = f"ADBC foreign key constraint violation: {e}"
        raise ForeignKeyViolationError(msg) from e

    def _raise_not_null_violation(self, e: Any) -> None:
        msg = f"ADBC not-null constraint violation: {e}"
        raise NotNullViolationError(msg) from e

    def _raise_check_violation(self, e: Any) -> None:
        msg = f"ADBC check constraint violation: {e}"
        raise CheckViolationError(msg) from e

    def _raise_integrity_error(self, e: Any) -> None:
        msg = f"ADBC integrity constraint violation: {e}"
        raise IntegrityError(msg) from e

    def _raise_parsing_error(self, e: Any) -> None:
        msg = f"ADBC SQL parsing error: {e}"
        raise SQLParsingError(msg) from e

    def _raise_connection_error(self, e: Any) -> None:
        msg = f"ADBC connection error: {e}"
        raise DatabaseConnectionError(msg) from e

    def _raise_transaction_error(self, e: Any) -> None:
        msg = f"ADBC transaction error: {e}"
        raise TransactionError(msg) from e

    def _raise_data_error(self, e: Any) -> None:
        msg = f"ADBC data error: {e}"
        raise DataError(msg) from e

    def _raise_generic_error(self, e: Any) -> None:
        msg = f"ADBC database error: {e}"
        raise SQLSpecError(msg) from e


class AdbcDriver(SyncDriverAdapterBase):
    """ADBC driver for Arrow Database Connectivity.

    Provides database connectivity through ADBC with support for multiple
    database dialects, parameter style conversion, and transaction management.
    """

    __slots__ = ("_data_dictionary", "_detected_dialect", "dialect")

    def __init__(
        self,
        connection: "AdbcConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        self._detected_dialect = detect_adbc_dialect(connection, logger)

        if statement_config is None:
            cache_config = get_cache_config()
            base_config = get_adbc_statement_config(self._detected_dialect)
            statement_config = base_config.replace(
                enable_caching=cache_config.compiled_cache_enabled, enable_parsing=True, enable_validation=True
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self.dialect = statement_config.dialect
        self._data_dictionary: SyncDataDictionaryBase | None = None

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

    def _normalized_dialect(self) -> str:
        dialect = self.dialect
        if dialect is None:
            return ""
        if isinstance(dialect, str):
            return dialect
        return str(dialect)

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
        dialect_name = self._normalized_dialect()

        if enable_cast_detection and prepared_statement and dialect_name in {"postgres", "postgresql"} and not is_many:
            parameter_casts = resolve_adbc_parameter_casts(prepared_statement)
            postgres_compatible = normalize_postgres_empty_parameters(dialect_name, parameters)
            return prepare_adbc_parameters_with_casts(
                postgres_compatible,
                parameter_casts,
                statement_config,
                dialect=dialect_name,
                json_serializer=cast("Callable[[Any], str]", self.driver_features.get("json_serializer", to_json)),
            )

        return super().prepare_driver_parameters(parameters, statement_config, is_many, prepared_statement)

    def _execute_many(self, cursor: "Cursor", statement: SQL) -> "ExecutionResult":
        """Execute SQL with multiple parameter sets.

        Args:
            cursor: Database cursor
            statement: SQL statement to execute

        Returns:
            Execution result with row counts
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        parameter_casts = resolve_adbc_parameter_casts(statement)
        dialect_name = self._normalized_dialect()

        try:
            if not prepared_parameters:
                cursor._rowcount = 0  # pyright: ignore[reportPrivateUsage]
                row_count = 0
            elif isinstance(prepared_parameters, (list, tuple)) and prepared_parameters:
                processed_params = []
                for param_set in prepared_parameters:
                    postgres_compatible = normalize_postgres_empty_parameters(dialect_name, param_set)

                    if dialect_name in {"postgres", "postgresql"}:
                        # For postgres, always use cast-aware parameter preparation
                        formatted_params = prepare_adbc_parameters_with_casts(
                            postgres_compatible,
                            parameter_casts,
                            self.statement_config,
                            dialect=dialect_name,
                            json_serializer=cast(
                                "Callable[[Any], str]", self.driver_features.get("json_serializer", to_json)
                            ),
                        )
                    else:
                        formatted_params = self.prepare_driver_parameters(
                            postgres_compatible, self.statement_config, is_many=False
                        )
                    processed_params.append(formatted_params)

                cursor.executemany(sql, processed_params)
                row_count = cursor.rowcount if cursor.rowcount is not None else -1
            else:
                cursor.executemany(sql, prepared_parameters)
                row_count = cursor.rowcount if cursor.rowcount is not None else -1

        except Exception:
            handle_postgres_rollback(dialect_name, cursor, logger)
            raise

        return self.create_execution_result(cursor, rowcount_override=row_count, is_many_result=True)

    def _execute_statement(self, cursor: "Cursor", statement: SQL) -> "ExecutionResult":
        """Execute single SQL statement.

        Args:
            cursor: Database cursor
            statement: SQL statement to execute

        Returns:
            Execution result with data or row count
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        parameter_casts = resolve_adbc_parameter_casts(statement)
        dialect_name = self._normalized_dialect()

        try:
            postgres_compatible_params = normalize_postgres_empty_parameters(dialect_name, prepared_parameters)

            if dialect_name in {"postgres", "postgresql"}:
                formatted_params = prepare_adbc_parameters_with_casts(
                    postgres_compatible_params,
                    parameter_casts,
                    self.statement_config,
                    dialect=dialect_name,
                    json_serializer=cast("Callable[[Any], str]", self.driver_features.get("json_serializer", to_json)),
                )
                cursor.execute(sql, parameters=formatted_params)
            else:
                cursor.execute(sql, parameters=postgres_compatible_params)

        except Exception:
            handle_postgres_rollback(dialect_name, cursor, logger)
            raise

        is_select_like = statement.returns_rows() or self._should_force_select(statement, cursor)

        if is_select_like:
            fetched_data = cursor.fetchall()
            dict_data, column_names = collect_adbc_rows(cast("list[Any] | None", fetched_data), cursor.description)
            return self.create_execution_result(
                cursor,
                selected_data=dict_data,
                column_names=column_names,
                data_row_count=len(dict_data),
                is_select_result=True,
            )

        row_count = cursor.rowcount if cursor.rowcount is not None else -1
        return self.create_execution_result(cursor, rowcount_override=row_count)

    def _execute_script(self, cursor: "Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute SQL script containing multiple statements.

        Args:
            cursor: Database cursor
            statement: SQL script to execute

        Returns:
            Execution result with statement counts
        """
        if statement.is_script:
            sql = statement.raw_sql
            prepared_parameters: list[Any] = []
        else:
            sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        statements = self.split_script_statements(sql, self.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_rowcount = 0
        dialect_name = self._normalized_dialect()

        try:
            for stmt in statements:
                if prepared_parameters:
                    postgres_compatible_params = normalize_postgres_empty_parameters(dialect_name, prepared_parameters)
                    cursor.execute(stmt, parameters=postgres_compatible_params)
                else:
                    cursor.execute(stmt)
                successful_count += 1
                if cursor.rowcount is not None:
                    last_rowcount = cursor.rowcount
        except Exception:
            handle_postgres_rollback(dialect_name, cursor, logger)
            raise

        return self.create_execution_result(
            cursor,
            statement_count=len(statements),
            successful_statements=successful_count,
            rowcount_override=last_rowcount,
            is_script_result=True,
        )

    def begin(self) -> None:
        """Begin database transaction."""
        try:
            with self.with_cursor(self.connection) as cursor:
                cursor.execute("BEGIN")
        except Exception as e:
            msg = f"Failed to begin transaction: {e}"
            raise SQLSpecError(msg) from e

    def rollback(self) -> None:
        """Rollback database transaction."""
        try:
            with self.with_cursor(self.connection) as cursor:
                cursor.execute("ROLLBACK")
        except Exception as e:
            msg = f"Failed to rollback transaction: {e}"
            raise SQLSpecError(msg) from e

    def commit(self) -> None:
        """Commit database transaction."""
        try:
            with self.with_cursor(self.connection) as cursor:
                cursor.execute("COMMIT")
        except Exception as e:
            msg = f"Failed to commit transaction: {e}"
            raise SQLSpecError(msg) from e

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        ADBC uses explicit BEGIN and does not expose reliable transaction state.

        Returns:
            False - ADBC requires explicit transaction management.
        """
        return False

    @property
    def data_dictionary(self) -> "SyncDataDictionaryBase":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = AdbcDataDictionary()
        return self._data_dictionary

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

        import pyarrow as pa

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

            # Apply schema casting if requested
            if arrow_schema is not None:
                if not isinstance(arrow_schema, pa.Schema):
                    msg = f"arrow_schema must be a pyarrow.Schema, got {type(arrow_schema).__name__}"
                    raise TypeError(msg)
                arrow_table = arrow_table.cast(arrow_schema)

            if return_format == "batch":
                batches = arrow_table.to_batches(max_chunksize=batch_size)
                arrow_data: Any = batches[0] if batches else pa.RecordBatch.from_pydict({})
            elif return_format == "batches":
                arrow_data = arrow_table.to_batches(max_chunksize=batch_size)
            elif return_format == "reader":
                batches = arrow_table.to_batches(max_chunksize=batch_size)
                arrow_data = pa.RecordBatchReader.from_batches(arrow_table.schema, batches)
            else:
                arrow_data = arrow_table

            # Create ArrowResult
            return create_arrow_result(
                statement=prepared_statement, data=arrow_data, rows_affected=arrow_table.num_rows
            )
        msg = "Unreachable"
        raise RuntimeError(msg)  # pragma: no cover

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


_ADBC_PROFILE = build_adbc_profile()

register_driver_profile("adbc", _ADBC_PROFILE)
