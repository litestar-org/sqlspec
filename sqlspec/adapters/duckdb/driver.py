"""DuckDB driver implementation."""

import contextlib
import typing
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import duckdb

from sqlspec.adapters.duckdb.core import build_duckdb_profile
from sqlspec.adapters.duckdb.data_dictionary import DuckDBSyncDataDictionary
from sqlspec.adapters.duckdb.type_converter import DuckDBOutputConverter
from sqlspec.core import (
    SQL,
    StatementConfig,
    build_statement_config_from_profile,
    create_arrow_result,
    get_cache_config,
    register_driver_profile,
)
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    ForeignKeyViolationError,
    IntegrityError,
    NotFoundError,
    NotNullViolationError,
    OperationalError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.serializers import to_json
from sqlspec.utils.type_guards import has_rowcount

if TYPE_CHECKING:
    from sqlspec.adapters.duckdb._typing import DuckDBConnection
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, Statement, StatementFilter
    from sqlspec.driver import ExecutionResult
    from sqlspec.driver._sync import SyncDataDictionaryBase
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.typing import ArrowReturnFormat, StatementParameters

from sqlspec.adapters.duckdb._typing import DuckDBSessionContext

__all__ = (
    "DuckDBCursor",
    "DuckDBDriver",
    "DuckDBExceptionHandler",
    "DuckDBSessionContext",
    "build_duckdb_statement_config",
    "duckdb_statement_config",
)

logger = get_logger("adapters.duckdb")

_type_converter = DuckDBOutputConverter()


class DuckDBCursor:
    """Context manager for DuckDB cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "DuckDBConnection") -> None:
        self.connection = connection
        self.cursor: Any | None = None

    def __enter__(self) -> Any:
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        if self.cursor is not None:
            self.cursor.close()


class DuckDBExceptionHandler:
    """Context manager for handling DuckDB database exceptions.

    Uses exception type and message-based detection to map DuckDB errors
    to specific SQLSpec exceptions for better error handling.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __exit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    def __enter__(self) -> "DuckDBExceptionHandler":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        _ = exc_tb
        if exc_type is None:
            return False
        try:
            self._map_duckdb_exception(exc_type, exc_val)
        except Exception as mapped:
            self.pending_exception = mapped
            return True
        return False

    def _map_duckdb_exception(self, exc_type: Any, e: Any) -> None:
        """Map DuckDB exception to SQLSpec exception.

        Uses exception type and message-based detection.

        Args:
            exc_type: Exception type
            e: Exception instance
        """
        error_msg = str(e).lower()
        exc_name = exc_type.__name__

        if "constraintexception" in exc_name.lower():
            self._handle_constraint_exception(e, error_msg)
        elif "catalogexception" in exc_name.lower():
            self._raise_not_found_error(e)
        elif "parserexception" in exc_name.lower() or "binderexception" in exc_name.lower():
            self._raise_parsing_error(e)
        elif "ioexception" in exc_name.lower():
            self._raise_operational_error(e)
        elif "conversionexception" in exc_name.lower() or "type mismatch" in error_msg:
            self._raise_data_error(e)
        else:
            self._raise_generic_error(e)

    def _handle_constraint_exception(self, e: Any, error_msg: str) -> None:
        """Handle constraint exceptions using message-based detection.

        Args:
            e: Exception instance
            error_msg: Lowercase error message
        """
        if "unique" in error_msg or "duplicate" in error_msg:
            self._raise_unique_violation(e)
        elif "foreign key" in error_msg or "violates foreign key" in error_msg:
            self._raise_foreign_key_violation(e)
        elif "not null" in error_msg or "null value" in error_msg:
            self._raise_not_null_violation(e)
        elif "check constraint" in error_msg or "check condition" in error_msg:
            self._raise_check_violation(e)
        else:
            self._raise_integrity_error(e)

    def _raise_unique_violation(self, e: Any) -> None:
        msg = f"DuckDB unique constraint violation: {e}"
        raise UniqueViolationError(msg) from e

    def _raise_foreign_key_violation(self, e: Any) -> None:
        msg = f"DuckDB foreign key constraint violation: {e}"
        raise ForeignKeyViolationError(msg) from e

    def _raise_not_null_violation(self, e: Any) -> None:
        msg = f"DuckDB not-null constraint violation: {e}"
        raise NotNullViolationError(msg) from e

    def _raise_check_violation(self, e: Any) -> None:
        msg = f"DuckDB check constraint violation: {e}"
        raise CheckViolationError(msg) from e

    def _raise_integrity_error(self, e: Any) -> None:
        msg = f"DuckDB integrity constraint violation: {e}"
        raise IntegrityError(msg) from e

    def _raise_not_found_error(self, e: Any) -> None:
        msg = f"DuckDB catalog error: {e}"
        raise NotFoundError(msg) from e

    def _raise_parsing_error(self, e: Any) -> None:
        msg = f"DuckDB SQL parsing error: {e}"
        raise SQLParsingError(msg) from e

    def _raise_operational_error(self, e: Any) -> None:
        msg = f"DuckDB operational error: {e}"
        raise OperationalError(msg) from e

    def _raise_data_error(self, e: Any) -> None:
        msg = f"DuckDB data error: {e}"
        raise DataError(msg) from e

    def _raise_generic_error(self, e: Any) -> None:
        msg = f"DuckDB database error: {e}"
        raise SQLSpecError(msg) from e


class DuckDBDriver(SyncDriverAdapterBase):
    """Synchronous DuckDB database driver.

    Provides SQL statement execution, transaction management, and result handling
    for DuckDB databases. Supports multiple parameter styles including QMARK,
    NUMERIC, and NAMED_DOLLAR formats.

    The driver handles script execution, batch operations, and integrates with
    the sqlspec.core modules for statement processing and caching.
    """

    __slots__ = ("_data_dictionary",)
    dialect = "duckdb"

    def __init__(
        self,
        connection: "DuckDBConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            cache_config = get_cache_config()
            updated_config = duckdb_statement_config.replace(
                enable_caching=cache_config.compiled_cache_enabled,
                enable_parsing=True,
                enable_validation=True,
                dialect="duckdb",
            )
            statement_config = updated_config

        if driver_features:
            param_config = statement_config.parameter_config
            json_serializer = driver_features.get("json_serializer")
            if json_serializer:
                param_config = param_config.with_json_serializers(json_serializer, tuple_strategy="tuple")

            enable_uuid_conversion = driver_features.get("enable_uuid_conversion", True)
            if not enable_uuid_conversion:
                type_converter = DuckDBOutputConverter(enable_uuid_conversion=enable_uuid_conversion)
                type_coercion_map = dict(param_config.type_coercion_map)
                type_coercion_map[str] = type_converter.convert_if_detected
                param_config = param_config.replace(type_coercion_map=type_coercion_map)

            if param_config is not statement_config.parameter_config:
                statement_config = statement_config.replace(parameter_config=param_config)

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: SyncDataDictionaryBase | None = None

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

    def _execute_script(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        """Execute SQL script with statement splitting and parameter handling.

        Parses multi-statement scripts and executes each statement sequentially
        with the provided parameters.

        Args:
            cursor: DuckDB cursor object
            statement: SQL statement with script content

        Returns:
            ExecutionResult with script execution metadata
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_result = None

        for stmt in statements:
            last_result = cursor.execute(stmt, prepared_parameters or ())
            successful_count += 1

        return self.create_execution_result(
            last_result, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def _execute_many(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using batch processing.

        Uses DuckDB's executemany method for batch operations and calculates
        row counts for both data modification and query operations.

        Args:
            cursor: DuckDB cursor object
            statement: SQL statement with multiple parameter sets

        Returns:
            ExecutionResult with batch execution metadata
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if prepared_parameters:
            cursor.executemany(sql, prepared_parameters)

            if statement.is_modifying_operation():
                row_count = len(prepared_parameters)
            else:
                try:
                    result = cursor.fetchone()
                    row_count = int(result[0]) if result and isinstance(result, tuple) and len(result) == 1 else 0
                except Exception:
                    row_count = max(cursor.rowcount, 0) if has_rowcount(cursor) else 0
        else:
            row_count = 0

        return self.create_execution_result(cursor, rowcount_override=row_count, is_many_result=True)

    def _execute_statement(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        """Execute single SQL statement with data handling.

        Executes a SQL statement with parameter binding and processes the results.
        Handles both data-returning queries and data modification operations.

        Args:
            cursor: DuckDB cursor object
            statement: SQL statement to execute

        Returns:
            ExecutionResult with execution metadata
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        cursor.execute(sql, prepared_parameters or ())

        is_select_like = statement.returns_rows() or self._should_force_select(statement, cursor)

        if is_select_like:
            fetched_data = cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]

            if fetched_data and isinstance(fetched_data[0], tuple):
                dict_data = [dict(zip(column_names, row, strict=False)) for row in fetched_data]
            else:
                dict_data = fetched_data

            return self.create_execution_result(
                cursor,
                selected_data=dict_data,
                column_names=column_names,
                data_row_count=len(dict_data),
                is_select_result=True,
            )

        try:
            result = cursor.fetchone()
            row_count = int(result[0]) if result and isinstance(result, tuple) and len(result) == 1 else 0
        except Exception:
            row_count = max(cursor.rowcount, 0) if has_rowcount(cursor) else 0

        return self.create_execution_result(cursor, rowcount_override=row_count)

    def begin(self) -> None:
        """Begin a database transaction."""
        try:
            self.connection.execute("BEGIN TRANSACTION")
        except duckdb.Error as e:
            msg = f"Failed to begin DuckDB transaction: {e}"
            raise SQLSpecError(msg) from e

    def rollback(self) -> None:
        """Rollback the current transaction."""
        try:
            self.connection.rollback()
        except duckdb.Error as e:
            msg = f"Failed to rollback DuckDB transaction: {e}"
            raise SQLSpecError(msg) from e

    def commit(self) -> None:
        """Commit the current transaction."""
        try:
            self.connection.commit()
        except duckdb.Error as e:
            msg = f"Failed to commit DuckDB transaction: {e}"
            raise SQLSpecError(msg) from e

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        DuckDB uses explicit BEGIN TRANSACTION and does not expose transaction state.

        Returns:
            False - DuckDB requires explicit transaction management.
        """
        return False

    @property
    def data_dictionary(self) -> "SyncDataDictionaryBase":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = DuckDBSyncDataDictionary()
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
        Example:
            >>> result = driver.select_to_arrow(
            ...     "SELECT * FROM users WHERE age > ?", 18
            ... )
            >>> df = result.to_pandas()  # Fast zero-copy conversion
        """
        ensure_pyarrow()

        import pyarrow as pa

        # Prepare statement
        config = statement_config or self.statement_config
        prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)

        # Execute query and get native Arrow
        with self.with_cursor(self.connection) as cursor, self.handle_database_exceptions():
            if cursor is None:
                msg = "Failed to create cursor"
                raise DatabaseConnectionError(msg)

            # Get compiled SQL and parameters
            sql, driver_params = self._get_compiled_sql(prepared_statement, config)

            # Execute query
            cursor.execute(sql, driver_params or ())

            # DuckDB native Arrow (zero-copy!)
            arrow_reader = cursor.arrow()
            arrow_table = arrow_reader.read_all()

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
        """Persist DuckDB query output to a storage backend using Arrow fast paths."""

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
        """Load Arrow data into DuckDB using temporary table registration."""

        self._require_capability("arrow_import_enabled")
        arrow_table = self._coerce_arrow_table(source)
        temp_view = f"_sqlspec_arrow_{uuid4().hex}"
        if overwrite:
            self.connection.execute(f"TRUNCATE TABLE {table}")
        self.connection.register(temp_view, arrow_table)
        try:
            self.connection.execute(f"INSERT INTO {table} SELECT * FROM {temp_view}")
        finally:
            with contextlib.suppress(Exception):
                self.connection.unregister(temp_view)

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
        """Read an artifact from storage and load it into DuckDB."""

        arrow_table, inbound = self._read_arrow_from_storage_sync(source, file_format=file_format)
        return self.load_from_arrow(table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound)


_DUCKDB_PROFILE = build_duckdb_profile()

register_driver_profile("duckdb", _DUCKDB_PROFILE)


def build_duckdb_statement_config(*, json_serializer: "typing.Callable[[Any], str] | None" = None) -> StatementConfig:
    """Construct the DuckDB statement configuration with optional JSON serializer."""

    serializer = json_serializer or to_json
    return build_statement_config_from_profile(
        _DUCKDB_PROFILE, statement_overrides={"dialect": "duckdb"}, json_serializer=serializer
    )


duckdb_statement_config = build_duckdb_statement_config()


MODIFYING_OPERATIONS: "tuple[str, ...]" = ("INSERT", "UPDATE", "DELETE")
