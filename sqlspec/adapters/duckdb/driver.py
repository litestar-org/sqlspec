"""DuckDB driver implementation."""

import contextlib
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import duckdb

from sqlspec.adapters.duckdb.core import (
    apply_driver_features,
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    normalize_execute_parameters,
    resolve_rowcount,
)
from sqlspec.adapters.duckdb.data_dictionary import DuckDBDataDictionary
from sqlspec.adapters.duckdb.type_converter import DuckDBOutputConverter
from sqlspec.core import SQL, StatementConfig, build_arrow_result_from_table, get_cache_config, register_driver_profile
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.exceptions import DatabaseConnectionError, SQLSpecError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_pyarrow

if TYPE_CHECKING:
    from sqlspec.adapters.duckdb._typing import DuckDBConnection
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, Statement, StatementFilter
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.typing import ArrowReturnFormat, StatementParameters

from sqlspec.adapters.duckdb._typing import DuckDBSessionContext

__all__ = ("DuckDBCursor", "DuckDBDriver", "DuckDBExceptionHandler", "DuckDBSessionContext")

logger = get_logger("sqlspec.adapters.duckdb")

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
        self.pending_exception = create_mapped_exception(exc_type, exc_val)
        return True


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
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        statement_config = apply_driver_features(statement_config, driver_features)

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: DuckDBDataDictionary | None = None

    # ─────────────────────────────────────────────────────────────────────────────
    # CORE DISPATCH METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def dispatch_execute(self, cursor: Any, statement: SQL) -> "ExecutionResult":
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
        cursor.execute(sql, normalize_execute_parameters(prepared_parameters))

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

    def dispatch_execute_many(self, cursor: Any, statement: SQL) -> "ExecutionResult":
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
            parameter_sets = cast("list[Any]", prepared_parameters)
            cursor.executemany(sql, parameter_sets)

            row_count = len(parameter_sets) if statement.is_modifying_operation() else resolve_rowcount(cursor)
        else:
            row_count = 0

        return self.create_execution_result(cursor, rowcount_override=row_count, is_many_result=True)

    def dispatch_execute_script(self, cursor: Any, statement: SQL) -> "ExecutionResult":
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
            last_result = cursor.execute(stmt, normalize_execute_parameters(prepared_parameters))
            successful_count += 1

        return self.create_execution_result(
            last_result, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    # ─────────────────────────────────────────────────────────────────────────────
    # TRANSACTION MANAGEMENT
    # ─────────────────────────────────────────────────────────────────────────────

    def begin(self) -> None:
        """Begin a database transaction."""
        try:
            self.connection.execute("BEGIN TRANSACTION")
        except duckdb.Error as e:
            msg = f"Failed to begin DuckDB transaction: {e}"
            raise SQLSpecError(msg) from e

    def commit(self) -> None:
        """Commit the current transaction."""
        try:
            self.connection.commit()
        except duckdb.Error as e:
            msg = f"Failed to commit DuckDB transaction: {e}"
            raise SQLSpecError(msg) from e

    def rollback(self) -> None:
        """Rollback the current transaction."""
        try:
            self.connection.rollback()
        except duckdb.Error as e:
            msg = f"Failed to rollback DuckDB transaction: {e}"
            raise SQLSpecError(msg) from e

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

    # ─────────────────────────────────────────────────────────────────────────────
    # UTILITY METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    @property
    def data_dictionary(self) -> "DuckDBDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = DuckDBDataDictionary()
        return self._data_dictionary

    # ─────────────────────────────────────────────────────────────────────────────
    # PRIVATE / INTERNAL METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        DuckDB uses explicit BEGIN TRANSACTION and does not expose transaction state.

        Returns:
            False - DuckDB requires explicit transaction management.
        """
        return False


register_driver_profile("duckdb", driver_profile)


MODIFYING_OPERATIONS: "tuple[str, ...]" = ("INSERT", "UPDATE", "DELETE")
