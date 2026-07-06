"""DuckDB driver implementation."""

import contextlib
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import duckdb
from sqlglot import exp

from sqlspec.adapters.duckdb._typing import DuckDBCursor, DuckDBSessionContext
from sqlspec.adapters.duckdb.core import (
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    normalize_execute_parameters,
    resolve_rowcount,
)
from sqlspec.adapters.duckdb.data_dictionary import DuckDBDataDictionary
from sqlspec.core import (
    SQL,
    StatementConfig,
    build_arrow_result_from_reader,
    build_arrow_result_from_table,
    get_cache_config,
    register_driver_profile,
)
from sqlspec.core.result import DMLResult
from sqlspec.driver import BaseSyncExceptionHandler, SyncDriverAdapterBase
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.text import quote_identifier

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.adapters.duckdb._typing import DuckDBConnection
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, SQLResult, Statement, StatementFilter
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.typing import ArrowReturnFormat, StatementParameters


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
        self.pending_exception = create_mapped_exception(exc_val)
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
        driver_features = dict(driver_features) if driver_features else {}

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: DuckDBDataDictionary | None = None

    # ─────────────────────────────────────────────────────────────────────────────
    # CORE DISPATCH METHODS
    # ─────────────────────────────────────────────────────────────────────────────

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
        cursor.execute(sql, normalize_execute_parameters(prepared_parameters))

        is_select_like = statement.returns_rows() or self._should_force_select(statement, cursor)

        if is_select_like:
            arrow_table = cursor.fetch_arrow_table()
            data = arrow_table.to_pylist()
            column_names = list(arrow_table.column_names)

            return self.create_execution_result(
                cursor,
                selected_data=data,
                column_names=column_names,
                data_row_count=len(data),
                is_select_result=True,
                row_format="dict",
            )

        row_count = resolve_rowcount(cursor)

        return self.create_execution_result(cursor, rowcount_override=row_count)

    def dispatch_execute_many(self, cursor: "DuckDBConnection", statement: SQL) -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using batch processing.

        Uses DuckDB's executemany method for batch operations and calculates
        row counts for both data modification and query operations.

        Args:
            cursor: DuckDB cursor object
            statement: SQL statement with multiple parameter sets

        Returns:
            ExecutionResult with batch execution metadata
        """
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)

        if prepared_parameters:
            parameter_sets = cast("list[Any]", prepared_parameters)
            cursor.executemany(sql, parameter_sets)

            row_count = len(parameter_sets) if statement.is_modifying_operation() else resolve_rowcount(cursor)
        else:
            row_count = 0

        return self.create_execution_result(cursor, rowcount_override=row_count, is_many_result=True)

    def execute_many(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        parameters: "Sequence[StatementParameters]",
        *filters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute many with a DuckDB bulk insert fast path for simple INSERT batches."""
        config = statement_config or self.statement_config
        if isinstance(statement, str) and not filters and not kwargs and config is self.statement_config:
            prepared_statement = SQL(statement, tuple(parameters), statement_config=config, is_many=True)
            _, prepared_parameters = self._compiled_sql(prepared_statement, config)
            processed_state = prepared_statement.get_processed_state()
            parsed_expression = processed_state.parsed_expression
            if isinstance(parsed_expression, exp.Insert) and not parsed_expression.args.get("returning"):
                bulk_result = self._execute_bulk_insert_many(parsed_expression, prepared_parameters)
                if bulk_result is not None:
                    return bulk_result
        return super().execute_many(statement, parameters, *filters, statement_config=statement_config, **kwargs)

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
        """
        ensure_pyarrow()

        config = statement_config or self.statement_config
        prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)

        exc_handler = self.handle_database_exceptions()
        arrow_result: ArrowResult | None = None

        with self.with_cursor(self.connection) as cursor, exc_handler:
            sql, driver_params = self._compiled_sql(prepared_statement, config)

            cursor.execute(sql, driver_params or ())

            if return_format in {"reader", "batches"}:
                arrow_reader = (
                    cursor.to_arrow_reader(batch_size) if batch_size is not None else cursor.to_arrow_reader()
                )
                return build_arrow_result_from_reader(
                    prepared_statement,
                    arrow_reader,
                    return_format=return_format,
                    batch_size=batch_size,
                    arrow_schema=arrow_schema,
                )

            arrow_table = cursor.to_arrow_table()

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
        """Persist DuckDB query output to a storage backend using Arrow fast paths."""

        _ = kwargs
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
    ) -> "StorageBridgeJob":
        """Load Arrow data into DuckDB using temporary table registration."""

        self._require_capability("arrow_import_enabled")
        ensure_pyarrow()
        import pyarrow as pa

        source_data = source.get_data() if hasattr(source, "get_data") else source
        arrow_table = None
        arrow_source: object
        if isinstance(source_data, pa.RecordBatchReader):
            arrow_source = source_data
        else:
            arrow_table = self._coerce_arrow_table(source_data)
            arrow_source = arrow_table
        temp_view = f"_sqlspec_arrow_{uuid4().hex}"
        if overwrite:
            self.connection.execute(f"TRUNCATE TABLE {table}")
        self.connection.register(temp_view, arrow_source)
        inserted_rows = 0
        try:
            insert_result = self.connection.execute(f"INSERT INTO {table} SELECT * FROM {temp_view}")
            inserted_rows = _resolve_duckdb_inserted_rows(insert_result)
        finally:
            with contextlib.suppress(Exception):
                self.connection.unregister(temp_view)

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

        arrow_table, inbound = self._read_storage_arrow(source, file_format=file_format)
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

    def collect_rows(self, cursor: "DuckDBConnection", fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        """Collect DuckDB rows for the direct execution path."""
        data, column_names = collect_rows(cast("list[Any] | None", fetched), cursor.description)
        return data, column_names, len(data)

    def _execute_bulk_insert_many(self, expression: exp.Insert, prepared_parameters: Any) -> "DMLResult | None":
        """Execute a batch INSERT via Arrow registration when the payload is simple."""
        if not isinstance(prepared_parameters, list) or not prepared_parameters:
            return None
        if not isinstance(expression.this, exp.Schema):
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

        target_table = table_expr.sql(dialect="duckdb")
        temp_view = f"_sqlspec_batch_{uuid4().hex}"
        self.connection.register(temp_view, arrow_table)
        try:
            self.connection.execute(f"INSERT INTO {target_table} SELECT * FROM {temp_view}")
        finally:
            with contextlib.suppress(Exception):
                self.connection.unregister(temp_view)

        return DMLResult("INSERT", len(rows))

    @staticmethod
    def _build_arrow_table(rows: "list[Any]", column_names: "list[str]") -> Any | None:
        """Build a pyarrow table from batch rows when they share a stable shape."""
        if not rows:
            return None
        first_row = rows[0]

        if isinstance(first_row, dict):
            keys = column_names or list(first_row.keys())
            if any(not isinstance(row, dict) for row in rows):
                return None
            import pyarrow as pa

            return pa.table({key: [row.get(key) for row in rows] for key in keys})

        if isinstance(first_row, (list, tuple)):
            values = list(first_row)
            if not column_names:
                column_names = [f"col_{index}" for index in range(len(values))]
            if any(not isinstance(row, (list, tuple)) or len(row) != len(column_names) for row in rows):
                return None
            import pyarrow as pa

            return pa.table({name: [row[index] for row in rows] for index, name in enumerate(column_names)})

        return None

    def resolve_rowcount(self, cursor: "DuckDBConnection") -> int:
        """Resolve rowcount from DuckDB cursor for the direct execution path."""
        return resolve_rowcount(cursor)

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        DuckDB uses explicit BEGIN TRANSACTION and does not expose transaction state.

        Returns:
            False - DuckDB requires explicit transaction management.
        """
        return False


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


register_driver_profile("duckdb", driver_profile)
