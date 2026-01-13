"""Psqlpy driver implementation for PostgreSQL connectivity.

Provides parameter style conversion, type coercion, error handling,
and transaction management.
"""

import inspect
from typing import TYPE_CHECKING, Any, cast

import psqlpy.exceptions

from sqlspec.adapters.psqlpy._typing import PsqlpySessionContext
from sqlspec.adapters.psqlpy.core import (
    build_insert_statement,
    coerce_numeric_for_write,
    coerce_records_for_execute_many,
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    encode_records_for_binary_copy,
    extract_rows_affected,
    format_table_identifier,
    get_parameter_casts,
    normalize_scalar_parameter,
    prepare_parameters_with_casts,
    split_schema_and_table,
)
from sqlspec.adapters.psqlpy.data_dictionary import PsqlpyDataDictionary
from sqlspec.adapters.psqlpy.type_converter import PostgreSQLOutputConverter
from sqlspec.core import SQL, StatementConfig, get_cache_config, register_driver_profile
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlspec.adapters.psqlpy._typing import PsqlpyConnection
    from sqlspec.core import ArrowResult
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry

__all__ = ("PsqlpyCursor", "PsqlpyDriver", "PsqlpyExceptionHandler", "PsqlpySessionContext")

logger = get_logger("sqlspec.adapters.psqlpy")

_type_converter = PostgreSQLOutputConverter()


class PsqlpyCursor:
    """Context manager for psqlpy cursor management."""

    __slots__ = ("_in_use", "connection")

    def __init__(self, connection: "PsqlpyConnection") -> None:
        self.connection = connection
        self._in_use = False

    async def __aenter__(self) -> "PsqlpyConnection":
        """Enter cursor context.

        Returns:
            Psqlpy connection object
        """
        self._in_use = True
        return self.connection

    async def __aexit__(self, *_: Any) -> None:
        """Exit cursor context.

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Exception traceback
        """
        self._in_use = False

    def is_in_use(self) -> bool:
        """Check if cursor is currently in use.

        Returns:
            True if cursor is in use, False otherwise
        """
        return self._in_use


class PsqlpyExceptionHandler:
    """Async context manager for handling psqlpy database exceptions.

    Maps PostgreSQL SQLSTATE error codes to specific SQLSpec exceptions
    for better error handling in application code.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __aexit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    async def __aenter__(self) -> "PsqlpyExceptionHandler":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_type is None:
            return False
        if issubclass(exc_type, (psqlpy.exceptions.DatabaseError, psqlpy.exceptions.Error)):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class PsqlpyDriver(AsyncDriverAdapterBase):
    """PostgreSQL driver implementation using psqlpy.

    Provides parameter style conversion, type coercion, error handling,
    and transaction management.
    """

    __slots__ = ("_data_dictionary",)
    dialect = "postgres"

    def __init__(
        self,
        connection: "PsqlpyConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: PsqlpyDataDictionary | None = None

    # ─────────────────────────────────────────────────────────────────────────────
    # CORE DISPATCH METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    async def dispatch_execute(self, cursor: "PsqlpyConnection", statement: SQL) -> "ExecutionResult":
        """Execute single SQL statement.

        Args:
            cursor: Psqlpy connection object
            statement: SQL statement to execute

        Returns:
            ExecutionResult with execution metadata
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        driver_parameters = prepared_parameters
        operation_type = statement.operation_type
        should_coerce = operation_type != "SELECT"
        effective_parameters = coerce_numeric_for_write(driver_parameters) if should_coerce else driver_parameters
        params = cast("Sequence[Any] | Mapping[str, Any] | None", effective_parameters) or []

        if statement.returns_rows():
            query_result = await cursor.fetch(sql, params)
            dict_rows, column_names = collect_rows(query_result)

            return self.create_execution_result(
                cursor,
                selected_data=dict_rows,
                column_names=column_names,
                data_row_count=len(dict_rows),
                is_select_result=True,
            )

        result = await cursor.execute(sql, params)
        rows_affected = extract_rows_affected(result)

        return self.create_execution_result(cursor, rowcount_override=rows_affected)

    async def dispatch_execute_many(self, cursor: "PsqlpyConnection", statement: SQL) -> "ExecutionResult":
        """Execute SQL with multiple parameter sets.

        Args:
            cursor: Psqlpy connection object
            statement: SQL statement with multiple parameter sets

        Returns:
            ExecutionResult with batch execution metadata
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if not prepared_parameters:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)

        driver_parameters = self.prepare_driver_parameters(
            prepared_parameters, self.statement_config, is_many=True, prepared_statement=statement
        )

        operation_type = statement.operation_type
        should_coerce = operation_type != "SELECT"

        formatted_parameters = []
        for param_set in driver_parameters:
            values = list(param_set) if isinstance(param_set, (list, tuple)) else [param_set]

            if should_coerce:
                values = list(coerce_numeric_for_write(values))

            formatted_parameters.append(values)

        await cursor.execute_many(sql, formatted_parameters)

        rows_affected = len(formatted_parameters)

        return self.create_execution_result(cursor, rowcount_override=rows_affected, is_many_result=True)

    async def dispatch_execute_script(self, cursor: "PsqlpyConnection", statement: SQL) -> "ExecutionResult":
        """Execute SQL script with statement splitting.

        Args:
            cursor: Psqlpy connection object
            statement: SQL statement with script content

        Returns:
            ExecutionResult with script execution metadata

        Notes:
            Uses execute() with empty parameters for each statement instead of execute_batch().
            execute_batch() uses simple query protocol which can break subsequent queries
            that rely on extended protocol (e.g., information_schema queries with name type).
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        prepared_parameters = cast("Sequence[Any] | Mapping[str, Any] | None", prepared_parameters)
        statement_config = statement.statement_config
        statements = self.split_script_statements(sql, statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_result = None

        for stmt in statements:
            last_result = await cursor.execute(stmt, prepared_parameters or [])
            successful_count += 1

        return self.create_execution_result(
            last_result, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    # ─────────────────────────────────────────────────────────────────────────────
    # TRANSACTION MANAGEMENT
    # ─────────────────────────────────────────────────────────────────────────────

    async def begin(self) -> None:
        """Begin a database transaction."""
        try:
            await self.connection.execute("BEGIN")
        except psqlpy.exceptions.DatabaseError as e:
            msg = f"Failed to begin psqlpy transaction: {e}"
            raise SQLSpecError(msg) from e

    async def commit(self) -> None:
        """Commit the current transaction."""
        try:
            await self.connection.execute("COMMIT")
        except psqlpy.exceptions.DatabaseError as e:
            msg = f"Failed to commit psqlpy transaction: {e}"
            raise SQLSpecError(msg) from e

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        try:
            await self.connection.execute("ROLLBACK")
        except psqlpy.exceptions.DatabaseError as e:
            msg = f"Failed to rollback psqlpy transaction: {e}"
            raise SQLSpecError(msg) from e

    def with_cursor(self, connection: "PsqlpyConnection") -> "PsqlpyCursor":
        """Create context manager for psqlpy cursor.

        Args:
            connection: Psqlpy connection object

        Returns:
            PsqlpyCursor context manager
        """
        return PsqlpyCursor(connection)

    def handle_database_exceptions(self) -> "PsqlpyExceptionHandler":
        """Handle database-specific exceptions.

        Returns:
            Exception handler context manager
        """
        return PsqlpyExceptionHandler()

    # ─────────────────────────────────────────────────────────────────────────────
    # STORAGE API METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    async def select_to_storage(
        self,
        statement: "SQL | str",
        destination: "StorageDestination",
        /,
        *parameters: Any,
        statement_config: "StatementConfig | None" = None,
        partitioner: "dict[str, object] | None" = None,
        format_hint: "StorageFormat | None" = None,
        telemetry: "StorageTelemetry | None" = None,
        **kwargs: Any,
    ) -> "StorageBridgeJob":
        """Execute a query and stream Arrow results to a storage backend."""

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
        """Load Arrow-formatted data into PostgreSQL via psqlpy binary COPY."""

        self._require_capability("arrow_import_enabled")
        arrow_table = self._coerce_arrow_table(source)
        if overwrite:
            qualified = format_table_identifier(table)
            async with self.handle_database_exceptions(), self.with_cursor(self.connection) as cursor:
                await cursor.execute(f"TRUNCATE TABLE {qualified}")

        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            schema_name, table_name = split_schema_and_table(table)
            async with self.handle_database_exceptions(), self.with_cursor(self.connection) as cursor:
                copy_kwargs: dict[str, Any] = {"columns": columns}
                if schema_name:
                    copy_kwargs["schema_name"] = schema_name
                try:
                    copy_payload = encode_records_for_binary_copy(records)
                    copy_operation = cursor.binary_copy_to_table(copy_payload, table_name, **copy_kwargs)
                    if inspect.isawaitable(copy_operation):
                        await copy_operation
                except (TypeError, psqlpy.exceptions.DatabaseError) as exc:
                    logger.debug("Binary COPY not available for psqlpy; falling back to INSERT statements: %s", exc)
                    insert_sql = build_insert_statement(table, columns)
                    formatted_records = coerce_records_for_execute_many(records)
                    insert_operation = cursor.execute_many(insert_sql, formatted_records)
                    if inspect.isawaitable(insert_operation):
                        await insert_operation

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
        """Load staged artifacts from storage using the storage bridge pipeline."""

        arrow_table, inbound = await self._read_arrow_from_storage_async(source, file_format=file_format)
        return await self.load_from_arrow(
            table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound
        )

    # ─────────────────────────────────────────────────────────────────────────────
    # UTILITY METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def prepare_driver_parameters(
        self,
        parameters: Any,
        statement_config: "StatementConfig",
        is_many: bool = False,
        prepared_statement: Any | None = None,
    ) -> Any:
        """Prepare parameters with cast-aware type coercion for psqlpy.

        Args:
            parameters: Parameters in any format
            statement_config: Statement configuration
            is_many: Whether this is for execute_many operation
            prepared_statement: Prepared statement containing the original SQL statement

        Returns:
            Parameters with cast-aware type coercion applied
        """
        enable_cast_detection = self.driver_features.get("enable_cast_detection", True)

        if enable_cast_detection and prepared_statement and self.dialect in {"postgres", "postgresql"} and not is_many:
            parameter_casts = get_parameter_casts(prepared_statement)
            prepared = prepare_parameters_with_casts(parameters, parameter_casts, statement_config)
        else:
            prepared = super().prepare_driver_parameters(parameters, statement_config, is_many, prepared_statement)

        if not is_many and isinstance(prepared, list):
            prepared = tuple(prepared)

        if not is_many and isinstance(prepared, tuple):
            return tuple(normalize_scalar_parameter(item) for item in prepared)

        return prepared

    @property
    def data_dictionary(self) -> "PsqlpyDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = PsqlpyDataDictionary()
        return self._data_dictionary

    # ─────────────────────────────────────────────────────────────────────────────
    # PRIVATE/INTERNAL METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction."""
        return bool(self.connection.in_transaction())


register_driver_profile("psqlpy", driver_profile)
