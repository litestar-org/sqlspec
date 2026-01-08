"""Psqlpy driver implementation for PostgreSQL connectivity.

Provides parameter style conversion, type coercion, error handling,
and transaction management.
"""

import inspect
from typing import TYPE_CHECKING, Any

import psqlpy.exceptions

from sqlspec.adapters.psqlpy._typing import PsqlpySessionContext
from sqlspec.adapters.psqlpy.core import (
    build_psqlpy_insert_statement,
    build_psqlpy_profile,
    build_psqlpy_statement_config,
    coerce_numeric_for_write,
    coerce_parameter_for_cast,
    coerce_records_for_execute_many,
    collect_psqlpy_rows,
    encode_records_for_binary_copy,
    format_table_identifier,
    normalize_scalar_parameter,
    parse_psqlpy_command_tag,
    psqlpy_statement_config,
    split_schema_and_table,
)
from sqlspec.adapters.psqlpy.data_dictionary import PsqlpyDataDictionary
from sqlspec.adapters.psqlpy.type_converter import PostgreSQLOutputConverter
from sqlspec.core import SQL, StatementConfig, get_cache_config, register_driver_profile
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    SQLParsingError,
    SQLSpecError,
    TransactionError,
    UniqueViolationError,
)
from sqlspec.typing import Empty
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import to_json
from sqlspec.utils.type_guards import has_query_result_metadata

if TYPE_CHECKING:
    from typing_extensions import Self

    from sqlspec.adapters.psqlpy._typing import PsqlpyConnection
    from sqlspec.core import ArrowResult
    from sqlspec.driver import ExecutionResult
    from sqlspec.driver._async import AsyncDataDictionaryBase
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry

__all__ = (
    "PsqlpyCursor",
    "PsqlpyDriver",
    "PsqlpyExceptionHandler",
    "PsqlpySessionContext",
    "build_psqlpy_statement_config",
    "psqlpy_statement_config",
)

logger = get_logger("adapters.psqlpy")

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
            try:
                self._map_postgres_exception(exc_val)
            except Exception as mapped:
                self.pending_exception = mapped
                return True
        return False

    def _map_postgres_exception(self, e: Any) -> None:
        """Map PostgreSQL exception to SQLSpec exception.

        psqlpy does not expose SQLSTATE codes directly, so we use message-based
        detection to map exceptions.

        Args:
            e: psqlpy exception instance

        Raises:
            Specific SQLSpec exception based on error message patterns
        """
        error_msg = str(e).lower()

        if "unique" in error_msg or "duplicate key" in error_msg:
            self._raise_unique_violation(e, None)
        elif "foreign key" in error_msg or "violates foreign key" in error_msg:
            self._raise_foreign_key_violation(e, None)
        elif "not null" in error_msg or ("null value" in error_msg and "violates not-null" in error_msg):
            self._raise_not_null_violation(e, None)
        elif "check constraint" in error_msg or "violates check constraint" in error_msg:
            self._raise_check_violation(e, None)
        elif "constraint" in error_msg:
            self._raise_integrity_error(e, None)
        elif "syntax error" in error_msg or "parse" in error_msg:
            self._raise_parsing_error(e, None)
        elif "connection" in error_msg or "could not connect" in error_msg:
            self._raise_connection_error(e, None)
        elif "deadlock" in error_msg or "serialization failure" in error_msg:
            self._raise_transaction_error(e, None)
        else:
            self._raise_generic_error(e, None)

    def _raise_unique_violation(self, e: Any, code: "str | None") -> None:
        msg = f"PostgreSQL unique constraint violation: {e}"
        raise UniqueViolationError(msg) from e

    def _raise_foreign_key_violation(self, e: Any, code: "str | None") -> None:
        msg = f"PostgreSQL foreign key constraint violation: {e}"
        raise ForeignKeyViolationError(msg) from e

    def _raise_not_null_violation(self, e: Any, code: "str | None") -> None:
        msg = f"PostgreSQL not-null constraint violation: {e}"
        raise NotNullViolationError(msg) from e

    def _raise_check_violation(self, e: Any, code: "str | None") -> None:
        msg = f"PostgreSQL check constraint violation: {e}"
        raise CheckViolationError(msg) from e

    def _raise_integrity_error(self, e: Any, code: "str | None") -> None:
        msg = f"PostgreSQL integrity constraint violation: {e}"
        raise IntegrityError(msg) from e

    def _raise_parsing_error(self, e: Any, code: "str | None") -> None:
        msg = f"PostgreSQL SQL syntax error: {e}"
        raise SQLParsingError(msg) from e

    def _raise_connection_error(self, e: Any, code: "str | None") -> None:
        msg = f"PostgreSQL connection error: {e}"
        raise DatabaseConnectionError(msg) from e

    def _raise_transaction_error(self, e: Any, code: "str | None") -> None:
        msg = f"PostgreSQL transaction error: {e}"
        raise TransactionError(msg) from e

    def _raise_data_error(self, e: Any, code: "str | None") -> None:
        msg = f"PostgreSQL data error: {e}"
        raise DataError(msg) from e

    def _raise_operational_error(self, e: Any, code: "str | None") -> None:
        msg = f"PostgreSQL operational error: {e}"
        raise OperationalError(msg) from e

    def _raise_generic_error(self, e: Any, code: "str | None") -> None:
        msg = f"PostgreSQL database error: {e}"
        raise SQLSpecError(msg) from e


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
            cache_config = get_cache_config()
            statement_config = psqlpy_statement_config.replace(enable_caching=cache_config.compiled_cache_enabled)

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: AsyncDataDictionaryBase[AsyncDriverAdapterBase] | None = None

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
            parameter_casts = self._get_parameter_casts(prepared_statement)
            prepared = self._prepare_parameters_with_casts(parameters, parameter_casts, statement_config)
        else:
            prepared = super().prepare_driver_parameters(parameters, statement_config, is_many, prepared_statement)

        if not is_many and isinstance(prepared, list):
            prepared = tuple(prepared)

        if not is_many and isinstance(prepared, tuple):
            return tuple(normalize_scalar_parameter(item) for item in prepared)

        return prepared

    def _get_parameter_casts(self, statement: SQL) -> "dict[int, str]":
        """Get parameter cast metadata from compiled statement.

        Args:
            statement: SQL statement with compiled metadata

        Returns:
            Dict mapping parameter positions to cast types
        """
        processed_state = statement.get_processed_state()
        if processed_state is not Empty:
            return processed_state.parameter_casts or {}
        return {}

    def _prepare_parameters_with_casts(
        self, parameters: Any, parameter_casts: "dict[int, str]", statement_config: "StatementConfig"
    ) -> Any:
        """Prepare parameters with cast-aware type coercion.

        Args:
            parameters: Parameter values (list, tuple, or scalar)
            parameter_casts: Mapping of parameter positions to cast types
            statement_config: Statement configuration for type coercion

        Returns:
            Parameters with cast-aware type coercion applied
        """
        if isinstance(parameters, (list, tuple)):
            result: list[Any] = []
            serializer = statement_config.parameter_config.json_serializer or to_json
            type_map = statement_config.parameter_config.type_coercion_map
            for idx, param in enumerate(parameters, start=1):
                cast_type = parameter_casts.get(idx, "")
                prepared_value = param
                if type_map:
                    for type_check, converter in type_map.items():
                        if isinstance(prepared_value, type_check):
                            prepared_value = converter(prepared_value)
                            break
                if cast_type:
                    prepared_value = coerce_parameter_for_cast(prepared_value, cast_type, serializer)
                result.append(prepared_value)
            return tuple(result) if isinstance(parameters, tuple) else result
        return parameters

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

    async def _execute_script(self, cursor: "PsqlpyConnection", statement: SQL) -> "ExecutionResult":
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

    async def _execute_many(self, cursor: "PsqlpyConnection", statement: SQL) -> "ExecutionResult":
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

    async def _execute_statement(self, cursor: "PsqlpyConnection", statement: SQL) -> "ExecutionResult":
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

        if statement.returns_rows():
            query_result = await cursor.fetch(sql, effective_parameters or [])
            dict_rows, column_names = collect_psqlpy_rows(query_result)

            return self.create_execution_result(
                cursor,
                selected_data=dict_rows,
                column_names=column_names,
                data_row_count=len(dict_rows),
                is_select_result=True,
            )

        result = await cursor.execute(sql, effective_parameters or [])
        rows_affected = self._extract_rows_affected(result)

        return self.create_execution_result(cursor, rowcount_override=rows_affected)

    def _extract_rows_affected(self, result: Any) -> int:
        """Extract rows affected from psqlpy result.

        Args:
            result: Psqlpy execution result object

        Returns:
            Number of rows affected, -1 if unable to determine
        """
        try:
            if has_query_result_metadata(result):
                if result.tag:
                    return parse_psqlpy_command_tag(result.tag)
                if result.status:
                    return parse_psqlpy_command_tag(result.status)
            if isinstance(result, str):
                return parse_psqlpy_command_tag(result)
        except Exception as e:
            logger.debug("Failed to parse psqlpy command tag: %s", e)
        return -1

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
            await self._truncate_table_async(table)

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
                    insert_sql = build_psqlpy_insert_statement(table, columns)
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

    async def begin(self) -> None:
        """Begin a database transaction."""
        try:
            await self.connection.execute("BEGIN")
        except psqlpy.exceptions.DatabaseError as e:
            msg = f"Failed to begin psqlpy transaction: {e}"
            raise SQLSpecError(msg) from e

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        try:
            await self.connection.execute("ROLLBACK")
        except psqlpy.exceptions.DatabaseError as e:
            msg = f"Failed to rollback psqlpy transaction: {e}"
            raise SQLSpecError(msg) from e

    async def commit(self) -> None:
        """Commit the current transaction."""
        try:
            await self.connection.execute("COMMIT")
        except psqlpy.exceptions.DatabaseError as e:
            msg = f"Failed to commit psqlpy transaction: {e}"
            raise SQLSpecError(msg) from e

    async def _truncate_table_async(self, table: str) -> None:
        qualified = format_table_identifier(table)
        async with self.handle_database_exceptions(), self.with_cursor(self.connection) as cursor:
            await cursor.execute(f"TRUNCATE TABLE {qualified}")

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction."""
        return bool(self.connection.in_transaction())

    @property
    def data_dictionary(self) -> "AsyncDataDictionaryBase[Self]":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = PsqlpyDataDictionary()
        return self._data_dictionary


_PSQLPY_PROFILE = build_psqlpy_profile()

register_driver_profile("psqlpy", _PSQLPY_PROFILE)
