"""Psqlpy driver implementation for PostgreSQL connectivity.

Provides parameter style conversion, type coercion, error handling,
and transaction management.
"""

import decimal
import re
from typing import TYPE_CHECKING, Any, Final

import psqlpy.exceptions
from psqlpy.extra_types import JSONB

from sqlspec.adapters.psqlpy.data_dictionary import PsqlpyAsyncDataDictionary
from sqlspec.adapters.psqlpy.type_converter import PostgreSQLTypeConverter
from sqlspec.core.cache import get_cache_config
from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import SQL, StatementConfig
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

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractAsyncContextManager

    from sqlspec.adapters.psqlpy._types import PsqlpyConnection
    from sqlspec.core.result import SQLResult
    from sqlspec.driver import ExecutionResult
    from sqlspec.driver._async import AsyncDataDictionaryBase

__all__ = ("PsqlpyCursor", "PsqlpyDriver", "PsqlpyExceptionHandler", "psqlpy_statement_config")

logger = get_logger("adapters.psqlpy")

_type_converter = PostgreSQLTypeConverter()


def _convert_decimals_in_structure(obj: Any) -> Any:
    """Recursively convert Decimal values to float in nested structures.

    Psqlpy's Rust layer expects native Python dict/list for JSONB parameters
    (when using CAST(... AS JSONB) in SQL), but cannot handle Decimal objects.
    This function walks through dict/list structures and converts any Decimal
    values to float while preserving the native Python structure.

    Args:
        obj: Object to process (dict, list, or scalar value).

    Returns:
        Object with all Decimal values converted to float, preserving structure.
    """
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: _convert_decimals_in_structure(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_convert_decimals_in_structure(item) for item in obj]
    return obj


def _prepare_dict_parameter(value: "dict[str, Any]") -> dict[str, Any]:
    """Normalize dict parameters while preserving native structures."""
    normalized = _convert_decimals_in_structure(value)
    return normalized if isinstance(normalized, dict) else value


def _prepare_list_parameter(value: "list[Any]") -> list[Any]:
    """Normalize list parameters while preserving native list semantics."""
    return [_convert_decimals_in_structure(item) for item in value]


def _prepare_tuple_parameter(value: "tuple[Any, ...]") -> tuple[Any, ...]:
    """Normalize tuple parameters by deferring to list handling."""
    return tuple(_convert_decimals_in_structure(item) for item in value)


def _normalize_scalar_parameter(value: Any) -> Any:
    """Return scalar value without additional coercion."""
    return value


def _coerce_numeric_for_write(value: Any) -> Any:
    """Convert write parameters to driver-compatible numeric types."""
    if isinstance(value, float):
        return decimal.Decimal(str(value))
    if isinstance(value, decimal.Decimal):
        return value
    if isinstance(value, list):
        return [_coerce_numeric_for_write(item) for item in value]
    if isinstance(value, tuple):
        coerced = [_coerce_numeric_for_write(item) for item in value]
        return tuple(coerced)
    if isinstance(value, dict):
        return {key: _coerce_numeric_for_write(item) for key, item in value.items()}
    return value


def _create_psqlpy_parameter_config(serializer: "Callable[[Any], str]") -> ParameterStyleConfig:
    """Construct parameter config with shared JSON serializer support."""

    base_config = ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NUMERIC,
        supported_parameter_styles={ParameterStyle.NUMERIC, ParameterStyle.NAMED_DOLLAR, ParameterStyle.QMARK},
        default_execution_parameter_style=ParameterStyle.NUMERIC,
        supported_execution_parameter_styles={ParameterStyle.NUMERIC},
        type_coercion_map={decimal.Decimal: float, str: _type_converter.convert_if_detected},
        has_native_list_expansion=False,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_parameter_format=True,
    ).with_json_serializers(serializer)

    updated_type_map = dict(base_config.type_coercion_map)
    updated_type_map[dict] = _prepare_dict_parameter
    updated_type_map[list] = _prepare_list_parameter
    updated_type_map[tuple] = _prepare_tuple_parameter

    return base_config.replace(type_coercion_map=updated_type_map)


psqlpy_statement_config = StatementConfig(
    dialect="postgres",
    parameter_config=_create_psqlpy_parameter_config(to_json),
    enable_parsing=True,
    enable_validation=True,
    enable_caching=True,
    enable_parameter_type_wrapping=True,
)

PSQLPY_STATUS_REGEX: Final[re.Pattern[str]] = re.compile(r"^([A-Z]+)(?:\s+(\d+))?\s+(\d+)$", re.IGNORECASE)


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
    """

    __slots__ = ()

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None:
            return
        if issubclass(exc_type, (psqlpy.exceptions.DatabaseError, psqlpy.exceptions.Error)):
            self._map_postgres_exception(exc_val)

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
        self._data_dictionary: AsyncDataDictionaryBase | None = None

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
            return tuple(_normalize_scalar_parameter(item) for item in prepared)

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
            for idx, param in enumerate(parameters, start=1):
                cast_type = parameter_casts.get(idx, "").upper()
                if cast_type in {"JSON", "JSONB"} and isinstance(param, list):
                    result.append(JSONB(param))
                else:
                    if statement_config.parameter_config.type_coercion_map:
                        for type_check, converter in statement_config.parameter_config.type_coercion_map.items():
                            if isinstance(param, type_check):
                                param = converter(param)
                                break
                    result.append(param)
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

    def handle_database_exceptions(self) -> "AbstractAsyncContextManager[None]":
        """Handle database-specific exceptions.

        Returns:
            Exception handler context manager
        """
        return PsqlpyExceptionHandler()

    async def _try_special_handling(self, cursor: "PsqlpyConnection", statement: SQL) -> "SQLResult | None":
        """Hook for psqlpy-specific special operations.

        Args:
            cursor: Psqlpy connection object
            statement: SQL statement to analyze

        Returns:
            SQLResult if special handling applied, None otherwise
        """
        _ = (cursor, statement)
        return None

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
                values = list(_coerce_numeric_for_write(values))

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
        effective_parameters = _coerce_numeric_for_write(driver_parameters) if should_coerce else driver_parameters

        if statement.returns_rows():
            query_result = await cursor.fetch(sql, effective_parameters or [])
            dict_rows: list[dict[str, Any]] = query_result.result() if query_result else []

            return self.create_execution_result(
                cursor,
                selected_data=dict_rows,
                column_names=list(dict_rows[0].keys()) if dict_rows else [],
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
            if hasattr(result, "tag") and result.tag:
                return self._parse_command_tag(result.tag)
            if hasattr(result, "status") and result.status:
                return self._parse_command_tag(result.status)
            if isinstance(result, str):
                return self._parse_command_tag(result)
        except Exception as e:
            logger.debug("Failed to parse psqlpy command tag: %s", e)
        return -1

    def _parse_command_tag(self, tag: str) -> int:
        """Parse PostgreSQL command tag to extract rows affected.

        Args:
            tag: PostgreSQL command tag string

        Returns:
            Number of rows affected, -1 if unable to parse
        """
        if not tag:
            return -1

        match = PSQLPY_STATUS_REGEX.match(tag.strip())
        if match:
            command = match.group(1).upper()
            if command == "INSERT" and match.group(3):
                return int(match.group(3))
            if command in {"UPDATE", "DELETE"} and match.group(3):
                return int(match.group(3))
        return -1

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

    @property
    def data_dictionary(self) -> "AsyncDataDictionaryBase":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = PsqlpyAsyncDataDictionary()
        return self._data_dictionary
