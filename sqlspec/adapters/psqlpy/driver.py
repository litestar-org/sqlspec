"""Psqlpy driver implementation for PostgreSQL connectivity.

Provides parameter style conversion, type coercion, error handling,
and transaction management.
"""

import datetime
import decimal
import re
import uuid
from typing import TYPE_CHECKING, Any, Final, Optional

import psqlpy
import psqlpy.exceptions

from sqlspec.core.cache import get_cache_config
from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import SQL, StatementConfig
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.exceptions import SQLParsingError, SQLSpecError
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager

    from sqlspec.adapters.psqlpy._types import PsqlpyConnection
    from sqlspec.core.result import SQLResult
    from sqlspec.driver import ExecutionResult

__all__ = ("PsqlpyCursor", "PsqlpyDriver", "PsqlpyExceptionHandler", "psqlpy_statement_config")

logger = get_logger("adapters.psqlpy")

psqlpy_statement_config = StatementConfig(
    dialect="postgres",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NUMERIC,
        supported_parameter_styles={ParameterStyle.NUMERIC, ParameterStyle.NAMED_DOLLAR, ParameterStyle.QMARK},
        default_execution_parameter_style=ParameterStyle.NUMERIC,
        supported_execution_parameter_styles={ParameterStyle.NUMERIC},
        type_coercion_map={tuple: list, decimal.Decimal: float},
        has_native_list_expansion=False,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_parameter_format=True,
    ),
    enable_parsing=True,
    enable_validation=True,
    enable_caching=True,
    enable_parameter_type_wrapping=True,
)

PSQLPY_STATUS_REGEX: Final[re.Pattern[str]] = re.compile(r"^([A-Z]+)(?:\s+(\d+))?\s+(\d+)$", re.IGNORECASE)

SPECIAL_TYPE_REGEX: Final[re.Pattern[str]] = re.compile(
    r"^(?:"
    r"(?P<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})|"
    r"(?P<ipv4>(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(?:/(?:3[0-2]|[12]?[0-9]))?)|"
    r"(?P<ipv6>(?:(?:[0-9a-f]{1,4}:){7}[0-9a-f]{1,4}|(?:[0-9a-f]{1,4}:){1,7}:|:(?::[0-9a-f]{1,4}){1,7}|(?:[0-9a-f]{1,4}:){1,6}:[0-9a-f]{1,4}|::(?:ffff:)?(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))(?:/(?:12[0-8]|1[01][0-9]|[1-9]?[0-9]))?)|"
    r"(?P<mac>(?:[0-9a-f]{2}[:-]){5}[0-9a-f]{2}|[0-9a-f]{12})|"
    r"(?P<iso_datetime>\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:?\d{2})?)|"
    r"(?P<iso_date>\d{4}-\d{2}-\d{2})|"
    r"(?P<iso_time>\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:?\d{2})?)|"
    r"(?P<interval>(?:(?:\d+\s+(?:year|month|day|hour|minute|second)s?\s*)+)|(?:P(?:\d+Y)?(?:\d+M)?(?:\d+D)?(?:T(?:\d+H)?(?:\d+M)?(?:\d+(?:\.\d+)?S)?)?))|"
    r"(?P<json>\{[\s\S]*\}|\[[\s\S]*\])|"
    r"(?P<pg_array>\{(?:[^{}]+|\{[^{}]*\})*\})"
    r")$",
    re.IGNORECASE,
)


def _detect_postgresql_type(value: str) -> Optional[str]:
    """Detect PostgreSQL data type from string value.

    Args:
        value: String value to analyze

    Returns:
        Type name if detected, None otherwise.
    """
    match = SPECIAL_TYPE_REGEX.match(value)
    if not match:
        return None

    for group_name in [
        "uuid",
        "ipv4",
        "ipv6",
        "mac",
        "iso_datetime",
        "iso_date",
        "iso_time",
        "interval",
        "json",
        "pg_array",
    ]:
        if match.group(group_name):
            return group_name

    return None


def _convert_uuid(value: str) -> Any:
    """Convert UUID string to UUID object.

    Args:
        value: UUID string to convert

    Returns:
        UUID object or original value if conversion fails
    """
    try:
        clean_uuid = value.replace("-", "").lower()
        uuid_length = 32
        if len(clean_uuid) == uuid_length:
            formatted = f"{clean_uuid[:8]}-{clean_uuid[8:12]}-{clean_uuid[12:16]}-{clean_uuid[16:20]}-{clean_uuid[20:]}"
            return uuid.UUID(formatted)
        return uuid.UUID(value)
    except (ValueError, AttributeError):
        return value


def _convert_iso_datetime(value: str) -> Any:
    """Convert ISO datetime string to datetime object.

    Args:
        value: ISO datetime string to convert

    Returns:
        datetime object or original value if conversion fails
    """
    try:
        normalized = value.replace("Z", "+00:00")
        return datetime.datetime.fromisoformat(normalized)
    except ValueError:
        return value


def _convert_iso_date(value: str) -> Any:
    """Convert ISO date string to date object.

    Args:
        value: ISO date string to convert

    Returns:
        date object or original value if conversion fails
    """
    try:
        return datetime.date.fromisoformat(value)
    except ValueError:
        return value


def _validate_json(value: str) -> str:
    """Validate JSON string format.

    Args:
        value: JSON string to validate

    Returns:
        Original string value
    """
    from sqlspec.utils.serializers import from_json

    try:
        from_json(value)
    except (ValueError, TypeError):
        return value
    return value


def _passthrough(value: str) -> str:
    """Pass value through unchanged.

    Args:
        value: String value to pass through

    Returns:
        Original value unchanged
    """
    return value


_PSQLPY_TYPE_CONVERTERS: dict[str, Any] = {
    "uuid": _convert_uuid,
    "iso_datetime": _convert_iso_datetime,
    "iso_date": _convert_iso_date,
    "iso_time": _passthrough,
    "json": _validate_json,
    "pg_array": _passthrough,
    "ipv4": _passthrough,
    "ipv6": _passthrough,
    "mac": _passthrough,
    "interval": _passthrough,
}


def _convert_psqlpy_parameters(value: Any) -> Any:
    """Convert parameters for psqlpy compatibility.

    Args:
        value: Parameter value to convert

    Returns:
        Converted value suitable for psqlpy execution
    """
    if isinstance(value, str):
        detected_type = _detect_postgresql_type(value)

        if detected_type:
            converter = _PSQLPY_TYPE_CONVERTERS.get(detected_type)
            if converter:
                return converter(value)

        return value

    if isinstance(value, (dict, list, tuple, uuid.UUID, datetime.datetime, datetime.date)):
        return value

    return value


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

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit cursor context.

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Exception traceback
        """
        _ = (exc_type, exc_val, exc_tb)
        self._in_use = False

    def is_in_use(self) -> bool:
        """Check if cursor is currently in use.

        Returns:
            True if cursor is in use, False otherwise
        """
        return self._in_use


class PsqlpyExceptionHandler:
    """Async context manager for handling psqlpy database exceptions."""

    __slots__ = ()

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None:
            return

        if issubclass(exc_type, psqlpy.exceptions.DatabaseError):
            e = exc_val
            msg = f"Psqlpy database error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, psqlpy.exceptions.InterfaceError):
            e = exc_val
            msg = f"Psqlpy interface error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, psqlpy.exceptions.Error):
            e = exc_val
            error_msg = str(e).lower()
            if "syntax" in error_msg or "parse" in error_msg:
                msg = f"Psqlpy SQL syntax error: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Psqlpy error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, Exception):
            e = exc_val
            error_msg = str(e).lower()
            if "parse" in error_msg or "syntax" in error_msg:
                msg = f"SQL parsing failed: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Unexpected async database operation error: {e}"
            raise SQLSpecError(msg) from e


class PsqlpyDriver(AsyncDriverAdapterBase):
    """PostgreSQL driver implementation using psqlpy.

    Provides parameter style conversion, type coercion, error handling,
    and transaction management.
    """

    __slots__ = ()
    dialect = "postgres"

    def __init__(
        self,
        connection: "PsqlpyConnection",
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        if statement_config is None:
            cache_config = get_cache_config()
            statement_config = psqlpy_statement_config.replace(
                enable_caching=cache_config.compiled_cache_enabled,
                enable_parsing=True,
                enable_validation=True,
                dialect="postgres",
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

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

    async def _try_special_handling(self, cursor: "PsqlpyConnection", statement: SQL) -> "Optional[SQLResult]":
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
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statement_config = statement.statement_config

        if not prepared_parameters:
            await cursor.execute_batch(sql)
            statements = self.split_script_statements(sql, statement_config, strip_trailing_semicolon=True)
            return self.create_execution_result(
                cursor, statement_count=len(statements), successful_statements=len(statements), is_script_result=True
            )
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

        formatted_parameters = []
        for param_set in prepared_parameters:
            if isinstance(param_set, (list, tuple)):
                converted_params = [_convert_psqlpy_parameters(param) for param in param_set]
                formatted_parameters.append(converted_params)
            else:
                formatted_parameters.append([_convert_psqlpy_parameters(param_set)])

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

        if prepared_parameters:
            prepared_parameters = [_convert_psqlpy_parameters(param) for param in prepared_parameters]

        if statement.returns_rows():
            query_result = await cursor.fetch(sql, prepared_parameters or [])
            dict_rows: list[dict[str, Any]] = query_result.result() if query_result else []

            return self.create_execution_result(
                cursor,
                selected_data=dict_rows,
                column_names=list(dict_rows[0].keys()) if dict_rows else [],
                data_row_count=len(dict_rows),
                is_select_result=True,
            )

        result = await cursor.execute(sql, prepared_parameters or [])
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
