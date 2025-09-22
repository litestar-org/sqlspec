"""Psqlpy driver implementation for PostgreSQL connectivity.

Provides parameter style conversion, type coercion, error handling,
and transaction management.
"""

import decimal
import re
from typing import TYPE_CHECKING, Any, Final, Optional

import psqlpy
import psqlpy.exceptions

from sqlspec.adapters.psqlpy.type_converter import PostgreSQLTypeConverter
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
    from sqlspec.driver._async import AsyncDataDictionaryBase

__all__ = ("PsqlpyCursor", "PsqlpyDriver", "PsqlpyExceptionHandler", "psqlpy_statement_config")

logger = get_logger("adapters.psqlpy")

_type_converter = PostgreSQLTypeConverter()

psqlpy_statement_config = StatementConfig(
    dialect="postgres",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NUMERIC,
        supported_parameter_styles={ParameterStyle.NUMERIC, ParameterStyle.NAMED_DOLLAR, ParameterStyle.QMARK},
        default_execution_parameter_style=ParameterStyle.NUMERIC,
        supported_execution_parameter_styles={ParameterStyle.NUMERIC},
        type_coercion_map={tuple: list, decimal.Decimal: float, str: _type_converter.convert_if_detected},
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

    __slots__ = ("_data_dictionary",)
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
        self._data_dictionary: Optional[AsyncDataDictionaryBase] = None

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
                formatted_parameters.append(list(param_set))
            else:
                formatted_parameters.append([param_set])

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

    @property
    def data_dictionary(self) -> "AsyncDataDictionaryBase":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            from sqlspec.adapters.psqlpy.data_dictionary import PsqlpyAsyncDataDictionary

            self._data_dictionary = PsqlpyAsyncDataDictionary()
        return self._data_dictionary
