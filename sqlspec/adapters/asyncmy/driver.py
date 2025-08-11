"""AsyncMy MySQL driver implementation for async MySQL operations.

Provides async MySQL/MariaDB connectivity with:
- Parameter style conversion (QMARK to POSITIONAL_PYFORMAT)
- MySQL-specific type coercion and data handling
- Error categorization for MySQL/MariaDB
- Transaction management
"""

import logging
from typing import TYPE_CHECKING, Any, Optional, Union

import asyncmy
import asyncmy.errors
from asyncmy.cursors import Cursor, DictCursor

from sqlspec.core.cache import get_cache_config
from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import StatementConfig
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.exceptions import SQLParsingError, SQLSpecError
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager

    from sqlspec.adapters.asyncmy._types import AsyncmyConnection
    from sqlspec.core.result import SQLResult
    from sqlspec.core.statement import SQL
    from sqlspec.driver import ExecutionResult

logger = logging.getLogger(__name__)

__all__ = ("AsyncmyCursor", "AsyncmyDriver", "AsyncmyExceptionHandler", "asyncmy_statement_config")


# Enhanced AsyncMy statement configuration using core modules with performance optimizations
asyncmy_statement_config = StatementConfig(
    dialect="mysql",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.QMARK,
        supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.POSITIONAL_PYFORMAT},
        default_execution_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
        supported_execution_parameter_styles={ParameterStyle.POSITIONAL_PYFORMAT},
        type_coercion_map={
            dict: to_json,
            list: to_json,
            tuple: lambda v: to_json(list(v)),
            bool: int,  # MySQL represents booleans as integers
        },
        has_native_list_expansion=False,
        needs_static_script_compilation=True,
        preserve_parameter_format=True,
    ),
    enable_parsing=True,
    enable_validation=True,
    enable_caching=True,
    enable_parameter_type_wrapping=True,
)


class AsyncmyCursor:
    """Async context manager for AsyncMy cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "AsyncmyConnection") -> None:
        self.connection = connection
        self.cursor: Optional[Union[Cursor, DictCursor]] = None

    async def __aenter__(self) -> Union[Cursor, DictCursor]:
        self.cursor = self.connection.cursor()
        return self.cursor

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = (exc_type, exc_val, exc_tb)
        if self.cursor is not None:
            await self.cursor.close()


class AsyncmyExceptionHandler:
    """Custom async context manager for handling AsyncMy database exceptions."""

    __slots__ = ()

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None:
            return

        if issubclass(exc_type, asyncmy.errors.IntegrityError):
            e = exc_val
            msg = f"AsyncMy MySQL integrity constraint violation: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, asyncmy.errors.ProgrammingError):
            e = exc_val
            error_msg = str(e).lower()
            if "syntax" in error_msg or "parse" in error_msg:
                msg = f"AsyncMy MySQL SQL syntax error: {e}"
                raise SQLParsingError(msg) from e
            msg = f"AsyncMy MySQL programming error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, asyncmy.errors.OperationalError):
            e = exc_val
            msg = f"AsyncMy MySQL operational error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, asyncmy.errors.DatabaseError):
            e = exc_val
            msg = f"AsyncMy MySQL database error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, asyncmy.errors.Error):
            e = exc_val
            msg = f"AsyncMy MySQL error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, Exception):
            e = exc_val
            error_msg = str(e).lower()
            if "parse" in error_msg or "syntax" in error_msg:
                msg = f"SQL parsing failed: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Unexpected async database operation error: {e}"
            raise SQLSpecError(msg) from e


class AsyncmyDriver(AsyncDriverAdapterBase):
    """AsyncMy MySQL/MariaDB driver for async database operations.

    Provides MySQL/MariaDB connectivity with:
    - Parameter style conversion (QMARK to POSITIONAL_PYFORMAT)
    - MySQL-specific type coercion (bool -> int, dict/list -> JSON)
    - Error categorization for MySQL/MariaDB
    - Transaction management
    """

    __slots__ = ()
    dialect = "mysql"

    def __init__(
        self,
        connection: "AsyncmyConnection",
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        if statement_config is None:
            cache_config = get_cache_config()
            enhanced_config = asyncmy_statement_config.replace(
                enable_caching=cache_config.compiled_cache_enabled,
                enable_parsing=True,
                enable_validation=True,
                dialect="mysql",
            )
            statement_config = enhanced_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: "AsyncmyConnection") -> "AsyncmyCursor":
        """Create async context manager for AsyncMy cursor."""
        return AsyncmyCursor(connection)

    def handle_database_exceptions(self) -> "AbstractAsyncContextManager[None]":
        """Handle database-specific exceptions and wrap them appropriately."""
        return AsyncmyExceptionHandler()

    async def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[SQLResult]":
        """Hook for AsyncMy-specific special operations.

        Args:
            cursor: AsyncMy cursor object
            statement: SQL statement to analyze

        Returns:
            None - always proceeds with standard execution for AsyncMy
        """
        _ = (cursor, statement)  # Mark as intentionally unused
        return None

    async def _execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL script using enhanced statement splitting and parameter handling.

        Uses core module optimization for statement parsing and parameter processing.
        Parameters are embedded as static values for script execution compatibility.
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_cursor = cursor

        for stmt in statements:
            await cursor.execute(stmt, prepared_parameters or None)
            successful_count += 1

        return self.create_execution_result(
            last_cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    async def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using optimized AsyncMy batch processing.

        Leverages core parameter processing for enhanced MySQL type handling and parameter conversion.
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        # Enhanced parameter validation for executemany
        if not prepared_parameters:
            msg = "execute_many requires parameters"
            raise ValueError(msg)

        await cursor.executemany(sql, prepared_parameters)

        # Calculate affected rows based on parameter count for AsyncMy
        affected_rows = len(prepared_parameters) if prepared_parameters else 0

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    async def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement with enhanced AsyncMy MySQL data handling and performance optimization.

        Uses core processing for optimal parameter handling and MySQL result processing.
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        await cursor.execute(sql, prepared_parameters or None)

        # Enhanced SELECT result processing for MySQL
        if statement.returns_rows():
            fetched_data = await cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description or []]

            # AsyncMy may return tuples or dicts - ensure consistent dict format
            if fetched_data and not isinstance(fetched_data[0], dict):
                data = [dict(zip(column_names, row)) for row in fetched_data]
            else:
                data = fetched_data

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        # Enhanced non-SELECT result processing for MySQL
        affected_rows = cursor.rowcount if cursor.rowcount is not None else -1
        last_id = getattr(cursor, "lastrowid", None) if cursor.rowcount and cursor.rowcount > 0 else None
        return self.create_execution_result(cursor, rowcount_override=affected_rows, last_inserted_id=last_id)

    # MySQL transaction management with enhanced async error handling
    async def begin(self) -> None:
        """Begin a database transaction with enhanced async error handling.

        Explicitly starts a MySQL transaction to ensure proper transaction boundaries.
        """
        try:
            # Execute explicit BEGIN to start transaction
            async with AsyncmyCursor(self.connection) as cursor:
                await cursor.execute("BEGIN")
        except asyncmy.errors.MySQLError as e:
            msg = f"Failed to begin MySQL transaction: {e}"
            raise SQLSpecError(msg) from e

    async def rollback(self) -> None:
        """Rollback the current transaction with enhanced async error handling."""
        try:
            await self.connection.rollback()
        except asyncmy.errors.MySQLError as e:
            msg = f"Failed to rollback MySQL transaction: {e}"
            raise SQLSpecError(msg) from e

    async def commit(self) -> None:
        """Commit the current transaction with enhanced async error handling."""
        try:
            await self.connection.commit()
        except asyncmy.errors.MySQLError as e:
            msg = f"Failed to commit MySQL transaction: {e}"
            raise SQLSpecError(msg) from e
