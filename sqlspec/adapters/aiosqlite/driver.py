"""AIOSQLite driver implementation for async SQLite operations.

Provides async SQLite database connectivity with:
- Async parameter processing with type coercion
- Thread-safe caching system
- Context management for resource handling
- SQLite-specific optimizations
"""

import contextlib
import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Optional

import aiosqlite

from sqlspec.core.cache import get_cache_config
from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import StatementConfig
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.exceptions import SQLParsingError, SQLSpecError
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager

    from sqlspec.adapters.aiosqlite._types import AiosqliteConnection
    from sqlspec.core.result import SQLResult
    from sqlspec.core.statement import SQL
    from sqlspec.driver import ExecutionResult

__all__ = ("AiosqliteCursor", "AiosqliteDriver", "AiosqliteExceptionHandler", "aiosqlite_statement_config")


aiosqlite_statement_config = StatementConfig(
    dialect="sqlite",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.QMARK,
        supported_parameter_styles={ParameterStyle.QMARK},
        default_execution_parameter_style=ParameterStyle.QMARK,
        supported_execution_parameter_styles={ParameterStyle.QMARK},
        type_coercion_map={
            bool: int,
            datetime.datetime: lambda v: v.isoformat(),
            datetime.date: lambda v: v.isoformat(),
            Decimal: str,
            dict: to_json,
            list: to_json,
            tuple: lambda v: to_json(list(v)),
        },
        has_native_list_expansion=False,
        needs_static_script_compilation=False,
        preserve_parameter_format=True,
    ),
    enable_parsing=True,
    enable_validation=True,
    enable_caching=True,
    enable_parameter_type_wrapping=True,
)


class AiosqliteCursor:
    """Async context manager for AIOSQLite cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "AiosqliteConnection") -> None:
        self.connection = connection
        self.cursor: Optional[aiosqlite.Cursor] = None

    async def __aenter__(self) -> "aiosqlite.Cursor":
        self.cursor = await self.connection.cursor()
        return self.cursor

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = (exc_type, exc_val, exc_tb)
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                await self.cursor.close()


class AiosqliteExceptionHandler:
    """Custom async context manager for handling AIOSQLite database exceptions."""

    __slots__ = ()

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None:
            return
        if issubclass(exc_type, aiosqlite.IntegrityError):
            e = exc_val
            msg = f"AIOSQLite integrity constraint violation: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, aiosqlite.OperationalError):
            e = exc_val
            error_msg = str(e).lower()
            if "locked" in error_msg:
                msg = f"AIOSQLite database locked: {e}. Consider enabling WAL mode or reducing concurrency."
                raise SQLSpecError(msg) from e
            if "syntax" in error_msg or "malformed" in error_msg:
                msg = f"AIOSQLite SQL syntax error: {e}"
                raise SQLParsingError(msg) from e
            msg = f"AIOSQLite operational error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, aiosqlite.DatabaseError):
            e = exc_val
            msg = f"AIOSQLite database error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, aiosqlite.Error):
            e = exc_val
            msg = f"AIOSQLite error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, Exception):
            e = exc_val
            error_msg = str(e).lower()
            if "parse" in error_msg or "syntax" in error_msg:
                msg = f"SQL parsing failed: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Unexpected async database operation error: {e}"
            raise SQLSpecError(msg) from e


class AiosqliteDriver(AsyncDriverAdapterBase):
    """AIOSQLite driver for async SQLite database operations.

    Provides async SQLite connectivity with:
    - Statement processing and parameter handling
    - Cursor management and resource cleanup
    - Exception handling for SQLite operations
    """

    __slots__ = ()
    dialect = "sqlite"

    def __init__(
        self,
        connection: "AiosqliteConnection",
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        if statement_config is None:
            cache_config = get_cache_config()
            enhanced_config = aiosqlite_statement_config.replace(
                enable_caching=cache_config.compiled_cache_enabled,
                enable_parsing=True,
                enable_validation=True,
                dialect="sqlite",
            )
            statement_config = enhanced_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: "AiosqliteConnection") -> "AiosqliteCursor":
        """Create async context manager for AIOSQLite cursor."""
        return AiosqliteCursor(connection)

    def handle_database_exceptions(self) -> "AbstractAsyncContextManager[None]":
        """Handle AIOSQLite-specific exceptions."""
        return AiosqliteExceptionHandler()

    async def _try_special_handling(self, cursor: "aiosqlite.Cursor", statement: "SQL") -> "Optional[SQLResult]":
        """Hook for AIOSQLite-specific special operations.

        Args:
            cursor: AIOSQLite cursor object
            statement: SQL statement to analyze

        Returns:
            None - always proceeds with standard execution for AIOSQLite
        """
        _ = (cursor, statement)
        return None

    async def _execute_script(self, cursor: "aiosqlite.Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute SQL script using statement splitting and parameter handling."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_cursor = cursor

        for stmt in statements:
            await cursor.execute(stmt, prepared_parameters or ())
            successful_count += 1

        return self.create_execution_result(
            last_cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    async def _execute_many(self, cursor: "aiosqlite.Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using async batch processing."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if not prepared_parameters:
            msg = "execute_many requires parameters"
            raise ValueError(msg)

        await cursor.executemany(sql, prepared_parameters)

        affected_rows = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    async def _execute_statement(self, cursor: "aiosqlite.Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement with async data handling."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        await cursor.execute(sql, prepared_parameters or ())

        if statement.returns_rows():
            fetched_data = await cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]

            data = [dict(zip(column_names, row)) for row in fetched_data]

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        affected_rows = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
        return self.create_execution_result(cursor, rowcount_override=affected_rows)

    async def begin(self) -> None:
        """Begin a database transaction."""
        try:
            if not self.connection.in_transaction:
                await self.connection.execute("BEGIN")
        except aiosqlite.Error as e:
            msg = f"Failed to begin transaction: {e}"
            raise SQLSpecError(msg) from e

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        try:
            await self.connection.rollback()
        except aiosqlite.Error as e:
            msg = f"Failed to rollback transaction: {e}"
            raise SQLSpecError(msg) from e

    async def commit(self) -> None:
        """Commit the current transaction."""
        try:
            await self.connection.commit()
        except aiosqlite.Error as e:
            msg = f"Failed to commit transaction: {e}"
            raise SQLSpecError(msg) from e
