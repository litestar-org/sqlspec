# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import contextlib
import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Optional

import aiosqlite

from sqlspec.driver import AsyncDriverAdapterBase, ExecutionResult
from sqlspec.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.statement import StatementConfig
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from sqlspec.adapters.aiosqlite._types import AiosqliteConnection
    from sqlspec.statement.result import SQLResult

# Shared AIOSQLite statement configuration
aiosqlite_statement_config = StatementConfig(
    dialect="sqlite",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.QMARK,
        supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_COLON},
        type_coercion_map={
            bool: int,
            datetime.datetime: lambda v: v.isoformat(),
            Decimal: str,
            dict: to_json,
            list: to_json,
            tuple: lambda v: to_json(list(v)),
        },
        has_native_list_expansion=False,
        needs_static_script_compilation=False,
    ),
)

__all__ = ("AiosqliteCursor", "AiosqliteDriver", "aiosqlite_statement_config")


class AiosqliteCursor:
    def __init__(self, connection: "AiosqliteConnection") -> None:
        self.connection = connection
        self.cursor: Optional[aiosqlite.Cursor] = None

    async def __aenter__(self) -> aiosqlite.Cursor:
        self.connection.row_factory = aiosqlite.Row
        self.cursor = await self.connection.cursor()
        return self.cursor

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.cursor:
            await self.cursor.close()


class AiosqliteDriver(AsyncDriverAdapterBase):
    """Reference implementation for an asynchronous aiosqlite driver."""

    dialect = "sqlite"

    def __init__(
        self,
        connection: "AiosqliteConnection",
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        # Set default aiosqlite-specific configuration
        if statement_config is None:
            statement_config = aiosqlite_statement_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: "AiosqliteConnection") -> "AiosqliteCursor":
        return AiosqliteCursor(connection)

    async def _try_special_handling(self, cursor: "aiosqlite.Cursor", statement: "Any") -> "Optional[SQLResult]":
        """Hook for AioSQLite-specific special operations.

        AioSQLite doesn't have special operations like PostgreSQL COPY,
        so this always returns None to proceed with standard execution.

        Args:
            cursor: AioSQLite cursor object
            statement: SQL statement to analyze

        Returns:
            None - always proceeds with standard execution
        """
        return None

    async def begin(self) -> None:
        """Begin a database transaction."""
        with contextlib.suppress(Exception):
            await self.connection.execute("BEGIN")

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        await self.connection.rollback()

    async def commit(self) -> None:
        """Commit the current transaction."""
        await self.connection.commit()

    async def _execute_script(
        self, cursor: "aiosqlite.Cursor", sql: str, prepared_params: Any, statement_config: "StatementConfig"
    ) -> "ExecutionResult":
        """Execute SQL script by splitting and executing statements individually.

        AioSQLite doesn't have executescript but supports parameters in execute() calls.
        """
        statements = self.split_script_statements(sql, statement_config, strip_trailing_semicolon=True)

        last_result = None
        for stmt in statements:
            last_result = await cursor.execute(stmt, prepared_params or ())

        # Get row count if available
        try:
            row_count = self._get_row_count(cursor)
        except Exception:
            row_count = None

        return self.create_execution_result(
            last_result,
            statement_count=len(statements),
            successful_statements=len(statements),
            rowcount_override=row_count,
            is_script_result=True,
        )

    async def _execute_many(self, cursor: "aiosqlite.Cursor", sql: str, prepared_params: Any) -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using aiosqlite executemany."""
        result = await cursor.executemany(sql, prepared_params)

        # Get row count if available
        try:
            row_count = self._get_row_count(cursor)
        except Exception:
            row_count = None

        return self.create_execution_result(result, rowcount_override=row_count, is_many_result=True)

    async def _execute_statement(self, cursor: "aiosqlite.Cursor", sql: str, prepared_params: Any) -> "ExecutionResult":
        """Execute single SQL statement using aiosqlite execute."""
        result = await cursor.execute(sql, prepared_params or ())

        # Get row count if available
        try:
            row_count = self._get_row_count(cursor)
        except Exception:
            row_count = None

        return self.create_execution_result(result, rowcount_override=row_count)

    async def _get_selected_data(self, cursor: "aiosqlite.Cursor") -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution."""
        fetched_data = await cursor.fetchall()
        column_names = [col[0] for col in cursor.description or []]
        data = [dict(row) for row in fetched_data]
        return data, column_names, len(data)

    def _get_row_count(self, cursor: "aiosqlite.Cursor") -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        return cursor.rowcount or 0
