# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import contextlib
import datetime
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Optional

import aiosqlite

from sqlspec.driver import AsyncDriverAdapterBase, ExecutionResult
from sqlspec.exceptions import SQLParsingError, SQLSpecError
from sqlspec.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.statement import StatementConfig
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from sqlspec.adapters.aiosqlite._types import AiosqliteConnection
    from sqlspec.statement.result import SQLResult
    from sqlspec.statement.sql import SQL

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
        super().__init__(
            connection=connection,
            statement_config=statement_config or aiosqlite_statement_config,
            driver_features=driver_features,
        )

    def with_cursor(self, connection: "AiosqliteConnection") -> "AiosqliteCursor":
        return AiosqliteCursor(connection)

    async def _try_special_handling(self, cursor: "aiosqlite.Cursor", statement: "Any") -> "Optional[SQLResult]":
        """Handle AioSQLite-specific operations.

        AioSQLite doesn't have special operations, so this always returns None.

        Args:
            cursor: AioSQLite cursor object
            statement: SQL statement to analyze

        Returns:
            None - proceeds with standard execution
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

    async def _execute_script(self, cursor: "aiosqlite.Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute SQL script by splitting and executing statements individually."""
        sql = statement.sql
        prepared_parameters = statement.parameters
        statement_config = statement.statement_config
        statements = self.split_script_statements(sql, statement_config, strip_trailing_semicolon=True)

        last_result = None
        for stmt in statements:
            last_result = await cursor.execute(stmt, prepared_parameters or ())

        return self.create_execution_result(
            last_result, statement_count=len(statements), successful_statements=len(statements), is_script_result=True
        )

    async def _execute_many(self, cursor: "aiosqlite.Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using aiosqlite executemany."""
        sql = statement.sql
        prepared_parameters = statement.parameters
        result = await cursor.executemany(sql, prepared_parameters)
        return self.create_execution_result(result, rowcount_override=cursor.rowcount or 0, is_many_result=True)

    async def _execute_statement(self, cursor: "aiosqlite.Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement using aiosqlite execute."""
        sql = statement.sql
        prepared_parameters = statement.parameters
        result = await cursor.execute(sql, prepared_parameters or ())

        if statement.returns_rows():
            fetched_data = await cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]
            data = [dict(row) for row in fetched_data]

            return self.create_execution_result(
                result, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        return self.create_execution_result(result, rowcount_override=cursor.rowcount or 0)

    def handle_database_exceptions(self) -> "AbstractAsyncContextManager[None]":
        """Handle AioSQLite-specific exceptions and wrap them appropriately."""
        return self._handle_database_exceptions_async()

    @asynccontextmanager
    async def _handle_database_exceptions_async(self) -> Any:
        try:
            yield
        except aiosqlite.Error as e:
            msg = f"AioSQLite database error: {e}"
            raise SQLSpecError(msg) from e
        except Exception as e:
            # Handle any other unexpected errors
            if "parse" in str(e).lower() or "syntax" in str(e).lower():
                msg = f"SQL parsing failed: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Unexpected error: {e}"
            raise SQLSpecError(msg) from e
