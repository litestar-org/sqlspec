# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import TYPE_CHECKING, Any, Optional, Union

import asyncmy
import asyncmy.errors
from asyncmy.cursors import Cursor, DictCursor

from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.exceptions import SQLParsingError, SQLSpecError
from sqlspec.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.statement.sql import SQL, StatementConfig

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.adapters.asyncmy._types import AsyncmyConnection
    from sqlspec.driver._common import ExecutionResult
    from sqlspec.statement.result import SQLResult

asyncmy_statement_config = StatementConfig(
    dialect="mysql",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,  # MySQL uses %s
        default_execution_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,  # MySQL uses %s
        supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.POSITIONAL_PYFORMAT},  # Support both ? and %s
        supported_execution_parameter_styles={ParameterStyle.POSITIONAL_PYFORMAT},  # Only %s for execution
        type_coercion_map={},
        has_native_list_expansion=False,
        needs_static_script_compilation=True,
        preserve_parameter_format=True,  # AsyncMy needs exact tuple/list format preservation
    ),
)

__all__ = ("AsyncmyCursor", "AsyncmyDriver", "asyncmy_statement_config")


class AsyncmyCursor:
    def __init__(self, connection: "AsyncmyConnection") -> None:
        self.connection = connection
        self.cursor: Optional[Union[Cursor, DictCursor]] = None

    async def __aenter__(self) -> Union[Cursor, DictCursor]:
        self.cursor = self.connection.cursor()
        return self.cursor

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.cursor:
            await self.cursor.close()


class AsyncmyDriver(AsyncDriverAdapterBase):
    """Asyncmy MySQL/MariaDB Driver Adapter. Modern protocol implementation."""

    dialect: "DialectType" = "mysql"

    def __init__(
        self,
        connection: "AsyncmyConnection",
        statement_config: Optional[StatementConfig] = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        super().__init__(
            connection=connection,
            statement_config=statement_config or asyncmy_statement_config,
            driver_features=driver_features,
        )

    async def begin(self) -> None:
        """Begin a transaction.

        MySQL/AsyncMy starts transactions automatically with the first command.
        """

    async def commit(self) -> None:
        """Commit the current transaction."""
        await self.connection.commit()

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        await self.connection.rollback()

    def with_cursor(self, connection: "AsyncmyConnection") -> "AsyncmyCursor":
        return AsyncmyCursor(connection)

    async def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[SQLResult]":
        """Handle AsyncMy-specific operations.

        AsyncMy doesn't have special operations, so this always returns None.
        """
        return None

    async def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """AsyncMy executemany implementation."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        await cursor.executemany(sql, prepared_parameters)

        return self.create_execution_result(
            cursor, rowcount_override=len(prepared_parameters) if prepared_parameters else 0, is_many_result=True
        )

    async def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """AsyncMy single execution."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        await cursor.execute(sql, prepared_parameters or None)

        if statement.returns_rows():
            data = await cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description or []]
            if data and not isinstance(data[0], dict):
                data = [dict(zip(column_names, row)) for row in data]

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        row_count = cursor.rowcount if cursor.rowcount is not None else -1
        return self.create_execution_result(cursor, rowcount_override=row_count)

    def handle_database_exceptions(self) -> "AbstractAsyncContextManager[None]":
        """Handle AsyncMy-specific exceptions and wrap them appropriately."""

        @asynccontextmanager
        async def _async_handle_database_exceptions() -> Any:
            try:
                yield
            except asyncmy.errors.MySQLError as e:
                msg = f"AsyncMy database error: {e}"
                raise SQLSpecError(msg) from e
            except Exception as e:
                # Handle any other unexpected errors
                if "parse" in str(e).lower() or "syntax" in str(e).lower():
                    msg = f"SQL parsing failed: {e}"
                    raise SQLParsingError(msg) from e
                msg = f"Unexpected error: {e}"
                raise SQLSpecError(msg) from e

        return _async_handle_database_exceptions()
