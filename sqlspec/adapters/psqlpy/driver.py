"""Psqlpy Driver Implementation."""
# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false

from typing import TYPE_CHECKING, Any, Optional

from sqlspec.adapters.psqlpy._types import PsqlpyConnection
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.parameters import ParameterStyle
from sqlspec.parameters.config import ParameterStyleConfig
from sqlspec.statement.sql import StatementConfig

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.driver._common import ExecutionResult
    from sqlspec.statement.result import SQLResult
    from sqlspec.statement.sql import SQL

# Shared PsqlPy statement configuration
psqlpy_statement_config = StatementConfig(
    dialect="postgres",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NUMERIC,
        supported_parameter_styles={ParameterStyle.NUMERIC, ParameterStyle.NAMED_DOLLAR},
        type_coercion_map={},
        has_native_list_expansion=True,
        needs_static_script_compilation=False,
    ),
)

__all__ = ("PsqlpyCursor", "PsqlpyDriver", "psqlpy_statement_config")


class PsqlpyCursor:
    def __init__(self, connection: "PsqlpyConnection") -> None:
        self.connection = connection

    async def __aenter__(self) -> "PsqlpyConnection":
        return self.connection

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass


class PsqlpyDriver(AsyncDriverAdapterBase):
    """Psqlpy Driver Adapter."""

    dialect: "DialectType" = "postgres"
    connection: "PsqlpyConnection"

    def __init__(
        self,
        connection: PsqlpyConnection,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        # Set default psqlpy-specific configuration
        if statement_config is None:
            statement_config = psqlpy_statement_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: PsqlpyConnection) -> "PsqlpyCursor":
        return PsqlpyCursor(connection)

    async def _try_special_handling(self, cursor: PsqlpyConnection, statement: "SQL") -> "Optional[SQLResult]":
        """Hook for PsqlPy-specific special operations.

        PsqlPy doesn't have special operations like PostgreSQL COPY,
        so this always returns None to proceed with standard execution.
        """
        return None

    async def _execute_many(
        self, cursor: PsqlpyConnection, sql: str, prepared_params: Any, statement: "SQL"
    ) -> "ExecutionResult":
        """PsqlPy executemany implementation."""
        await cursor.execute_many(sql, prepared_params)

        # PsqlPy doesn't easily expose rowcount, so we use -1 to indicate unavailable row count
        return self.create_execution_result(cursor, rowcount_override=-1, is_many_result=True)

    async def _execute_script(
        self,
        cursor: PsqlpyConnection,
        sql: str,
        prepared_params: Any,
        statement_config: "StatementConfig",
        statement: "SQL",
    ) -> "ExecutionResult":
        """Execute SQL script by splitting and executing statements sequentially.

        PsqlPy doesn't have executescript but supports parameters in all statements.
        """
        statements = self.split_script_statements(sql, statement_config, strip_trailing_semicolon=True)
        statement_count = len(statements)  # Script splitter already filters empty statements

        last_result = None
        for stmt in statements:
            last_result = await cursor.execute(stmt, prepared_params or ())

        return self.create_execution_result(
            last_result,
            statement_count=statement_count,
            successful_statements=statement_count,  # Assume all successful if no exception
            is_script_result=True,
        )

    async def _execute_statement(
        self, cursor: PsqlpyConnection, sql: str, prepared_params: Any, statement: "SQL"
    ) -> "ExecutionResult":
        """Execute single SQL statement using PsqlPy-optimized approach."""
        if statement.returns_rows():
            # For PsqlPy, we need to use fetch() instead of execute() for SELECT queries
            query_result = await cursor.fetch(sql, prepared_params)
            dict_rows: list[dict[str, Any]] = []
            if query_result:
                dict_rows = query_result.result()

            column_names = list(dict_rows[0].keys()) if dict_rows else []
            return self.create_execution_result(
                cursor,
                selected_data=dict_rows,
                column_names=column_names,
                data_row_count=len(dict_rows),
                is_select_result=True,
            )

        # Use execute() for non-row-returning queries
        await cursor.execute(sql, prepared_params)

        # PsqlPy doesn't easily expose rowcount, so we use -1 to indicate unavailable row count
        return self.create_execution_result(cursor, rowcount_override=-1)

    async def begin(self) -> None:
        """Begin transaction using psqlpy-specific method."""
        await self.connection.execute("BEGIN")

    async def rollback(self) -> None:
        """Rollback transaction using psqlpy-specific method."""
        await self.connection.execute("ROLLBACK")

    async def commit(self) -> None:
        """Commit transaction using psqlpy-specific method."""
        await self.connection.execute("COMMIT")
