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
        supported_parameter_styles={ParameterStyle.NUMERIC},  # $1, $2
        type_coercion_map={
            # Psqlpy handles most types natively
            # Add any specific type mappings as needed
        },
        has_native_list_expansion=True,  # Psqlpy handles lists natively
        needs_static_script_compilation=False,  # Psqlpy supports parameters in scripts
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

        PsqlPy requires special handling for SELECT queries that need separate fetch() call.
        Also handles scripts by executing multiple statements sequentially.
        """
        if statement.is_script:
            sql, params = statement.compile()
            prepared_params = self.prepare_driver_parameters(params, self.statement_config, is_many=False)
            # Use the proper script splitter to handle complex cases
            statements = self.split_script_statements(sql, self.statement_config, strip_trailing_semicolon=True)
            statement_count = len(statements)

            for stmt in statements:
                if stmt.strip():  # Skip empty statements
                    await cursor.execute(stmt, prepared_params)

            # Create ExecutionResult and build SQLResult directly
            execution_result = self.create_execution_result(
                cursor,
                statement_count=statement_count,
                successful_statements=statement_count,  # Assume all successful if no exception
                is_script_result=True,
            )
            return self.build_statement_result(statement, execution_result)

        # Handle SELECT queries that need separate fetch() call
        if statement.returns_rows() and not statement.is_script and not statement.is_many:
            sql, params = statement.compile()
            prepared_params = self.prepare_driver_parameters(params, self.statement_config, is_many=False)

            # For PsqlPy, we need to use fetch() instead of execute() for SELECT queries
            query_result = await cursor.fetch(sql, prepared_params)
            dict_rows: list[dict[str, Any]] = []
            if query_result:
                dict_rows = query_result.result()

            column_names = list(dict_rows[0].keys()) if dict_rows else []

            # Create ExecutionResult with pre-fetched data and build SQLResult directly
            execution_result = self.create_execution_result(
                cursor,
                selected_data=dict_rows,
                column_names=column_names,
                data_row_count=len(dict_rows),
                is_select_result=True,
            )
            return self.build_statement_result(statement, execution_result)

        return None

    async def _execute_many(self, cursor: PsqlpyConnection, sql: str, prepared_params: Any) -> "ExecutionResult":
        """PsqlPy executemany implementation."""
        await cursor.execute_many(sql, prepared_params)

        # PsqlPy doesn't easily expose rowcount, so we approximate using parameter count
        row_count = len(prepared_params) if prepared_params else 0

        return self.create_execution_result(cursor, rowcount_override=row_count, is_many_result=True)

    async def _execute_statement(self, cursor: PsqlpyConnection, sql: str, prepared_params: Any) -> "ExecutionResult":
        """PsqlPy single execution for non-row-returning queries."""
        await cursor.execute(sql, prepared_params)

        # Get row count if available (PsqlPy doesn't easily expose rowcount)
        try:
            row_count = self._get_row_count(cursor)
        except Exception:
            row_count = None

        return self.create_execution_result(cursor, rowcount_override=row_count)

    async def _get_selected_data(self, cursor: PsqlpyConnection) -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution.

        Note: For PsqlPy, data is pre-fetched in _try_special_handling,
        so this method should not be called.
        """
        # This should not be called for PsqlPy since we pre-fetch in _try_special_handling
        return [], [], 0

    def _get_row_count(self, cursor: PsqlpyConnection) -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        # psqlpy doesn't easily expose rowcount, so we return -1
        return -1

    async def begin(self) -> None:
        """Begin transaction using psqlpy-specific method."""
        await self.connection.execute("BEGIN")

    async def rollback(self) -> None:
        """Rollback transaction using psqlpy-specific method."""
        await self.connection.execute("ROLLBACK")

    async def commit(self) -> None:
        """Commit transaction using psqlpy-specific method."""
        await self.connection.execute("COMMIT")
