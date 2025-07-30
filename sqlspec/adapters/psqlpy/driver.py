"""Psqlpy Driver Implementation."""
# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false

from typing import TYPE_CHECKING, Any, Optional

from sqlspec.adapters.psqlpy._types import PsqlpyConnection
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.parameters import ParameterStyle

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.result import SQLResult
    from sqlspec.statement.sql import SQL, StatementConfig


__all__ = ("PsqlpyCursor", "PsqlpyDriver")


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

    def __init__(self, connection: PsqlpyConnection, statement_config: "Optional[StatementConfig]" = None, driver_features: "Optional[dict[str, Any]]" = None) -> None:
        from sqlspec.statement.sql import StatementConfig

        # Set default psqlpy-specific configuration
        if statement_config is None:
            statement_config = StatementConfig(
                dialect="postgres",
                supported_parameter_styles={ParameterStyle.NUMERIC},  # $1, $2
                default_parameter_style=ParameterStyle.NUMERIC,
                type_coercion_map={
                    # Psqlpy handles most types natively
                    # Add any specific type mappings as needed
                },
                has_native_list_expansion=True,  # Psqlpy handles lists natively
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: PsqlpyConnection) -> "PsqlpyCursor":
        return PsqlpyCursor(connection)

    async def _perform_execute(self, cursor: PsqlpyConnection, statement: "SQL") -> None:
        sql, params = self._get_compiled_sql(statement, self.statement_config)

        # Store compiled SQL and params to avoid re-compilation in _extract_select_data
        self._last_compiled_sql = sql
        self._last_prepared_params = self.prepare_driver_parameters(params, self.statement_config, is_many=False)

        if statement.is_script:
            # PsqlPy doesn't have executescript - execute statements one by one
            # But we can still use parameters since we're using regular execute()
            prepared_params = self._last_prepared_params
            # Use the proper script splitter to handle complex cases
            statements = self.split_script_statements(sql, self.statement_config, strip_trailing_semicolon=True)
            for stmt in statements:
                if stmt.strip():  # Skip empty statements
                    await cursor.execute(stmt, prepared_params)
        elif statement.is_many:
            # For execute_many, params is already a list of parameter sets
            prepared_params = self.prepare_driver_parameters(params, self.statement_config, is_many=True)
            await cursor.execute_many(sql, prepared_params)
        else:
            await cursor.execute(sql, self._last_prepared_params)

    async def _extract_select_data(self, cursor: PsqlpyConnection) -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution.

        Note: psqlpy requires a separate fetch() call after execute().
        """
        # Use the compiled SQL and prepared params stored during _perform_execute
        # This avoids re-compilation and follows single-pass processing principle
        if hasattr(self, "_last_compiled_sql") and hasattr(self, "_last_prepared_params"):
            sql = self._last_compiled_sql
            prepared_params = self._last_prepared_params
            query_result = await cursor.fetch(sql, prepared_params)
            dict_rows: list[dict[str, Any]] = []
            if query_result:
                dict_rows = query_result.result()
        else:
            # Fallback - this shouldn't happen in normal flow
            dict_rows = []

        column_names = list(dict_rows[0].keys()) if dict_rows else []
        return dict_rows, column_names, len(dict_rows)

    def _extract_execute_rowcount(self, cursor: PsqlpyConnection) -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        # psqlpy doesn't easily expose rowcount, so we return -1
        return -1

    async def _build_result(self, cursor: PsqlpyConnection, statement: "SQL") -> "SQLResult":
        """Build result - override to handle psqlpy's special needs."""
        try:
            return await super()._build_result(cursor, statement)
        finally:
            # Clean up stored compiled results
            if hasattr(self, "_last_compiled_sql"):
                delattr(self, "_last_compiled_sql")
            if hasattr(self, "_last_prepared_params"):
                delattr(self, "_last_prepared_params")

    async def begin(self) -> None:
        """Begin transaction using psqlpy-specific method."""
        await self.connection.execute("BEGIN")

    async def rollback(self) -> None:
        """Rollback transaction using psqlpy-specific method."""
        await self.connection.execute("ROLLBACK")

    async def commit(self) -> None:
        """Commit transaction using psqlpy-specific method."""
        await self.connection.execute("COMMIT")
