"""Psqlpy Driver Implementation."""
# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from typing_extensions import TypeAlias

    from sqlspec.statement.result import SQLResult
    from sqlspec.statement.sql import SQL, SQLConfig


from psqlpy import Connection

from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.parameters import DriverParameterConfig, ParameterStyle

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

__all__ = ("PsqlpyConnection", "PsqlpyDriver")

if TYPE_CHECKING:
    PsqlpyConnection: TypeAlias = Connection
else:
    # Direct assignment for mypyc runtime
    PsqlpyConnection = Connection
logger = logging.getLogger("sqlspec")


class PsqlpyDriver(AsyncDriverAdapterBase):
    """Psqlpy Driver Adapter."""

    dialect: "DialectType" = "postgres"
    parameter_config = DriverParameterConfig(
        supported_parameter_styles=[ParameterStyle.NUMERIC],  # $1, $2
        default_parameter_style=ParameterStyle.NUMERIC,
        type_coercion_map={
            # Psqlpy handles most types natively
            # Add any specific type mappings as needed
        },
        has_native_list_expansion=True,  # Psqlpy handles lists natively
    )

    def __init__(self, connection: PsqlpyConnection, config: "Optional[SQLConfig]" = None) -> None:
        super().__init__(connection=connection, config=config)

    @asynccontextmanager
    async def with_cursor(self, connection: PsqlpyConnection) -> "AsyncGenerator[Any, None]":
        yield connection

    async def _perform_execute(self, cursor: PsqlpyConnection, statement: "SQL") -> None:
        sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)

        if statement.is_many:
            # For execute_many, params is already a list of parameter sets
            prepared_params = self._prepare_driver_parameters_many(params) if params else []
            await cursor.execute_many(sql, prepared_params)
        else:
            prepared_params = self._prepare_driver_parameters(params)
            await cursor.execute(sql, prepared_params or [])

    async def _extract_select_data(self, cursor: PsqlpyConnection) -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution.

        Note: psqlpy requires a separate fetch() call after execute().
        """
        # Similar to asyncpg, we need to fetch after execute
        if hasattr(self, "_last_statement"):
            statement = self._last_statement
            sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)
            prepared_params = self._prepare_driver_parameters(params)
            query_result = await cursor.fetch(sql, prepared_params or [])
            dict_rows: list[dict[str, Any]] = []
            if query_result:
                dict_rows = query_result.result()
        else:
            dict_rows = []

        column_names = list(dict_rows[0].keys()) if dict_rows else []
        return dict_rows, column_names, len(dict_rows)

    def _extract_execute_rowcount(self, cursor: PsqlpyConnection) -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        # psqlpy doesn't easily expose rowcount, so we return -1
        return -1

    async def _build_result(self, cursor: PsqlpyConnection, statement: "SQL") -> "SQLResult":
        """Build result - override to handle psqlpy's special needs."""
        # Store statement for _extract_select_data
        self._last_statement = statement
        try:
            return await super()._build_result(cursor, statement)
        finally:
            # Clean up
            if hasattr(self, "_last_statement"):
                delattr(self, "_last_statement")

    async def begin(self) -> None:
        """Begin transaction using psqlpy-specific method."""
        await self.connection.execute("BEGIN")

    async def rollback(self) -> None:
        """Rollback transaction using psqlpy-specific method."""
        await self.connection.execute("ROLLBACK")

    async def commit(self) -> None:
        """Commit transaction using psqlpy-specific method."""
        await self.connection.execute("COMMIT")
