"""Psqlpy Driver Implementation."""
# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false

import logging
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    
    from typing_extensions import TypeAlias

    from sqlspec.statement.sql import SQL, SQLConfig


from psqlpy import Connection

from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.parameters import DriverParameterConfig, ParameterStyle
from sqlspec.statement.result import SQLResult

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

__all__ = ("PsqlpyConnection", "PsqlpyDriver")

if TYPE_CHECKING:
    PsqlpyConnection: TypeAlias = Connection
else:
    # Direct assignment for mypyc runtime
    PsqlpyConnection = Connection
logger = logging.getLogger("sqlspec")

from contextlib import asynccontextmanager


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

    async def _build_result(self, cursor: PsqlpyConnection, statement: "SQL") -> "SQLResult":
        if self.returns_rows(statement.expression):
            return await self._build_select_result(cursor, statement)
        return self._build_modify_result(cursor, statement)

    async def _build_select_result(self, cursor: PsqlpyConnection, statement: "SQL") -> "SQLResult":
        sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)
        prepared_params = self._prepare_driver_parameters(params)
        query_result = await cursor.fetch(sql, prepared_params or [])
        dict_rows: list[dict[str, Any]] = []
        if query_result:
            dict_rows = query_result.result()
        column_names = list(dict_rows[0].keys()) if dict_rows else []
        return SQLResult(
            statement=statement,
            data=dict_rows,
            column_names=column_names,
            rows_affected=len(dict_rows),
            operation_type="SELECT",
        )

    def _build_modify_result(self, cursor: PsqlpyConnection, statement: "SQL") -> "SQLResult":
        affected_count = -1
        return SQLResult(
            statement=statement,
            data=[],
            rows_affected=affected_count,
            operation_type=self._determine_operation_type(statement),
            metadata={"status_message": "OK"},
        )

    async def begin(self, connection: "Optional[Any]" = None) -> None:
        """Begin transaction using psqlpy-specific method."""
        conn = connection or self.connection
        await conn.execute("BEGIN")

    async def rollback(self, connection: "Optional[Any]" = None) -> None:
        """Rollback transaction using psqlpy-specific method."""
        conn = connection or self.connection
        await conn.execute("ROLLBACK")

    async def commit(self, connection: "Optional[Any]" = None) -> None:
        """Commit transaction using psqlpy-specific method."""
        conn = connection or self.connection
        await conn.execute("COMMIT")
