# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import contextlib
import datetime
import logging
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar, Optional

import aiosqlite

from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.parameters import DriverParameterConfig, ParameterStyle
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlglot.dialects.dialect import DialectType
    from typing_extensions import TypeAlias

    from sqlspec.statement.result import SQLResult
    from sqlspec.statement.sql import SQL, SQLConfig

__all__ = ("AiosqliteConnection", "AiosqliteDriver")

logger = logging.getLogger("sqlspec")

if TYPE_CHECKING:
    AiosqliteConnection: TypeAlias = aiosqlite.Connection
else:
    # Direct assignment for mypyc runtime
    AiosqliteConnection = aiosqlite.Connection


class AiosqliteDriver(AsyncDriverAdapterBase):
    """Reference implementation for an asynchronous aiosqlite driver."""

    dialect: "DialectType" = "sqlite"
    default_parameter_style: "ClassVar[str]" = "qmark"
    parameter_config: ClassVar[DriverParameterConfig] = DriverParameterConfig(
        supported_parameter_styles=[ParameterStyle.QMARK],  # Only supports ?
        default_parameter_style=ParameterStyle.QMARK,
        type_coercion_map={
            bool: int,
            datetime.datetime: lambda v: v.isoformat(),
            Decimal: str,
            dict: to_json,
            list: to_json,
            tuple: lambda v: to_json(list(v)),
        },
        has_native_list_expansion=False,
    )

    def __init__(self, connection: "AiosqliteConnection", config: "Optional[SQLConfig]" = None) -> None:
        super().__init__(connection=connection, config=config)

    @asynccontextmanager
    async def with_cursor(
        self, connection: "Optional[AiosqliteConnection]" = None
    ) -> "AsyncGenerator[aiosqlite.Cursor, None]":
        conn_to_use = connection or self.connection
        conn_to_use.row_factory = aiosqlite.Row
        cursor = await conn_to_use.cursor()
        try:
            yield cursor
        finally:
            await cursor.close()

    async def begin(self, connection: "Optional[Any]" = None) -> None:
        """Begin a database transaction."""
        conn = connection or self.connection
        with contextlib.suppress(Exception):
            # aiosqlite might already be in a transaction or handle transactions differently
            await conn.execute("BEGIN")

    async def rollback(self, connection: "Optional[Any]" = None) -> None:
        """Rollback the current transaction."""
        conn = connection or self.connection
        await conn.rollback()

    async def commit(self, connection: "Optional[Any]" = None) -> None:
        """Commit the current transaction."""
        conn = connection or self.connection
        await conn.commit()

    async def _perform_execute(self, cursor: "aiosqlite.Cursor", statement: "SQL") -> None:
        # Compile with driver's parameter style
        sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)

        if statement.is_many:
            # For execute_many, params is already a list of parameter sets
            prepared_params = self._prepare_driver_parameters_many(params) if params else []
            await cursor.executemany(sql, prepared_params)
        else:
            # Prepare parameters for driver consumption
            prepared_params = self._prepare_driver_parameters(params)
            await cursor.execute(sql, prepared_params or ())

    async def _build_result(self, cursor: "aiosqlite.Cursor", statement: "SQL") -> "SQLResult":
        if self.returns_rows(statement.expression):
            # Fetch data asynchronously before calling base method
            fetched_data = await cursor.fetchall()
            # Create a mock cursor object that has the data and description
            mock_cursor = type(
                "MockCursor", (), {"fetchall": lambda _: fetched_data, "description": cursor.description}
            )()
            return self._build_select_result(mock_cursor, statement)
        return self._build_modify_result(cursor, statement)
