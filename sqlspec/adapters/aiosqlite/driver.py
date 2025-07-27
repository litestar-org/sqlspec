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
from sqlspec.statement.splitter import split_sql_script
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlglot.dialects.dialect import DialectType
    from typing_extensions import TypeAlias

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

    async def begin(self) -> None:
        """Begin a database transaction."""
        with contextlib.suppress(Exception):
            # aiosqlite might already be in a transaction or handle transactions differently
            await self.connection.execute("BEGIN")

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        await self.connection.rollback()

    async def commit(self) -> None:
        """Commit the current transaction."""
        await self.connection.commit()

    async def _perform_execute(self, cursor: "aiosqlite.Cursor", statement: "SQL") -> None:
        # Compile with driver's parameter style
        sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)

        if statement.is_script:
            # aiosqlite doesn't support executescript, so we need to execute statements one by one
            # Use the proper script splitter to handle complex cases
            statements = split_sql_script(sql, dialect=str(self.dialect) if self.dialect else None)
            for stmt in statements:
                if stmt.strip():  # Skip empty statements
                    await cursor.execute(stmt)
        elif statement.is_many:
            # For execute_many, params is already a list of parameter sets
            prepared_params = self._prepare_driver_parameters_many(params) if params else []
            await cursor.executemany(sql, prepared_params)
        else:
            # Prepare parameters for driver consumption
            prepared_params = self._prepare_driver_parameters(params)
            await cursor.execute(sql, prepared_params or ())

    async def _extract_select_data(self, cursor: "aiosqlite.Cursor") -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution."""
        fetched_data = await cursor.fetchall()
        column_names = [col[0] for col in cursor.description or []]
        # Convert Row objects to dicts
        data = [dict(row) for row in fetched_data]
        return data, column_names, len(data)

    def _extract_execute_rowcount(self, cursor: "aiosqlite.Cursor") -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        return cursor.rowcount if cursor.rowcount is not None else 0
