# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import contextlib
import datetime
import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar, Optional

import aiosqlite

from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.parameters import DriverParameterConfig, ParameterStyle
from sqlspec.statement.splitter import split_sql_script
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType
    from typing_extensions import TypeAlias

    from sqlspec.statement.sql import SQL, SQLConfig

    AiosqliteConnection: TypeAlias = aiosqlite.Connection
else:
    AiosqliteConnection = aiosqlite.Connection

__all__ = ("AiosqliteConnection", "AiosqliteCursor", "AiosqliteDriver")

logger = logging.getLogger("sqlspec")


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

    dialect: "DialectType" = "sqlite"
    default_parameter_style: "ClassVar[str]" = "qmark"
    parameter_config: DriverParameterConfig

    def __init__(self, connection: "AiosqliteConnection", config: "Optional[SQLConfig]" = None) -> None:
        super().__init__(connection=connection, config=config)
        self.parameter_config = DriverParameterConfig(
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

    def with_cursor(self, connection: "Optional[AiosqliteConnection]" = None) -> "AiosqliteCursor":
        conn_to_use = connection or self.connection
        return AiosqliteCursor(conn_to_use)

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
            # But we can still use parameters since we're using regular execute()
            prepared_params = self._prepare_driver_parameters(params)
            statements = split_sql_script(sql, dialect=str(self.dialect) if self.dialect else None)
            for stmt in statements:
                if stmt.strip():  # Skip empty statements
                    await cursor.execute(stmt, prepared_params or ())
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
