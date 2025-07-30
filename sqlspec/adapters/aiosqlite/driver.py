# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import contextlib
import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar, Optional

import aiosqlite

from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.parameters import ParameterStyle
from sqlspec.statement.sql import StatementConfig
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.adapters.aiosqlite._types import AiosqliteConnection


__all__ = ("AiosqliteCursor", "AiosqliteDriver")


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

    def __init__(self, connection: "AiosqliteConnection", statement_config: "Optional[StatementConfig]" = None, driver_features: "Optional[dict[str, Any]]" = None) -> None:
        # Set default aiosqlite-specific configuration
        if statement_config is None:
            statement_config = StatementConfig(
                dialect="sqlite",
                supported_parameter_styles={ParameterStyle.QMARK},
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
                needs_static_script_compilation=False,  # aiosqlite supports parameters in execute() calls
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

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

    # Use default _execute_script implementation from base class

    async def _execute_many(self, cursor: "aiosqlite.Cursor", sql: str, prepared_params: Any) -> None:
        """Execute SQL with multiple parameter sets using aiosqlite executemany."""
        await cursor.executemany(sql, prepared_params)

    async def _execute_statement(self, cursor: "aiosqlite.Cursor", sql: str, prepared_params: Any) -> None:
        """Execute single SQL statement using aiosqlite execute."""
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
        return cursor.rowcount or 0
