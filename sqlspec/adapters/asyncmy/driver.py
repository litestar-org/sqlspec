# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import logging
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Optional, Union

from asyncmy import Connection

from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.parameters import DriverParameterConfig, ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
# Type handlers removed - MySQL has good native type support

if TYPE_CHECKING:
    from asyncmy.cursors import Cursor, DictCursor
    from sqlglot.dialects.dialect import DialectType

__all__ = ("AsyncmyConnection", "AsyncmyDriver")

logger = logging.getLogger("sqlspec")

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    AsyncmyConnection: TypeAlias = Connection
else:
    AsyncmyConnection = Connection


class AsyncmyDriver(AsyncDriverAdapterBase):
    """Asyncmy MySQL/MariaDB Driver Adapter. Modern protocol implementation."""

    dialect: "DialectType" = "mysql"
    parameter_config = DriverParameterConfig(
        supported_parameter_styles=[
            ParameterStyle.POSITIONAL_PYFORMAT,  # %s
            ParameterStyle.NAMED_PYFORMAT,       # %(name)s
        ],
        default_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
        type_coercion_map={
            # MySQL has good native type support
            # Add any specific type mappings as needed
        },
        has_native_list_expansion=False,  # MySQL doesn't handle arrays natively
    )

    def __init__(self, connection: AsyncmyConnection, config: Optional[SQLConfig] = None) -> None:
        super().__init__(connection=connection, config=config)
        # MySQL type conversions
        # MySQL 5.7+ has native JSON support and handles most types well
        # Type handlers can be registered if the base class supports it

    async def begin(self, connection: "Optional[Any]" = None) -> None:
        """Begin a transaction.

        MySQL/AsyncMy starts transactions automatically with the first command.
        This ensures autocommit is disabled.
        """
        # MySQL starts transactions automatically - no explicit action needed

    async def commit(self, connection: "Optional[Any]" = None) -> None:
        """Commit the current transaction."""
        conn = connection or self.connection
        await conn.commit()

    async def rollback(self, connection: "Optional[Any]" = None) -> None:
        """Rollback the current transaction."""
        conn = connection or self.connection
        await conn.rollback()

    def _connection(self, connection: "Optional[AsyncmyConnection]" = None) -> "AsyncmyConnection":
        """Get the connection to use for the operation."""
        return connection or self.connection

    @asynccontextmanager
    async def with_cursor(self, connection: "AsyncmyConnection") -> "AsyncGenerator[Union[Cursor, DictCursor], None]":
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            await cursor.close()

    async def _perform_execute(self, cursor: "Union[Cursor, DictCursor]", statement: "SQL") -> None:
        """Execute the SQL statement using the provided cursor."""
        sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)

        if statement.is_many:
            # For execute_many, params is already a list of parameter sets
            prepared_params = self._prepare_driver_parameters_many(params) if params else []
            await cursor.executemany(sql, prepared_params)
        else:
            prepared_params = self._prepare_driver_parameters(params)
            await cursor.execute(sql, prepared_params or None)

    async def _build_result(self, cursor: "Union[Cursor, DictCursor]", statement: "SQL") -> "SQLResult":
        """Build and return the result of the SQL execution."""
        if self.returns_rows(statement.expression):
            data = await cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description or []]
            return SQLResult(
                statement=statement,
                data=data,
                column_names=column_names,
                rows_affected=len(data),
                operation_type="SELECT",
            )

        return SQLResult(
            statement=statement,
            data=[],
            rows_affected=cursor.rowcount if cursor.rowcount is not None else -1,
            operation_type=self._determine_operation_type(statement),
            metadata={"status_message": "OK"},
        )

    def _prepare_driver_parameters(self, parameters: "Any") -> "Any":
        """Prepare parameters for the AsyncMy driver."""
        return parameters
