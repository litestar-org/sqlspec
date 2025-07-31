# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
from typing import TYPE_CHECKING, Any, Optional, Union

from asyncmy.cursors import Cursor, DictCursor

from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.parameters import ParameterStyle
from sqlspec.statement.sql import SQL, StatementConfig

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.adapters.asyncmy._types import AsyncmyConnection

__all__ = ("AsyncmyCursor", "AsyncmyDriver")


class AsyncmyCursor:
    def __init__(self, connection: "AsyncmyConnection") -> None:
        self.connection = connection
        self.cursor: Optional[Union[Cursor, DictCursor]] = None

    async def __aenter__(self) -> Union[Cursor, DictCursor]:
        self.cursor = self.connection.cursor()
        return self.cursor

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.cursor:
            await self.cursor.close()


class AsyncmyDriver(AsyncDriverAdapterBase):
    """Asyncmy MySQL/MariaDB Driver Adapter. Modern protocol implementation."""

    dialect: "DialectType" = "mysql"

    def __init__(
        self,
        connection: "AsyncmyConnection",
        statement_config: Optional[StatementConfig] = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        from sqlspec.statement.sql import StatementConfig

        # Set default asyncmy-specific configuration
        if statement_config is None:
            statement_config = StatementConfig(
                dialect="mysql",
                default_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
                supported_parameter_styles={
                    ParameterStyle.POSITIONAL_PYFORMAT,  # %s
                    ParameterStyle.NAMED_PYFORMAT,  # %(name)s
                },
                type_coercion_map={},  # MySQL has good native type support
                has_native_list_expansion=False,  # MySQL doesn't handle arrays natively
                needs_static_script_compilation=True,  # MySQL requires static compilation for scripts
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        # MySQL type conversions
        # MySQL 5.7+ has native JSON support and handles most types well
        # Type handlers can be registered if the base class supports it

    async def begin(self) -> None:
        """Begin a transaction.

        MySQL/AsyncMy starts transactions automatically with the first command.
        This ensures autocommit is disabled.
        """
        # MySQL starts transactions automatically - no explicit action needed

    async def commit(self) -> None:
        """Commit the current transaction."""
        await self.connection.commit()

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        await self.connection.rollback()

    def with_cursor(self, connection: "AsyncmyConnection") -> "AsyncmyCursor":
        return AsyncmyCursor(connection)

    async def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[tuple[Any, Optional[int], Any]]":
        """Hook for AsyncMy-specific special operations.

        AsyncMy doesn't have special operations like PostgreSQL COPY,
        so this always returns None to proceed with standard execution.
        """
        return None

    async def _execute_many(self, cursor: Any, sql: str, prepared_params: Any) -> Any:
        """AsyncMy executemany implementation."""
        await cursor.executemany(sql, prepared_params)
        return cursor

    async def _execute_statement(self, cursor: Any, sql: str, prepared_params: Any) -> Any:
        """AsyncMy single execution."""
        await cursor.execute(sql, prepared_params or None)
        return cursor

    async def _get_selected_data(self, cursor: Any) -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution."""
        data = await cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description or []]
        # Ensure data is list of dicts
        if data and not isinstance(data[0], dict):
            # Convert tuples to dicts
            data = [dict(zip(column_names, row)) for row in data]
        return data, column_names, len(data)

    def _get_row_count(self, cursor: Any) -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        return cursor.rowcount if cursor.rowcount is not None else -1
