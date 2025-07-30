# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
from typing import TYPE_CHECKING, Any, Optional, Union

from asyncmy.cursors import Cursor, DictCursor

from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.parameters import ParameterStyle
from sqlspec.statement.sql import SQL, SQLConfig

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

    def __init__(self, connection: "AsyncmyConnection", statement_config: Optional[SQLConfig] = None) -> None:
        from sqlspec.statement.sql import SQLConfig

        # Set default asyncmy-specific configuration
        if statement_config is None:
            statement_config = SQLConfig(
                supported_parameter_styles=[
                    ParameterStyle.POSITIONAL_PYFORMAT,  # %s
                    ParameterStyle.NAMED_PYFORMAT,  # %(name)s
                ],
                default_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
                type_coercion_map={
                    # MySQL has good native type support
                    # Add any specific type mappings as needed
                },
                has_native_list_expansion=False,  # MySQL doesn't handle arrays natively
            )

        super().__init__(connection=connection, statement_config=statement_config)
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

    async def _perform_execute(self, cursor: "Union[Cursor, DictCursor]", statement: "SQL") -> None:
        """Execute the SQL statement using the provided cursor."""
        if statement.is_script:
            # Scripts use STATIC compilation to transpile parameters automatically
            sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
            # MySQL doesn't have executescript - execute statements one by one
            statements = self._split_script_statements(sql, strip_trailing_semicolon=True)
            for stmt in statements:
                if stmt.strip():  # Skip empty statements
                    await cursor.execute(stmt)
        else:
            # Enable intelligent parameter conversion - MySQL supports both POSITIONAL_PYFORMAT and NAMED_PYFORMAT
            sql, params = self._get_compiled_sql(
                statement, self.statement_config.get_parameter_config().default_parameter_style
            )

            if statement.is_many:
                # For execute_many, params is already a list of parameter sets
                prepared_params = self._prepare_driver_parameters_many(params) if params else []
                await cursor.executemany(sql, prepared_params)
            else:
                prepared_params = self._prepare_driver_parameters(params)
                await cursor.execute(sql, prepared_params or None)

    async def _extract_select_data(
        self, cursor: "Union[Cursor, DictCursor]"
    ) -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution."""
        data = await cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description or []]
        # Ensure data is list of dicts
        if data and not isinstance(data[0], dict):
            # Convert tuples to dicts
            data = [dict(zip(column_names, row)) for row in data]
        return data, column_names, len(data)

    def _extract_execute_rowcount(self, cursor: "Union[Cursor, DictCursor]") -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        return cursor.rowcount if cursor.rowcount is not None else -1

    def _prepare_driver_parameters(self, parameters: "Any") -> "Any":
        """Prepare parameters for the AsyncMy driver."""
        return parameters
