# type: ignore
import logging
from collections.abc import AsyncGenerator, Iterable, Sequence
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from asyncmy import Connection

from sqlspec.base import AsyncDriverAdapterProtocol
from sqlspec.sql.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin
from sqlspec.sql.parameters import ParameterStyle
from sqlspec.sql.result import ExecuteResult, SelectResult
from sqlspec.sql.statement import SQLStatement, Statement, StatementConfig

if TYPE_CHECKING:
    from asyncmy.cursors import Cursor

    from sqlspec.sql.filters import StatementFilter
    from sqlspec.typing import StatementParameterType

__all__ = ("AsyncmyConnection", "AsyncmyDriver")

logger = logging.getLogger("sqlspec")

AsyncmyConnection = Connection


class AsyncmyDriver(
    SQLTranslatorMixin["AsyncmyConnection"],
    AsyncDriverAdapterProtocol["AsyncmyConnection"],
    AsyncArrowMixin["AsyncmyConnection"],
    ResultConverter,
):
    """Asyncmy MySQL/MariaDB Driver Adapter.

    This driver implements the new unified SQLSpec protocol with the core 3 methods:
    - execute() - Universal method for all SQL operations
    - execute_many() - Batch operations
    - execute_script() - Multi-statement scripts

    Enhanced Features:
    - Full SQLStatement integration with filters and validation
    - MySQL-specific parameter style handling (pyformat)
    - Comprehensive parameter validation and security checks
    - Support for filter composition (pagination, search, etc.)
    """

    connection: "AsyncmyConnection"
    __supports_arrow__: ClassVar[bool] = False  # MySQL doesn't typically support Arrow
    dialect: str = "mysql"

    def __init__(self, connection: "AsyncmyConnection", statement_config: "Optional[StatementConfig]" = None) -> None:
        """Initialize the Asyncmy driver adapter."""
        super().__init__(connection, statement_config=statement_config)

    def _get_placeholder_style(self) -> ParameterStyle:
        """Return the placeholder style for MySQL (pyformat)."""
        return ParameterStyle.PYFORMAT_POSITIONAL  # MySQL uses %s for positional

    @staticmethod
    @asynccontextmanager
    async def _with_cursor(connection: "AsyncmyConnection") -> AsyncGenerator["Cursor", None]:
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            await cursor.close()

    async def execute(
        self,
        statement: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: "Any",
    ) -> "Union[SelectResult[dict[str, Any]], ExecuteResult[dict[str, Any]]]":
        """Execute a SQL statement and return a StatementResult.

        This is the unified method for all SQL operations (SELECT, INSERT, UPDATE, DELETE).
        Use the StatementResult methods to extract the data you need.

        Args:
            statement: The SQL statement to execute.
            parameters: Parameters for the statement.
            *filters: Statement filters to apply (e.g., pagination, search filters).
            connection: Optional connection override.
            statement_config: Optional statement configuration.
            **kwargs: Additional keyword arguments.

        Returns:
            A StatementResult containing the operation results.

        Example:
            >>> from sqlspec.sql.filters import LimitOffset, SearchFilter
            >>> # Basic query
            >>> result = await driver.execute(
            ...     "SELECT * FROM users WHERE id = %s", [123]
            ... )
            >>> # Query with filters
            >>> result = await driver.execute(
            ...     "SELECT * FROM users",
            ...     LimitOffset(limit=10, offset=0),
            ...     SearchFilter(field_name="name", value="John"),
            ... )
        """
        conn = self._connection(connection)
        config = statement_config or self.statement_config

        stmt = SQLStatement(statement, parameters, *filters, dialect=self.dialect, statement_config=config, **kwargs)

        stmt.validate()
        placeholder_style = self._get_placeholder_style()
        final_sql = stmt.to_sql(placeholder_style=placeholder_style)
        ordered_params = stmt.get_parameters(style=placeholder_style)

        # Convert parameters to list format for asyncmy using simplified logic
        if ordered_params is not None and not isinstance(ordered_params, list):
            if isinstance(ordered_params, Iterable) and not isinstance(ordered_params, (str, bytes)):
                ordered_params = list(ordered_params)
            else:
                ordered_params = [ordered_params]

        async with self._with_cursor(conn) as cursor:
            await cursor.execute(final_sql, ordered_params)

            if self.returns_rows(stmt.expression):
                results = await cursor.fetchall()
                column_names = [column[0] for column in cursor.description or []]
                rows = [dict(zip(column_names, row)) for row in results]
                raw_select_result_data = rows[0] if rows else cast("dict[str, Any]", {})
                return SelectResult(rows=rows, column_names=column_names, raw_result=raw_select_result_data)

            rowcount = getattr(cursor, "rowcount", -1)
            operation_type = "UNKNOWN"
            if stmt.expression and hasattr(stmt.expression, "key"):
                operation_type = str(stmt.expression.key).upper()

            return ExecuteResult(
                raw_result=cast("dict[str, Any]", {}),
                rows_affected=rowcount,
                operation_type=operation_type,
            )

    async def execute_many(
        self,
        statement: "Statement",
        parameters: "Optional[Sequence[StatementParameterType]]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: "Any",
    ) -> "ExecuteResult[dict[str, Any]]":
        """Execute a SQL statement with multiple parameter sets.

        Useful for batch INSERT, UPDATE, or DELETE operations.

        Args:
            statement: The SQL statement to execute.
            parameters: Sequence of parameter sets.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            statement_config: Optional statement configuration.
            **kwargs: Additional keyword arguments.

        Returns:
            An ExecuteResult containing the batch operation results.

        Example:
            >>> # Batch insert with validation
            >>> await driver.execute_many(
            ...     "INSERT INTO users (name, email) VALUES (%s, %s)",
            ...     [
            ...         ["John", "john@example.com"],
            ...         ["Jane", "jane@example.com"],
            ...     ],
            ... )
        """
        conn = self._connection(connection)
        config = statement_config or self.statement_config

        # Create template statement with filters for validation
        template_stmt = SQLStatement(
            statement,
            None,  # No parameters for template
            *filters,
            dialect=self.dialect,
            statement_config=config,
            **kwargs,
        )

        # Validate template and get final SQL
        template_stmt.validate()
        placeholder_style = self._get_placeholder_style()
        final_sql = template_stmt.to_sql(placeholder_style=placeholder_style)

        # Process parameter sets
        processed_params_list: list[list[Any]] = []
        param_sequence = parameters if parameters is not None else []

        if param_sequence:
            # Create a building config that skips validation for individual parameter sets
            building_config = replace(config, enable_validation=False)

            for param_set in param_sequence:
                item_stmt = SQLStatement(
                    template_stmt.sql,  # Use processed SQL from template
                    param_set,
                    dialect=self.dialect,
                    statement_config=building_config,
                )
                ordered_params_for_item = item_stmt.get_parameters(style=placeholder_style)

                if isinstance(ordered_params_for_item, list):
                    processed_params_list.append(ordered_params_for_item)
                elif ordered_params_for_item is None:
                    processed_params_list.append([])
                # Convert to list
                elif isinstance(ordered_params_for_item, Iterable) and not isinstance(
                    ordered_params_for_item, (str, bytes)
                ):
                    processed_params_list.append(list(ordered_params_for_item))
                else:
                    processed_params_list.append([ordered_params_for_item])

        async with self._with_cursor(conn) as cursor:
            if param_sequence:
                await cursor.executemany(final_sql, processed_params_list)
                total_affected = getattr(cursor, "rowcount", -1)
                if total_affected == -1 and processed_params_list:
                    total_affected = len(processed_params_list)
            else:
                total_affected = 0

            operation_type = "EXECUTE"
            if template_stmt.expression and hasattr(template_stmt.expression, "key"):
                operation_type = str(template_stmt.expression.key).upper()

            return ExecuteResult(
                raw_result=cast("dict[str, Any]", {}), rows_affected=total_affected, operation_type=operation_type
            )

    async def execute_script(
        self,
        statement: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: "Any",
    ) -> "str":
        """Execute a multi-statement SQL script.

        For script execution, parameters are rendered as static literals directly
        in the SQL rather than using placeholders, since scripts may contain
        multiple statements that don't support parameterization.

        Args:
            statement: The SQL script to execute.
            parameters: Parameters for the script.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            statement_config: Optional statement configuration.
            **kwargs: Additional keyword arguments.

        Returns:
            A string with execution results/output.

        Example:
            >>> # Execute a script with parameters rendered as literals
            >>> result = await driver.execute_script(
            ...     \"\"\"
            ...     CREATE TABLE IF NOT EXISTS temp_table (id INT, name VARCHAR(50));
            ...     INSERT INTO temp_table VALUES (%s, %s);
            ...     SELECT COUNT(*) FROM temp_table;
            ...     \"\"\",
            ...     [1, "Test User"]
            ... )
        """
        conn = self._connection(connection)
        config = statement_config or self.statement_config

        merged_params = parameters
        if kwargs:
            if merged_params is None:
                merged_params = kwargs
            elif isinstance(merged_params, dict):
                merged_params = {**merged_params, **kwargs}

        stmt = SQLStatement(statement, merged_params, *filters, dialect=self.dialect, statement_config=config)
        stmt.validate()
        final_sql = stmt.to_sql(placeholder_style=ParameterStyle.STATIC, dialect=self.dialect)

        async with self._with_cursor(conn) as cursor:
            await cursor.execute(final_sql)
            return f"Script executed successfully. Rows affected: {getattr(cursor, 'rowcount', 0)}"

    def _connection(self, connection: "Optional[AsyncmyConnection]" = None) -> "AsyncmyConnection":
        """Get the connection to use for the operation."""
        return connection or self.connection
