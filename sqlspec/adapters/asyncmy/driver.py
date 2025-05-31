# type: ignore
import logging
from collections.abc import AsyncGenerator, Iterable, Sequence
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union

from asyncmy import Connection

from sqlspec.driver import AsyncDriverAdapterProtocol
from sqlspec.statement.builder import QueryBuilder
from sqlspec.statement.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ExecuteResult, SelectResult
from sqlspec.statement.sql import SQL, SQLConfig, Statement

if TYPE_CHECKING:
    from asyncmy.cursors import Cursor

    from sqlspec.statement.filters import StatementFilter
    from sqlspec.typing import DictRow, ModelDTOT, SQLParameterType

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
    __supports_arrow__: ClassVar[bool] = False
    dialect: str = "mysql"

    def __init__(self, connection: "AsyncmyConnection", config: "Optional[SQLConfig]" = None) -> None:
        """Initialize the Asyncmy driver adapter."""
        super().__init__(connection, config=config)

    def _get_parameter_style(self) -> ParameterStyle:
        """Return the placeholder style for MySQL (pyformat)."""
        return ParameterStyle.PYFORMAT_POSITIONAL

    @staticmethod
    @asynccontextmanager
    async def _with_cursor(connection: "AsyncmyConnection") -> AsyncGenerator["Cursor", None]:
        cursor: Cursor = await connection.cursor()
        try:
            yield cursor
        finally:
            if hasattr(cursor, "close") and callable(cursor.close):
                await cursor.close()

    async def execute(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        connection: "Optional[AsyncmyConnection]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: "Any",
    ) -> "Union[SelectResult[ModelDTOT], SelectResult[DictRow], ExecuteResult[Any]]":
        """Execute a SQL statement and return a StatementResult.

        This is the unified method for all SQL operations (SELECT, INSERT, UPDATE, DELETE).
        Use the StatementResult methods to extract the data you need.

        Args:
            statement: The SQL statement or query builder to execute.
            parameters: Parameters for the statement.
            *filters: Statement filters to apply (e.g., pagination, search filters).
            schema_type: Optional Pydantic model or dataclass to map results to.
            connection: Optional connection override.
            config: Optional statement configuration.
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
        if schema_type is not None:
            logger.warning("schema_type parameter is not yet fully implemented for asyncmy driver's execute method.")

        conn = self._connection(connection)
        effective_config = config or self.config

        current_stmt: SQL
        if isinstance(statement, QueryBuilder):
            base_sql_obj = statement.to_statement(config=effective_config)
            current_stmt = SQL(
                base_sql_obj, parameters, *filters, dialect=self.dialect, config=effective_config, **kwargs
            )
        elif isinstance(statement, SQL):
            current_stmt = SQL(statement, parameters, *filters, dialect=self.dialect, config=effective_config, **kwargs)
        else:  # Raw Statement
            current_stmt = SQL(statement, parameters, *filters, dialect=self.dialect, config=effective_config, **kwargs)

        current_stmt.validate()
        parameter_style = self._get_parameter_style()
        final_sql = current_stmt.to_sql(placeholder_style=parameter_style)
        ordered_params = current_stmt.get_parameters(style=parameter_style)

        # asyncmy expects parameters as a list or tuple for %s style, or dict for %(name)s style.
        # Since we use PYFORMAT_POSITIONAL (%s), it needs a list/tuple.
        processed_params: Optional[Union[list[Any], tuple[Any, ...]]] = None
        if ordered_params is not None:
            if isinstance(ordered_params, (list, tuple)):
                processed_params = ordered_params
            elif isinstance(ordered_params, Iterable) and not isinstance(ordered_params, (str, bytes)):
                processed_params = list(ordered_params)  # Convert iterable to list
            else:
                processed_params = [ordered_params]  # Single param, wrap in list

        async with self._with_cursor(conn) as cursor:
            await cursor.execute(final_sql, processed_params)  # type: ignore[arg-type]

            if self.returns_rows(current_stmt.expression):
                results = await cursor.fetchall()
                column_names = [column[0] for column in cursor.description or []]
                rows: list[DictRow] = [dict(zip(column_names, row)) for row in results]
                raw_select_result_data = rows[0] if rows else {}
                return SelectResult(rows=rows, column_names=column_names, raw_result=raw_select_result_data)

            rowcount = getattr(cursor, "rowcount", -1)
            operation_type = "UNKNOWN"
            if current_stmt.expression and hasattr(current_stmt.expression, "key"):
                operation_type = str(current_stmt.expression.key).upper()

            return ExecuteResult(
                raw_result={},
                rows_affected=rowcount,
                operation_type=operation_type,
            )

    async def execute_many(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[ExecuteResult[Any]]]",
        parameters: "Optional[Sequence[SQLParameterType]]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: "Any",
    ) -> "ExecuteResult[Any]":
        """Execute a SQL statement with multiple parameter sets.

        Useful for batch INSERT, UPDATE, or DELETE operations.

        Args:
            statement: The SQL statement or query builder to execute.
            parameters: Sequence of parameter sets.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            config: Optional statement configuration.
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
        effective_config = config or self.config

        template_sql_input: Statement
        if isinstance(statement, QueryBuilder):
            template_sql_input = statement.to_statement(config=effective_config).sql
        elif isinstance(statement, SQL):
            template_sql_input = statement.sql
        else:  # Raw Statement
            template_sql_input = statement

        template_stmt = SQL(
            template_sql_input,
            None,
            *filters,
            dialect=self.dialect,
            config=effective_config,
            **kwargs,
        )

        template_stmt.validate()
        parameter_style = self._get_parameter_style()
        final_sql = template_stmt.to_sql(placeholder_style=parameter_style)

        # asyncmy executemany expects a list of tuples/lists.
        processed_params_list: list[Union[list[Any], tuple[Any, ...]]] = []
        param_sequence = parameters if parameters is not None else []

        if param_sequence:
            building_config = replace(effective_config, enable_validation=False)
            for param_set in param_sequence:
                item_stmt = SQL(
                    template_stmt.sql,
                    param_set,
                    dialect=self.dialect,
                    config=building_config,
                )
                ordered_params_for_item = item_stmt.get_parameters(style=parameter_style)

                current_item_params_as_sequence: Union[list[Any], tuple[Any, ...]]
                if isinstance(ordered_params_for_item, (list, tuple)):
                    current_item_params_as_sequence = ordered_params_for_item
                elif ordered_params_for_item is None:
                    current_item_params_as_sequence = []
                elif isinstance(ordered_params_for_item, Iterable) and not isinstance(
                    ordered_params_for_item, (str, bytes)
                ):
                    current_item_params_as_sequence = list(ordered_params_for_item)
                else:
                    current_item_params_as_sequence = [ordered_params_for_item]
                processed_params_list.append(current_item_params_as_sequence)

        async with self._with_cursor(conn) as cursor:
            total_affected = 0
            if processed_params_list:
                await cursor.executemany(final_sql, processed_params_list)  # type: ignore[arg-type]
                total_affected = getattr(cursor, "rowcount", 0)  # executemany often provides a sum or last op count
                # Default to 0 if not present for safety.

            operation_type = "EXECUTE"
            if template_stmt.expression and hasattr(template_stmt.expression, "key"):
                operation_type = str(template_stmt.expression.key).upper()

            return ExecuteResult(raw_result={}, rows_affected=total_affected, operation_type=operation_type)

    async def execute_script(
        self,
        statement: "Union[str, SQL]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: "Any",
    ) -> "str":
        """Execute a multi-statement SQL script.

        For script execution, parameters are rendered as static literals directly
        in the SQL rather than using placeholders, since scripts may contain
        multiple statements that don't support parameterization.

        Args:
            statement: The SQL script or SQL object to execute.
            parameters: Parameters for the script.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            config: Optional statement configuration.
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
        effective_config = config or self.config

        merged_params = parameters
        if kwargs:
            if merged_params is None:
                merged_params = kwargs
            elif isinstance(merged_params, dict):
                merged_params = {**merged_params, **kwargs}

        script_stmt: SQL
        if isinstance(statement, SQL):
            script_stmt = SQL(statement, merged_params, *filters, dialect=self.dialect, config=effective_config)
        else:  # Raw string
            script_stmt = SQL(statement, merged_params, *filters, dialect=self.dialect, config=effective_config)

        script_stmt.validate()
        final_sql = script_stmt.to_sql(placeholder_style=ParameterStyle.STATIC, dialect=self.dialect)

        rowcount_sum = 0
        async with self._with_cursor(conn) as cursor:
            # asyncmy.Cursor.execute with multi=True handles ;-separated statements.
            # The result/rowcount for such an execution can be tricky.
            # It might only reflect the last statement or a sum, driver-dependent.
            await cursor.execute(final_sql, multi=True)  # type: ignore[call-arg]
            current_rowcount = getattr(cursor, "rowcount", 0)
            if current_rowcount != -1:
                rowcount_sum = current_rowcount  # Use the reported rowcount if not -1
            else:
                # If -1, it's often an indicator that rowcount is not applicable or reliable for multi-statement query.
                # We could try to count statements if needed, but it's an approximation.
                rowcount_sum = -1  # Indicate unknown/unreliable count from script

        return f"Script executed. Approximate rows affected: {rowcount_sum}"

    def _connection(self, connection: "Optional[AsyncmyConnection]" = None) -> "AsyncmyConnection":
        """Get the connection to use for the operation."""
        return connection or self.connection
