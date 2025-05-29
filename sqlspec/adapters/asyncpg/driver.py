import logging
import re
from collections.abc import Iterable, Sequence
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from asyncpg import Connection as AsyncpgNativeConnection
from typing_extensions import TypeAlias

from sqlspec.base import AsyncDriverAdapterProtocol
from sqlspec.sql.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin
from sqlspec.sql.parameters import ParameterStyle
from sqlspec.sql.result import ExecuteResult, SelectResult
from sqlspec.sql.statement import SQLStatement, Statement, StatementConfig
from sqlspec.typing import StatementParameterType

if TYPE_CHECKING:
    from asyncpg import Record
    from asyncpg.pool import PoolConnectionProxy

    from sqlspec.sql.filters import StatementFilter


__all__ = ("AsyncpgConnection", "AsyncpgDriver")

logger = logging.getLogger("sqlspec")

if TYPE_CHECKING:
    AsyncpgConnection: TypeAlias = Union[AsyncpgNativeConnection[Record], PoolConnectionProxy[Record]]
else:
    AsyncpgConnection: TypeAlias = Union[AsyncpgNativeConnection, Any]

# Compile the row count regex once for efficiency
ROWCOUNT_REGEX = re.compile(r"^(?:INSERT|UPDATE|DELETE|MERGE) \d+ (\d+)$", re.IGNORECASE)


class AsyncpgDriver(
    SQLTranslatorMixin["AsyncpgConnection"],
    AsyncDriverAdapterProtocol["AsyncpgConnection"],
    AsyncArrowMixin["AsyncpgConnection"],
    ResultConverter,
):
    """AsyncPG Postgres Driver Adapter.

    This driver implements the new unified SQLSpec protocol with the core 3 methods:
    - execute() - Universal method for all SQL operations
    - execute_many() - Batch operations
    - execute_script() - Multi-statement scripts

    Enhanced Features:
    - Full SQLStatement integration with filters and validation
    - PostgreSQL-specific parameter style handling ($1, $2, etc.)
    - Comprehensive parameter validation and security checks
    - Support for filter composition (pagination, search, etc.)
    """

    connection: "AsyncpgConnection"
    __supports_arrow__: ClassVar[bool] = False  # asyncpg doesn't support Arrow natively
    dialect: str = "postgres"
    statement_config: Optional[StatementConfig] = None

    def __init__(self, connection: "AsyncpgConnection", statement_config: Optional[StatementConfig] = None) -> None:
        """Initialize the AsyncPG driver adapter."""
        super().__init__(connection, statement_config=statement_config)

    def _get_placeholder_style(self) -> ParameterStyle:
        """Return the placeholder style for PostgreSQL ($1, $2, etc.)."""
        return ParameterStyle.NUMERIC

    async def execute(
        self,
        statement: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        statement_config: Optional["StatementConfig"] = None,
        **kwargs: Any,
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
            ...     "SELECT * FROM users WHERE id = $1", [123]
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

        # Convert parameters to list format for asyncpg using simplified logic
        if ordered_params is not None and not isinstance(ordered_params, list):
            if isinstance(ordered_params, Iterable) and not isinstance(ordered_params, (str, bytes)):
                ordered_params = list(ordered_params)
            else:
                ordered_params = [ordered_params]

        if self.returns_rows(stmt.expression):
            records: list[Record] = await conn.fetch(final_sql, *(ordered_params or []))
            if not records:
                return SelectResult(raw_result=cast("dict[str, Any]", {}), rows=[], column_names=[])

            column_names = list(records[0].keys())
            dict_rows = [dict(record.items()) for record in records]
            raw_select_result_data = dict_rows[0] if dict_rows else cast("dict[str, Any]", {})

            return SelectResult(
                raw_result=raw_select_result_data,
                rows=dict_rows,
                column_names=column_names,
            )

        status_str: str = await conn.execute(final_sql, *(ordered_params or []))
        rows_affected = 0
        match = ROWCOUNT_REGEX.match(status_str)
        if match:
            rows_affected = int(match.group(1))

        operation_type = "UNKNOWN"
        if stmt.expression and hasattr(stmt.expression, "key"):
            operation_type = str(stmt.expression.key).upper()

        return ExecuteResult(
            raw_result=cast("dict[str, Any]", {}),
            rows_affected=rows_affected,
            operation_type=operation_type,
        )

    async def execute_many(
        self,
        statement: "Statement",
        parameters: Optional[Sequence[StatementParameterType]] = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        statement_config: Optional[StatementConfig] = None,
        **kwargs: Any,
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
            ...     "INSERT INTO users (name, email) VALUES ($1, $2)",
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
        processed_params_list: list[tuple[Any, ...]] = []
        param_sequence = parameters if parameters is not None else []

        if param_sequence:
            # Create a building config that skips validation for individual parameter sets
            building_config = replace(config or StatementConfig(), enable_validation=False)

            for param_set in param_sequence:
                item_stmt = SQLStatement(
                    template_stmt.sql,  # Use processed SQL from template
                    param_set,
                    dialect=self.dialect,
                    statement_config=building_config,
                )
                ordered_params_for_item = item_stmt.get_parameters(style=placeholder_style)

                if isinstance(ordered_params_for_item, list):
                    processed_params_list.append(tuple(ordered_params_for_item))
                elif ordered_params_for_item is None:
                    processed_params_list.append(())
                # Convert to tuple
                elif isinstance(ordered_params_for_item, Iterable) and not isinstance(
                    ordered_params_for_item, (str, bytes)
                ):
                    processed_params_list.append(tuple(ordered_params_for_item))
                else:
                    processed_params_list.append((ordered_params_for_item,))

        if not param_sequence:
            return ExecuteResult(raw_result=cast("dict[str, Any]", {}), rows_affected=0, operation_type="EXECUTE")

        await conn.executemany(final_sql, processed_params_list)

        rows_affected = len(processed_params_list) if processed_params_list else 0

        operation_type = "EXECUTE"
        if template_stmt.expression and hasattr(template_stmt.expression, "key"):
            operation_type = str(template_stmt.expression.key).upper()

        return ExecuteResult(
            raw_result=cast("dict[str, Any]", {}),
            rows_affected=rows_affected,
            operation_type=operation_type,
        )

    async def execute_script(
        self,
        statement: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        statement_config: Optional[StatementConfig] = None,
        **kwargs: Any,
    ) -> str:
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

        status: str = await conn.execute(final_sql)
        return status

    def _connection(self, connection: "Optional[AsyncpgConnection]" = None) -> "AsyncpgConnection":
        """Return the connection to use. If None, use the default connection."""
        return connection if connection is not None else self.connection
