import logging
from collections.abc import Generator, Iterable, Sequence
from contextlib import contextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from duckdb import DuckDBPyConnection

from sqlspec.base import SyncDriverAdapterProtocol
from sqlspec.exceptions import SQLConversionError
from sqlspec.sql.mixins import ResultConverter, SQLTranslatorMixin, SyncArrowMixin
from sqlspec.sql.parameters import ParameterStyle
from sqlspec.sql.result import ArrowResult, ExecuteResult, SelectResult
from sqlspec.sql.statement import SQLStatement, Statement, StatementConfig
from sqlspec.typing import StatementParameterType

if TYPE_CHECKING:
    from sqlspec.sql.filters import StatementFilter

__all__ = ("DuckDBConnection", "DuckDBDriver")

logger = logging.getLogger("sqlspec")

DuckDBConnection = DuckDBPyConnection


class DuckDBDriver(
    SyncArrowMixin["DuckDBConnection"],
    SQLTranslatorMixin["DuckDBConnection"],
    SyncDriverAdapterProtocol["DuckDBConnection"],
    ResultConverter,
):
    """DuckDB Sync Driver Adapter.

    This driver implements the new unified SQLSpec protocol with the core 3 methods:
    - execute() - Universal method for all SQL operations
    - execute_many() - Batch operations
    - execute_script() - Multi-statement scripts

    Enhanced Features:
    - Full SQLStatement integration with filters and validation
    - DuckDB-specific parameter style handling (qmark: ?)
    - Comprehensive parameter validation and security checks
    - Support for filter composition (pagination, search, etc.)
    - Native Arrow support for high-performance analytics
    """

    connection: "DuckDBConnection"
    __supports_arrow__: ClassVar[bool] = True  # DuckDB has excellent Arrow support
    use_cursor: bool = True
    dialect: str = "duckdb"

    def __init__(
        self,
        connection: "DuckDBConnection",
        statement_config: Optional[StatementConfig] = None,
        use_cursor: bool = True,
    ) -> None:
        """Initialize the DuckDB driver adapter."""
        super().__init__(connection, statement_config=statement_config)

    @contextmanager
    def _get_cursor(self, connection: Optional[DuckDBConnection] = None) -> Generator[DuckDBPyConnection, None, None]:
        """Get a cursor for the connection."""
        conn_to_use = connection or self.connection
        cursor = conn_to_use.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def _get_placeholder_style(self) -> ParameterStyle:
        """Return the placeholder style for DuckDB (qmark: ?)."""
        return ParameterStyle.QMARK

    def execute(
        self,
        statement: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[DuckDBConnection] = None,
        statement_config: Optional[StatementConfig] = None,
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
            >>> result = driver.execute(
            ...     "SELECT * FROM users WHERE id = ?", [123]
            ... )
            >>> # Query with filters
            >>> result = driver.execute(
            ...     "SELECT * FROM users",
            ...     LimitOffset(limit=10, offset=0),
            ...     SearchFilter(field_name="name", value="John"),
            ... )
        """
        conn = self._connection(connection)
        config = statement_config or self.statement_config

        stmt = SQLStatement(statement, parameters, *filters, dialect=self.dialect, statement_config=config, **kwargs)
        stmt.validate()

        final_sql = stmt.to_sql(placeholder_style=self._get_placeholder_style())
        ordered_params = stmt.get_parameters(style=self._get_placeholder_style())

        # Convert parameters to list format for DuckDB using simplified logic
        if ordered_params is not None and not isinstance(ordered_params, list):
            if isinstance(ordered_params, Iterable) and not isinstance(ordered_params, (str, bytes)):
                ordered_params = list(ordered_params)
            else:
                ordered_params = [ordered_params]

        with self._get_cursor(conn) as cursor:
            cursor.execute(final_sql, ordered_params or [])
            if self.returns_rows(stmt.expression):
                raw_data_tuples = cursor.fetchall()
                if not raw_data_tuples:
                    return SelectResult(raw_result=cast("dict[str, Any]", {}), rows=[], column_names=[])
                column_names = [col[0] for col in cursor.description or []]
                rows = [dict(zip(column_names, row)) for row in raw_data_tuples]
                return SelectResult(
                    raw_result=rows[0] if rows else cast("dict[str, Any]", {}),
                    rows=rows,
                    column_names=column_names,
                )

            rows_affected = cursor.rowcount if cursor.rowcount is not None else -1
            operation_type = "UNKNOWN"
            if stmt.expression and hasattr(stmt.expression, "key"):
                operation_type = str(stmt.expression.key).upper()

            return ExecuteResult(
                raw_result=cast("dict[str, Any]", {}),
                rows_affected=rows_affected,
                operation_type=operation_type,
            )

    def execute_many(
        self,
        statement: "Statement",
        parameters: Optional[Sequence[StatementParameterType]] = None,
        *filters: "StatementFilter",
        connection: Optional[DuckDBConnection] = None,
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
            >>> driver.execute_many(
            ...     "INSERT INTO users (name, email) VALUES (?, ?)",
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

        template_stmt.validate()
        final_sql = template_stmt.to_sql(placeholder_style=self._get_placeholder_style())

        # Process parameter sets
        processed_params_list: list[list[Any]] = []
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
                ordered_params_for_item = item_stmt.get_parameters(style=self._get_placeholder_style())

                if isinstance(ordered_params_for_item, list):
                    processed_params_list.append(ordered_params_for_item)
                elif ordered_params_for_item is None:
                    processed_params_list.append([])
                elif isinstance(ordered_params_for_item, Iterable) and not isinstance(
                    ordered_params_for_item, (str, bytes)
                ):
                    processed_params_list.append(list(ordered_params_for_item))
                else:
                    processed_params_list.append([ordered_params_for_item])

        if not param_sequence:
            return ExecuteResult(raw_result=cast("dict[str, Any]", {}), rows_affected=0, operation_type="EXECUTE")

        with self._get_cursor(conn) as cursor:
            cursor.executemany(final_sql, processed_params_list)
            rows_affected = cursor.rowcount if cursor.rowcount is not None else -1
            if rows_affected == -1 and processed_params_list:
                rows_affected = len(processed_params_list)

        operation_type = "EXECUTE"
        if template_stmt.expression and hasattr(template_stmt.expression, "key"):
            operation_type = str(template_stmt.expression.key).upper()

        return ExecuteResult(
            raw_result=cast("dict[str, Any]", {}),
            rows_affected=rows_affected,
            operation_type=operation_type,
        )

    def execute_script(
        self,
        statement: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[DuckDBConnection] = None,
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
        final_sql = stmt.to_sql(placeholder_style=ParameterStyle.STATIC)

        with self._get_cursor(conn) as cursor:
            cursor.execute(final_sql)
            return "SCRIPT EXECUTED"

    def select_to_arrow(
        self,
        statement: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[DuckDBConnection] = None,
        statement_config: Optional[StatementConfig] = None,
        **kwargs: Any,
    ) -> "ArrowResult":
        """Execute a SELECT statement and return results as an Apache Arrow Table.

        This method leverages DuckDB's excellent Arrow integration for high-performance
        analytics workloads.

        Args:
            statement: The SQL query to execute.
            parameters: Parameters for the query.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            statement_config: Optional statement configuration.
            **kwargs: Additional keyword arguments.

        Raises:
            TypeError: If the SQL statement is not a valid query.

        Returns:
            An Arrow Table containing the query results.
        """
        conn = self._connection(connection)
        config = statement_config or self.statement_config

        stmt = SQLStatement(statement, parameters, *filters, dialect=self.dialect, statement_config=config, **kwargs)
        stmt.validate()

        if not self.returns_rows(stmt.expression):
            op_type = "UNKNOWN"
            if stmt.expression and hasattr(stmt.expression, "key"):
                op_type = str(stmt.expression.key).upper()
            msg = f"Cannot fetch Arrow table for a non-query statement. Command type: {op_type}"
            raise TypeError(msg)

        final_sql = stmt.to_sql(placeholder_style=self._get_placeholder_style())
        ordered_params = stmt.get_parameters(style=self._get_placeholder_style())

        # Convert parameters to list format for DuckDB using simplified logic
        if ordered_params is not None and not isinstance(ordered_params, list):
            if isinstance(ordered_params, Iterable) and not isinstance(ordered_params, (str, bytes)):
                ordered_params = list(ordered_params)
            else:
                ordered_params = [ordered_params]

        with self._get_cursor(conn) as cursor:
            relation = cursor.execute(final_sql, ordered_params or [])
            if relation is None:
                msg = "DuckDB execute returned None for a query expected to return rows for Arrow."
                raise SQLConversionError(msg)
            return ArrowResult(raw_result=relation.arrow())

    def _connection(self, connection: Optional[DuckDBConnection] = None) -> DuckDBConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection
