import logging
from collections.abc import Generator, Iterable, Sequence
from contextlib import contextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union

from duckdb import DuckDBPyConnection

from sqlspec.driver import SyncDriverAdapterProtocol
from sqlspec.exceptions import SQLConversionError
from sqlspec.statement.builder import QueryBuilder
from sqlspec.statement.mixins import ResultConverter, SQLTranslatorMixin, SyncArrowMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, ExecuteResult, SelectResult
from sqlspec.statement.sql import SQL, SQLConfig, Statement
from sqlspec.typing import DictRow, ModelDTOT, SQLParameterType

if TYPE_CHECKING:
    from pyarrow import Table as ArrowTable

    from sqlspec.statement.filters import StatementFilter

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
        config: Optional[SQLConfig] = None,
        use_cursor: bool = True,
    ) -> None:
        """Initialize the DuckDB driver adapter."""
        super().__init__(connection, config=config)

    @contextmanager
    def _get_cursor(self, connection: Optional[DuckDBConnection] = None) -> Generator[DuckDBPyConnection, None, None]:
        conn_to_use = connection or self.connection
        cursor = conn_to_use.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def _get_parameter_style(self) -> ParameterStyle:
        return ParameterStyle.QMARK

    def execute(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: Optional[SQLParameterType] = None,
        *filters: "StatementFilter",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        connection: Optional[DuckDBConnection] = None,
        config: Optional[SQLConfig] = None,
        **kwargs: Any,
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
        effective_config = config or self.config

        # Process the input statement
        if isinstance(statement, QueryBuilder):
            # If QueryBuilder, convert to SQL object first, respecting its config
            # The QueryBuilder's .to_statement() will produce an SQL object with its own parameters and structure.
            # We then use this as the base for applying driver-level filters and parameters.
            base_sql_obj = statement.to_statement(config=effective_config)
            # Re-initialize SQL with the builder's output, but allow new parameters/filters for this execution context
            stmt = SQL(base_sql_obj, parameters, *filters, dialect=self.dialect, config=effective_config, **kwargs)
        else:
            # It's a raw Statement (str or sqlglot.Expression)
            stmt = SQL(statement, parameters, *filters, dialect=self.dialect, config=effective_config, **kwargs)

        stmt.validate()

        final_sql = stmt.to_sql(placeholder_style=self._get_parameter_style())
        ordered_params = stmt.get_parameters(style=self._get_parameter_style())

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
                column_names = [col[0] for col in cursor.description or []]
                if not raw_data_tuples:
                    return SelectResult(raw_result={}, rows=[], column_names=column_names)
                rows = [dict(zip(column_names, row)) for row in raw_data_tuples]
                return SelectResult(
                    raw_result=rows[0] if rows else {},
                    rows=rows,
                    column_names=column_names,
                )

            rows_affected = cursor.rowcount if cursor.rowcount is not None else -1
            operation_type = "UNKNOWN"
            if stmt.expression and hasattr(stmt.expression, "key"):
                operation_type = str(stmt.expression.key).upper()

            return ExecuteResult(
                raw_result={},
                rows_affected=rows_affected,
                operation_type=operation_type,
            )

    def execute_many(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[ExecuteResult[Any]]]",
        parameters: Optional[Sequence[SQLParameterType]] = None,
        *filters: "StatementFilter",
        connection: Optional[DuckDBConnection] = None,
        config: Optional[SQLConfig] = None,
        **kwargs: Any,
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
            >>> driver.execute_many(
            ...     "INSERT INTO users (name, email) VALUES (?, ?)",
            ...     [
            ...         ["John", "john@example.com"],
            ...         ["Jane", "jane@example.com"],
            ...     ],
            ... )
        """
        conn = self._connection(connection)
        effective_config = config or self.config

        # Process the input statement for the template
        template_sql_input: Statement
        if isinstance(statement, QueryBuilder):
            # For execute_many, the initial statement (template) should not have parameters itself.
            # Parameters are applied per item in the batch.
            template_sql_input = statement.to_statement(config=effective_config).sql  # Get the SQL string
        elif isinstance(statement, SQL):
            template_sql_input = statement.sql  # Get the SQL string
        else:
            template_sql_input = statement  # It's a raw Statement (str or sqlglot.Expression)

        # Create template statement with filters for validation
        # Parameters are not passed to the template_stmt for execute_many
        template_stmt = SQL(
            template_sql_input,
            None,  # No parameters for template itself
            *filters,
            dialect=self.dialect,
            config=effective_config,
            **kwargs,
        )
        template_stmt.validate()
        final_sql = template_stmt.to_sql(placeholder_style=self._get_parameter_style())

        # Process parameter sets
        processed_params_list: list[list[Any]] = []
        param_sequence = parameters if parameters is not None else []

        if param_sequence:
            # Create a building config that skips validation for individual parameter sets
            # as the template was already validated.
            building_config = replace(effective_config, enable_validation=False)

            for param_set in param_sequence:
                # For each parameter set, we create a temporary SQL object
                # using the template_stmt's raw SQL. This is primarily for parameter processing.
                item_stmt = SQL(
                    template_stmt.sql,  # Use processed SQL string from template
                    param_set,
                    dialect=self.dialect,
                    config=building_config,
                )
                ordered_params_for_item = item_stmt.get_parameters(style=self._get_parameter_style())

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

        if not processed_params_list:
            return ExecuteResult(raw_result={}, rows_affected=0, operation_type="EXECUTE")

        with self._get_cursor(conn) as cursor:
            cursor.executemany(final_sql, processed_params_list)
            rows_affected = cursor.rowcount if cursor.rowcount is not None else -1
            if rows_affected == -1 and processed_params_list:
                rows_affected = len(processed_params_list)

        operation_type = "EXECUTE"
        if template_stmt.expression and hasattr(template_stmt.expression, "key"):
            operation_type = str(template_stmt.expression.key).upper()

        return ExecuteResult(
            raw_result={},
            rows_affected=rows_affected,
            operation_type=operation_type,
        )

    def execute_script(
        self,
        statement: "Statement",
        parameters: Optional[SQLParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[DuckDBConnection] = None,
        config: Optional[SQLConfig] = None,
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
            config: Optional statement configuration.
            **kwargs: Additional keyword arguments.

        Returns:
            A string with execution results/output.
        """
        conn = self._connection(connection)
        config = config or self.config

        merged_params = parameters
        if kwargs:
            if merged_params is None:
                merged_params = kwargs
            elif isinstance(merged_params, dict):
                merged_params = {**merged_params, **kwargs}

        stmt = SQL(statement, merged_params, *filters, dialect=self.dialect, config=config)
        stmt.validate()
        final_sql = stmt.to_sql(placeholder_style=ParameterStyle.STATIC)

        with self._get_cursor(conn) as cursor:
            cursor.execute(final_sql)
            return "SCRIPT EXECUTED"

    def select_to_arrow(
        self,
        statement: "Statement",
        parameters: Optional[SQLParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[DuckDBConnection] = None,
        config: Optional[SQLConfig] = None,
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
            config: Optional statement configuration.
            **kwargs: Additional keyword arguments.

        Raises:
            TypeError: If the SQL statement is not a valid query.
            SQLConversionError: If the DuckDB execute returned None for a query expected to return rows for Arrow.

        Returns:
            An Arrow Table containing the query results.
        """
        conn = self._connection(connection)
        config = config or self.config

        stmt = SQL(statement, parameters, *filters, dialect=self.dialect, config=config, **kwargs)
        stmt.validate()

        if not self.returns_rows(stmt.expression):
            op_type = "UNKNOWN"
            if stmt.expression and hasattr(stmt.expression, "key"):
                op_type = str(stmt.expression.key).upper()
            msg = f"Cannot fetch Arrow table for a non-query statement. Command type: {op_type}"
            raise TypeError(msg)

        final_sql = stmt.to_sql(placeholder_style=self._get_parameter_style())
        ordered_params = stmt.get_parameters(style=self._get_parameter_style())

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
            arrow_table: ArrowTable = relation.arrow()
            return ArrowResult(raw_result=arrow_table)

    def _connection(self, connection: Optional[DuckDBConnection] = None) -> DuckDBConnection:
        return connection or self.connection
