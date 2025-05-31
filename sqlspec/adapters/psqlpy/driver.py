"""Psqlpy Driver Implementation."""

import logging
from collections.abc import Iterable, Sequence
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union

from psqlpy import Connection, QueryResult

from sqlspec.driver import AsyncDriverAdapterProtocol
from sqlspec.statement.builder import QueryBuilder
from sqlspec.statement.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ExecuteResult, SelectResult
from sqlspec.statement.sql import SQL, SQLConfig, Statement
from sqlspec.typing import DictRow, ModelDTOT, SQLParameterType

if TYPE_CHECKING:
    from sqlspec.statement.filters import StatementFilter

__all__ = ("PsqlpyConnection", "PsqlpyDriver")

PsqlpyConnection = Connection  # type: ignore[misc]
logger = logging.getLogger("sqlspec")


class PsqlpyDriver(
    SQLTranslatorMixin["PsqlpyConnection"],
    AsyncDriverAdapterProtocol["PsqlpyConnection"],
    AsyncArrowMixin["PsqlpyConnection"],
    ResultConverter,
):
    """Psqlpy Postgres Driver Adapter.

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

    connection: "PsqlpyConnection"
    __supports_arrow__: ClassVar[bool] = False  # psqlpy doesn't support Arrow natively
    dialect: str = "postgres"

    def __init__(self, connection: "PsqlpyConnection", config: Optional[SQLConfig] = None) -> None:
        """Initialize the psqlpy driver adapter."""
        super().__init__(connection, config=config)

    def _get_parameter_style(self) -> ParameterStyle:
        """Return the placeholder style for PostgreSQL ($1, $2, etc.)."""
        return ParameterStyle.NUMERIC

    async def execute(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: Optional[SQLParameterType] = None,
        *filters: "StatementFilter",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        connection: Optional[PsqlpyConnection] = None,
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
        if schema_type is not None:
            logger.warning("schema_type parameter is not yet fully implemented for psqlpy driver's execute method.")

        conn = self._connection(connection)
        effective_config = config or self.config

        # Process the input statement
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

        # Convert parameters to list format for psqlpy using simplified logic
        params_for_psqlpy = None
        if ordered_params is not None:
            if isinstance(ordered_params, list):
                params_for_psqlpy = ordered_params
            elif isinstance(ordered_params, Iterable) and not isinstance(ordered_params, (str, bytes)):
                params_for_psqlpy = list(ordered_params)
            else:
                params_for_psqlpy = [ordered_params]

        if self.returns_rows(current_stmt.expression):
            query_result: QueryResult = await conn.fetch(final_sql, parameters=params_for_psqlpy)
            dict_rows: list[DictRow] = query_result.result()
            if not dict_rows:
                # Ensure raw_result is an empty dict for SelectResult[DictRow] if no rows
                return SelectResult(rows=[], column_names=[], raw_result={})

            column_names = list(dict_rows[0].keys())
            return SelectResult(
                rows=dict_rows,
                column_names=column_names,
                raw_result=dict_rows[0],  # First row as raw_result for SelectResult[DictRow]
            )

        query_result_dml: QueryResult = await conn.execute(final_sql, parameters=params_for_psqlpy)

        # Try to get affected rows from the query result - simplified
        rows_affected = (
            getattr(query_result_dml, "affected_rows", None)
            or getattr(query_result_dml, "row_count", None)
            or getattr(query_result_dml, "rowcount", -1)
        )

        operation_type = "UNKNOWN"
        if current_stmt.expression and hasattr(current_stmt.expression, "key"):
            operation_type = str(current_stmt.expression.key).upper()

        return ExecuteResult(
            raw_result={},
            rows_affected=rows_affected,
            operation_type=operation_type,
        )

    async def execute_many(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[ExecuteResult[Any]]]",
        parameters: Optional[Sequence[SQLParameterType]] = None,
        *filters: "StatementFilter",
        connection: Optional[PsqlpyConnection] = None,
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
            >>> await driver.execute_many(
            ...     "INSERT INTO users (name, email) VALUES ($1, $2)",
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
            template_sql_input = statement.to_statement(config=effective_config).sql
        elif isinstance(statement, SQL):
            template_sql_input = statement.sql
        else:  # Raw Statement
            template_sql_input = statement

        # Create template statement with filters for validation
        template_stmt = SQL(
            template_sql_input,
            None,  # No parameters for template itself
            *filters,
            dialect=self.dialect,
            config=effective_config,
            **kwargs,
        )

        template_stmt.validate()
        parameter_style = self._get_parameter_style()
        final_sql = template_stmt.to_sql(placeholder_style=parameter_style)
        param_sequence = parameters if parameters is not None else []

        if not param_sequence:
            return ExecuteResult(raw_result={}, rows_affected=0, operation_type="EXECUTE")

        # Process parameter sets individually - simplified
        total_rows_affected = 0
        # psqlpy executemany is available but the driver here executes one by one.
        # Keeping existing logic for now, but this could be a point of optimization if psqlpy.executemany is suitable.
        building_config = replace(effective_config, enable_validation=False)

        for param_set in param_sequence:
            item_stmt = SQL(
                template_stmt.sql,  # Use processed SQL from template
                param_set,
                dialect=self.dialect,
                config=building_config,
            )
            item_params = item_stmt.get_parameters(style=parameter_style)
            item_params_for_psqlpy = (
                item_params if isinstance(item_params, list) else ([item_params] if item_params is not None else None)
            )

            current_qr: QueryResult = await conn.execute(final_sql, parameters=item_params_for_psqlpy)

            # Simplified affected rows detection
            current_affected = (
                getattr(current_qr, "affected_rows", None)
                or getattr(current_qr, "row_count", None)
                or getattr(current_qr, "rowcount", None)
            )

            if current_affected and current_affected != -1:
                total_rows_affected += current_affected
            elif template_stmt.expression and not self.returns_rows(template_stmt.expression):
                total_rows_affected += 1  # Heuristic for DML operations

        operation_type = "EXECUTE"  # Default operation type
        if template_stmt.expression and hasattr(template_stmt.expression, "key"):
            operation_type = str(template_stmt.expression.key).upper()

        return ExecuteResult(
            raw_result={},
            rows_affected=total_rows_affected,
            operation_type=operation_type,
        )

    async def execute_script(
        self,
        statement: "Union[str, SQL]",
        parameters: Optional[SQLParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[PsqlpyConnection] = None,
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

        # Process the input statement
        script_stmt: SQL
        if isinstance(statement, SQL):
            script_stmt = SQL(statement, merged_params, *filters, dialect=self.dialect, config=config)  # type: ignore[arg-type]
        else:  # Raw string
            script_stmt = SQL(statement, merged_params, *filters, dialect=self.dialect, config=config)

        script_stmt.validate()
        final_sql = script_stmt.to_sql(placeholder_style=ParameterStyle.STATIC)

        await conn.execute(final_sql, parameters=None)
        return "SCRIPT EXECUTED"

    def _connection(self, connection: "Optional[PsqlpyConnection]" = None) -> "PsqlpyConnection":
        """Get the connection to use for the operation."""
        return connection or self.connection
