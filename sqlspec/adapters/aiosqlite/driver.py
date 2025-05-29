# ruff: noqa: PLR6301
import logging
from collections.abc import Iterable, Sequence
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

import aiosqlite

from sqlspec.base import AsyncDriverAdapterProtocol
from sqlspec.sql.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin
from sqlspec.sql.parameters import ParameterStyle
from sqlspec.sql.result import ExecuteResult, SelectResult
from sqlspec.sql.statement import SQLStatement, Statement, StatementConfig

if TYPE_CHECKING:
    from sqlspec.sql.filters import StatementFilter
    from sqlspec.typing import StatementParameterType

__all__ = ("AiosqliteConnection", "AiosqliteDriver")

logger = logging.getLogger("sqlspec")

AiosqliteConnection = aiosqlite.Connection


class AiosqliteDriver(
    SQLTranslatorMixin["AiosqliteConnection"],
    AsyncDriverAdapterProtocol[AiosqliteConnection],
    AsyncArrowMixin["AiosqliteConnection"],
    ResultConverter,
):
    """SQLite Async Driver Adapter.

    This driver implements the new unified SQLSpec protocol with the core 3 methods:
    - execute() - Universal method for all SQL operations
    - execute_many() - Batch operations
    - execute_script() - Multi-statement scripts

    Enhanced Features:
    - Full SQLStatement integration with filters and validation
    - SQLite-specific parameter style handling (qmark: ?)
    - Comprehensive parameter validation and security checks
    - Support for filter composition (pagination, search, etc.)
    """

    connection: "AiosqliteConnection"
    __supports_arrow__: ClassVar[bool] = False  # SQLite doesn't support Arrow natively
    dialect: str = "sqlite"

    def __init__(self, connection: AiosqliteConnection, statement_config: Optional[StatementConfig] = None) -> None:
        """Initialize the aiosqlite driver adapter."""
        super().__init__(connection, statement_config=statement_config)

    def _get_placeholder_style(self) -> ParameterStyle:
        """Return the placeholder style for SQLite (qmark: ?)."""
        return ParameterStyle.QMARK

    async def execute(
        self,
        statement: "Statement",
        parameters: Optional["StatementParameterType"] = None,
        *filters: "StatementFilter",
        connection: Optional[AiosqliteConnection] = None,
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
            >>> result = await driver.execute(
            ...     "SELECT * FROM users WHERE id = ?", [123]
            ... )
            >>> # Query with filters
            >>> result = await driver.execute(
            ...     "SELECT * FROM users",
            ...     LimitOffset(limit=10, offset=0),
            ...     SearchFilter(field_name="name", value="John"),
            ... )
        """
        conn: AiosqliteConnection = self._connection(connection)
        conn.row_factory = aiosqlite.Row
        config = statement_config or self.statement_config

        stmt = SQLStatement(statement, parameters, *filters, dialect=self.dialect, statement_config=config, **kwargs)

        stmt.validate()
        placeholder_style = self._get_placeholder_style()
        final_sql = stmt.to_sql(placeholder_style=placeholder_style)
        ordered_params = stmt.get_parameters(style=placeholder_style)

        # Convert parameters to tuple format for aiosqlite using simplified logic
        db_params_tuple: tuple[Any, ...] = ()
        if ordered_params is not None:
            if isinstance(ordered_params, list) or (
                isinstance(ordered_params, Iterable) and not isinstance(ordered_params, (str, bytes))
            ):
                db_params_tuple = tuple(ordered_params)
            else:
                db_params_tuple = (ordered_params,)

        cursor: Optional[aiosqlite.Cursor] = None
        try:
            cursor = await conn.cursor()
            logger.debug("Executing SQL (aiosqlite): %s with params: %s", final_sql, db_params_tuple)
            await cursor.execute(final_sql, db_params_tuple)

            if self.returns_rows(stmt.expression):
                raw_rows: list[aiosqlite.Row] = list(await cursor.fetchall())
                column_names = [d[0] for d in cursor.description or []]
                dict_rows = [dict(row) for row in raw_rows]
                raw_select_result_data = dict_rows[0] if dict_rows else cast("dict[str, Any]", {})
                return SelectResult(
                    rows=dict_rows,
                    column_names=column_names,
                    raw_result=raw_select_result_data,
                )

            affected_rows = cursor.rowcount
            last_id = cursor.lastrowid
            await conn.commit()

            operation_type = "UNKNOWN"
            if stmt.expression and hasattr(stmt.expression, "key"):
                operation_type = str(stmt.expression.key).upper()

            return ExecuteResult(
                raw_result=cast("dict[str, Any]", {}),
                rows_affected=affected_rows,
                last_inserted_id=last_id,
                operation_type=operation_type,
            )
        finally:
            if cursor:
                await cursor.close()

    async def execute_many(
        self,
        statement: "Statement",
        parameters: Optional[Sequence["StatementParameterType"]] = None,
        *filters: "StatementFilter",
        connection: Optional[AiosqliteConnection] = None,
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
            ...     "INSERT INTO users (name, email) VALUES (?, ?)",
            ...     [
            ...         ["John", "john@example.com"],
            ...         ["Jane", "jane@example.com"],
            ...     ],
            ... )
        """
        conn: AiosqliteConnection = self._connection(connection)
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
            logger.debug("execute_many called with no parameters for SQL: %s", final_sql)
            return ExecuteResult(raw_result=cast("dict[str, Any]", {}), rows_affected=0, operation_type="EXECUTE")

        logger.debug(
            "Executing SQL (aiosqlite) many: %s with %s param sets.",
            final_sql,
            len(processed_params_list),
        )

        affected_rows: int = -1
        cursor: Optional[aiosqlite.Cursor] = None
        try:
            cursor = await conn.cursor()
            await cursor.executemany(final_sql, processed_params_list)
            affected_rows = cursor.rowcount
            if affected_rows == -1 and processed_params_list:
                affected_rows = len(processed_params_list)
            await conn.commit()
        finally:
            if cursor:
                await cursor.close()

        operation_type = "EXECUTE"
        if template_stmt.expression and hasattr(template_stmt.expression, "key"):
            operation_type = str(template_stmt.expression.key).upper()

        return ExecuteResult(
            raw_result=cast("dict[str, Any]", {}),
            rows_affected=affected_rows,
            operation_type=operation_type,
        )

    async def execute_script(
        self,
        statement: "Statement",
        parameters: Optional["StatementParameterType"] = None,
        *filters: "StatementFilter",
        connection: Optional[AiosqliteConnection] = None,
        statement_config: Optional[StatementConfig] = None,
        **kwargs: Any,
    ) -> str:
        """Execute a multi-statement SQL script.

        For script execution, parameters are rendered as static literals directly
        in the SQL rather than using placeholders, since aiosqlite's executescript
        doesn't support parameter binding.

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
        conn: AiosqliteConnection = self._connection(connection)
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

        logger.debug("Executing script (aiosqlite): %s", final_sql)
        cursor: Optional[aiosqlite.Cursor] = None
        try:
            cursor = await conn.cursor()
            await cursor.executescript(final_sql)
            await conn.commit()
        finally:
            if cursor:
                await cursor.close()
        return "SCRIPT EXECUTED"

    def _connection(self, connection: Optional[AiosqliteConnection] = None) -> AiosqliteConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection
