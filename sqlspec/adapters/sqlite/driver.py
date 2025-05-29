import contextlib
import logging
import sqlite3
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from sqlspec.base import SyncDriverAdapterProtocol
from sqlspec.sql.filters import StatementFilter
from sqlspec.sql.mixins import ResultConverter, SQLTranslatorMixin
from sqlspec.sql.parameters import ParameterStyle
from sqlspec.sql.result import ExecuteResult, SelectResult
from sqlspec.sql.statement import (
    SQLStatement,
    Statement,
    StatementConfig,
)
from sqlspec.typing import StatementParameterType

if TYPE_CHECKING:
    from sqlspec.sql.filters import StatementFilter

__all__ = ("SqliteConnection", "SqliteDriver")

logger = logging.getLogger("sqlspec")

SqliteConnection = sqlite3.Connection


class SqliteDriver(
    SQLTranslatorMixin["SqliteConnection"],
    SyncDriverAdapterProtocol["SqliteConnection"],
    ResultConverter,
):
    """SQLite Sync Driver Adapter.

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

    connection: SqliteConnection
    __supports_arrow__: ClassVar[bool] = False
    dialect: str = "sqlite"
    statement_config: Optional[StatementConfig]

    def __init__(self, connection: "SqliteConnection", statement_config: Optional[StatementConfig] = None) -> None:
        """Initialize the SQLite driver adapter."""
        super().__init__(connection, statement_config=statement_config)

    def _get_placeholder_style(self) -> ParameterStyle:  # noqa: PLR6301
        """Return the placeholder style for SQLite."""
        return ParameterStyle.QMARK

    @staticmethod
    @contextmanager
    def _with_cursor(connection: "SqliteConnection") -> Iterator[sqlite3.Cursor]:
        """Provide cursor with automatic cleanup.

        Args:
            connection: The SQLite connection to create cursor from.

        Yields:
            sqlite3.Cursor: The database cursor.
        """
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            with contextlib.suppress(Exception):
                cursor.close()

    def execute(
        self,
        statement: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional["SqliteConnection"] = None,
        statement_config: Optional[StatementConfig] = None,
        **kwargs: Any,
    ) -> "Union[SelectResult[dict[str, Any]], ExecuteResult[dict[str, Any]]]":
        """Execute a SQL statement and return a StatementResult.

        This is the unified method for all SQL operations (SELECT, INSERT, UPDATE, DELETE).
        Use the StatementResult methods to extract the data you need.

        Args:
            statement: The SQL statement to execute.
            parameters: Parameters for the statement.
            *filters: Statement filters to apply.
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

        # Convert parameters to tuple format for SQLite using simplified logic
        db_params: tuple[Any, ...] = ()
        if ordered_params is not None:
            if isinstance(ordered_params, list) or (
                isinstance(ordered_params, Iterable) and not isinstance(ordered_params, (str, bytes))
            ):
                db_params = tuple(ordered_params)
            else:
                db_params = (ordered_params,)

        with self._with_cursor(conn) as cursor:
            cursor.execute(final_sql, db_params)

            if self.returns_rows(stmt.expression):
                raw_data_tuples = cursor.fetchall()
                column_names = [col[0] for col in cursor.description or []]
                rows = [dict(zip(column_names, row)) for row in raw_data_tuples]
                raw_result_data = rows[0] if rows else cast("dict[str, Any]", {})
                return SelectResult(rows=rows, column_names=column_names, raw_result=raw_result_data)

            rowcount = getattr(cursor, "rowcount", -1)

            operation_type = "UNKNOWN"
            if stmt.expression and hasattr(stmt.expression, "key"):
                operation_type = str(stmt.expression.key).upper()

            return ExecuteResult(
                raw_result=cast("dict[str, Any]", {}),
                rows_affected=rowcount,
                operation_type=operation_type,
                last_inserted_id=getattr(cursor, "lastrowid", None),
            )

    def execute_many(
        self,
        statement: "Statement",
        parameters: Optional[Sequence[StatementParameterType]] = None,
        *filters: "StatementFilter",
        connection: Optional["SqliteConnection"] = None,
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
                ordered_params_for_item = item_stmt.get_parameters(style=self._get_placeholder_style())

                if isinstance(ordered_params_for_item, list):
                    processed_params_list.append(tuple(ordered_params_for_item))
                elif ordered_params_for_item is None:
                    processed_params_list.append(())
                elif isinstance(ordered_params_for_item, Iterable) and not isinstance(
                    ordered_params_for_item, (str, bytes)
                ):
                    processed_params_list.append(tuple(ordered_params_for_item))
                else:
                    processed_params_list.append((ordered_params_for_item,))

        with self._with_cursor(conn) as cursor:
            if not param_sequence:
                total_affected = 0
            else:
                cursor.executemany(final_sql, processed_params_list)
                total_affected = getattr(cursor, "rowcount", -1)
                if total_affected == -1 and processed_params_list:
                    total_affected = len(processed_params_list)

            operation_type = "EXECUTE"
            if template_stmt.expression and hasattr(template_stmt.expression, "key"):
                operation_type = str(template_stmt.expression.key).upper()

            return ExecuteResult(
                raw_result=cast("dict[str, Any]", {}), rows_affected=total_affected, operation_type=operation_type
            )

    def execute_script(
        self,
        statement: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional["SqliteConnection"] = None,
        statement_config: Optional[StatementConfig] = None,
        **kwargs: Any,
    ) -> str:
        """Execute a multi-statement SQL script.

        For script execution, parameters are rendered as static literals directly
        in the SQL rather than using placeholders, since SQLite's executescript
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

        with self._with_cursor(conn) as cursor:
            cursor.executescript(final_sql)
            return "SCRIPT EXECUTED"

    def _connection(self, connection: Optional["SqliteConnection"] = None) -> "SqliteConnection":
        return connection or self.connection
