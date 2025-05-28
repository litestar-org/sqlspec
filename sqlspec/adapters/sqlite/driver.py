import contextlib
import logging
import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from sqlglot import exp

from sqlspec.base import SyncDriverAdapterProtocol
from sqlspec.sql.filters import StatementFilter, apply_filter
from sqlspec.sql.mixins import ResultConverter, SQLTranslatorMixin
from sqlspec.sql.parameters import ParameterStyle
from sqlspec.sql.result import ExecuteResult, SelectResult
from sqlspec.sql.statement import SQLSanitizer, SQLStatement, SQLValidator, Statement
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
    """

    connection: SqliteConnection
    __supports_arrow__: ClassVar[bool] = False
    dialect: str = "sqlite"

    def __init__(self, connection: "SqliteConnection") -> None:
        """Initialize the SQLite driver adapter."""
        super().__init__(connection)

    def _get_placeholder_style(self) -> ParameterStyle:
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
        sql: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional["SqliteConnection"] = None,
        validator: Optional["SQLValidator"] = None,
        sanitizer: Optional["SQLSanitizer"] = None,
        **kwargs: Any,
    ) -> "Union[SelectResult[dict[str, Any]], ExecuteResult[dict[str, Any]]]":
        """Execute a SQL statement and return a StatementResult.

        This is the unified method for all SQL operations (SELECT, INSERT, UPDATE, DELETE).
        Use the StatementResult methods to extract the data you need.

        Args:
            sql: The SQL statement to execute.
            parameters: Parameters for the statement.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            validator: Optional validator for the statement.
            sanitizer: Optional sanitizer for the statement.
            **kwargs: Additional keyword arguments.

        Returns:
            A StatementResult containing the operation results.
        """
        conn = self._connection(connection)

        final_sql, ordered_params, query_obj = super()._process_sql_params(
            sql, parameters, *filters, validator=validator, sanitizer=sanitizer, **kwargs
        )

        db_params: tuple[Any, ...] = tuple(ordered_params)

        with self._with_cursor(conn) as cursor:
            cursor.execute(final_sql, db_params)

            if self.returns_rows(query_obj.expression):
                raw_data_tuples = cursor.fetchall()
                column_names = [col[0] for col in cursor.description or []]
                rows = [dict(zip(column_names, row)) for row in raw_data_tuples]
                raw_result_data = rows[0] if rows else cast("dict[str, Any]", {})
                return SelectResult(rows=rows, column_names=column_names, raw_result=raw_result_data)

            rowcount = getattr(cursor, "rowcount", -1)
            operation_type_val = "EXECUTE"

            if isinstance(query_obj.expression, exp.Insert):
                operation_type_val = "INSERT"
            elif isinstance(query_obj.expression, exp.Update):
                operation_type_val = "UPDATE"
            elif isinstance(query_obj.expression, exp.Delete):
                operation_type_val = "DELETE"

            return ExecuteResult(
                raw_result=cast("dict[str, Any]", {}),
                rows_affected=rowcount,
                operation_type=operation_type_val,
                last_inserted_id=getattr(cursor, "lastrowid", None),
            )

    def execute_many(
        self,
        sql: "Statement",
        parameters: Optional[Sequence[StatementParameterType]] = None,
        *filters: "StatementFilter",
        connection: Optional["SqliteConnection"] = None,
        validator: Optional["SQLValidator"] = None,
        sanitizer: Optional["SQLSanitizer"] = None,
        **kwargs: Any,
    ) -> "ExecuteResult[dict[str, Any]]":
        """Execute a SQL statement with multiple parameter sets.

        Useful for batch INSERT, UPDATE, or DELETE operations.

        Args:
            sql: The SQL statement to execute.
            parameters: Sequence of parameter sets.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            validator: Optional validator for the statement.
            sanitizer: Optional sanitizer for the statement.
            **kwargs: Additional keyword arguments.

        Returns:
            An ExecuteResult containing the batch operation results.
        """
        conn = self._connection(connection)

        sql_template_str, _, query_obj = super()._process_sql_params(
            sql, None, *filters, validator=validator, sanitizer=sanitizer, **kwargs
        )

        param_sequence = parameters if parameters is not None else []

        processed_params_list: list[tuple[Any, ...]] = []
        if param_sequence:
            for param_set in param_sequence:
                if isinstance(param_set, (list, tuple)):
                    processed_params_list.append(tuple(param_set))
                elif isinstance(param_set, dict):
                    temp_stmt = SQLStatement(
                        query_obj.expression,
                        parameters=param_set,
                        dialect=self.dialect,
                        validator=validator,
                        sanitizer=sanitizer,
                    )
                    _, ordered_single_params = temp_stmt.get_ordered_parameters(placeholder_style=ParameterStyle.QMARK)
                    processed_params_list.append(tuple(ordered_single_params))
                else:
                    processed_params_list.append((param_set,))

        with self._with_cursor(conn) as cursor:
            if not processed_params_list:
                total_affected = 0
            else:
                cursor.executemany(sql_template_str, processed_params_list)
                total_affected = getattr(cursor, "rowcount", -1)
                if total_affected == -1 and processed_params_list:
                    total_affected = len(processed_params_list)

            operation_type_val = "EXECUTE"
            if isinstance(query_obj.expression, exp.Insert):
                operation_type_val = "INSERT"
            elif isinstance(query_obj.expression, exp.Update):
                operation_type_val = "UPDATE"
            elif isinstance(query_obj.expression, exp.Delete):
                operation_type_val = "DELETE"

            return ExecuteResult(
                raw_result=cast("dict[str, Any]", {}), rows_affected=total_affected, operation_type=operation_type_val
            )

    def execute_script(
        self,
        sql: "Statement",
        parameters: Optional["StatementParameterType"] = None,
        *filters: "StatementFilter",
        connection: Optional["SqliteConnection"] = None,
        validator: Optional["SQLValidator"] = None,
        sanitizer: Optional["SQLSanitizer"] = None,
        **kwargs: Any,
    ) -> str:
        """Execute a multi-statement SQL script.

        For script execution, parameters are rendered as static literals directly
        in the SQL rather than using placeholders, since SQLite's executescript
        doesn't support parameter binding.

        Args:
            sql: The SQL script to execute.
            parameters: Parameters for the script.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            validator: Optional validator for the script.
            sanitizer: Optional sanitizer for the script.
            **kwargs: Additional keyword arguments.

        Returns:
            A string with execution results/output.
        """
        conn = self._connection(connection)

        stmt = SQLStatement(
            sql,
            parameters=parameters,
            dialect=self.dialect,
            validator=validator,
            sanitizer=sanitizer,
        )

        # Apply filters
        for filter_obj in filters:
            stmt = apply_filter(stmt, filter_obj)

        validation_result = stmt.validator.validate(stmt.expression, stmt._dialect)
        if (
            not validation_result.is_safe
            and stmt.validator.min_risk_to_raise is not None
            and validation_result.risk_level.value >= stmt.validator.min_risk_to_raise.value
        ):
            error_msg = f"SQL script validation failed with risk level {validation_result.risk_level}:\n"
            error_msg += "Issues:\n" + "\n".join([f"- {issue}" for issue in validation_result.issues])
            if validation_result.warnings:
                error_msg += "\nWarnings:\n" + "\n".join([f"- {warn}" for warn in validation_result.warnings])
            from sqlspec.exceptions import SQLValidationError

            raise SQLValidationError(
                error_msg, stmt.get_sql(placeholder_style=ParameterStyle.STATIC), validation_result.risk_level
            )

        final_sql = stmt.get_sql(placeholder_style=ParameterStyle.STATIC)

        with self._with_cursor(conn) as cursor:
            cursor.executescript(final_sql)
            return "DONE"
