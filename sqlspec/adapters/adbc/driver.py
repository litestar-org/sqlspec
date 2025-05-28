import contextlib
import logging
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from adbc_driver_manager.dbapi import Connection, Cursor
from sqlglot import exp

from sqlspec.base import SyncDriverAdapterProtocol
from sqlspec.sql.mixins import ResultConverter, SQLTranslatorMixin
from sqlspec.sql.parameters import ParameterStyle
from sqlspec.sql.result import ExecuteResult, SelectResult
from sqlspec.sql.statement import SQLStatement, SQLTransformer, SQLValidator, Statement

if TYPE_CHECKING:
    from sqlspec.sql.filters import StatementFilter
    from sqlspec.typing import ArrowTable, StatementParameterType

__all__ = ("AdbcConnection", "AdbcDriver")

logger = logging.getLogger("sqlspec")

AdbcConnection = Connection


class AdbcDriver(
    SQLTranslatorMixin["AdbcConnection"],
    SyncDriverAdapterProtocol["AdbcConnection"],
    ResultConverter,
):
    """ADBC Sync Driver Adapter.

    This driver implements the new unified SQLSpec protocol with the core 3 methods:
    - execute() - Universal method for all SQL operations
    - execute_many() - Batch operations
    - execute_script() - Multi-statement scripts

    It also provides optional Arrow support via select_to_arrow().
    """

    connection: AdbcConnection
    __supports_arrow__: ClassVar[bool] = True
    dialect: str = "adbc"

    def __init__(self, connection: "AdbcConnection") -> None:
        """Initialize the ADBC driver adapter."""
        super().__init__(connection)
        self.dialect = self._get_dialect(connection)

    @staticmethod
    def _get_dialect(connection: "AdbcConnection") -> str:
        """Get the database dialect based on the driver name.

        Args:
            connection: The ADBC connection object.

        Returns:
            The database dialect.
        """
        driver_info = connection.adbc_get_info()
        vendor_name = driver_info.get("vendor_name", "").lower()

        if "postgres" in vendor_name:
            return "postgres"
        if "bigquery" in vendor_name:
            return "bigquery"
        if "sqlite" in vendor_name:
            return "sqlite"
        if "duckdb" in vendor_name:
            return "duckdb"
        if "mysql" in vendor_name:
            return "mysql"
        if "snowflake" in vendor_name:
            return "snowflake"

        return "postgres"

    def _get_placeholder_style(self) -> ParameterStyle:
        """Return the placeholder style based on the detected ADBC dialect."""
        if self.dialect == "sqlite":
            return ParameterStyle.QMARK
        if self.dialect == "duckdb":
            return ParameterStyle.QMARK
        if self.dialect == "postgres":
            return ParameterStyle.NUMERIC
        if self.dialect == "mysql":
            return ParameterStyle.QMARK
        if self.dialect == "bigquery":
            return ParameterStyle.NAMED_AT
        return ParameterStyle.QMARK

    @staticmethod
    def _cursor(connection: "AdbcConnection", *args: Any, **kwargs: Any) -> "Cursor":
        return connection.cursor(*args, **kwargs)

    @contextmanager
    def _with_cursor(self, connection: "AdbcConnection") -> Iterator["Cursor"]:
        cursor = self._cursor(connection)
        try:
            yield cursor
        finally:
            with contextlib.suppress(Exception):
                cursor.close()  # type: ignore[no-untyped-call]

    def execute(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AdbcConnection]" = None,
        validator: "Optional[SQLValidator]" = None,
        sanitizer: "Optional[SQLTransformer]" = None,
        **kwargs: "Any",
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
        connection = self._connection(connection)
        final_sql, ordered_params, query_obj = self._process_sql_params(sql, parameters, *filters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(final_sql, ordered_params)

            if self.returns_rows(query_obj.expression):
                results = cursor.fetchall()
                column_names = [column[0] for column in cursor.description or []]
                rows = [dict(zip(column_names, row)) for row in results]
                raw_select_result_data = rows[0] if rows else cast("dict[str, Any]", {})
                return SelectResult(rows=rows, column_names=column_names, raw_result=raw_select_result_data)

            rowcount = getattr(cursor, "rowcount", -1)

            return ExecuteResult(
                raw_result=cast("dict[str, Any]", {}),
                rows_affected=rowcount,
                operation_type=cast("str", query_obj.operation_type),
            )

    def execute_many(
        self,
        sql: "Statement",
        parameters: "Optional[Sequence[StatementParameterType]]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AdbcConnection]" = None,
        validator: "Optional[SQLValidator]" = None,
        sanitizer: "Optional[SQLTransformer]" = None,
        **kwargs: "Any",
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
        connection = self._connection(connection)

        sql_template_str, _, query_obj = super()._process_sql_params(
            sql, None, *filters, validator=validator, sanitizer=sanitizer, **kwargs
        )

        param_sequence = parameters if parameters is not None else []

        processed_params_list: list[list[Any]] = []
        if param_sequence:
            placeholder_style_for_ordering = self._get_placeholder_style()
            for param_set in param_sequence:
                if isinstance(param_set, (list, tuple)):
                    processed_params_list.append(list(param_set))
                elif isinstance(param_set, dict):
                    temp_stmt = SQLStatement(
                        query_obj.expression,
                        parameters=param_set,
                        dialect=query_obj._dialect,
                        validator=validator,
                        sanitizer=sanitizer,
                    )
                    _, ordered_single_params = temp_stmt.get_ordered_parameters(
                        placeholder_style=placeholder_style_for_ordering
                    )
                    processed_params_list.append(list(ordered_single_params))
                else:
                    processed_params_list.append([param_set])

        with self._with_cursor(connection) as cursor:
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
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AdbcConnection]" = None,
        validator: "Optional[SQLValidator]" = None,
        sanitizer: "Optional[SQLTransformer]" = None,
        **kwargs: "Any",
    ) -> "str":
        """Execute a multi-statement SQL script.

        For script execution, parameters are rendered as static literals directly
        in the SQL rather than using placeholders, since scripts may contain
        multiple statements that don't support parameterization.

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
        connection = self._connection(connection)

        # For script execution, use static parameter style to embed parameters as literals
        query_obj = SQLStatement(
            sql,
            parameters=parameters,
            kwargs=kwargs or None,
            dialect=self.dialect if not isinstance(sql, SQLStatement) else sql.dialect or self.dialect,
        )

        # Get SQL with static parameter rendering (no placeholders)
        final_sql = query_obj.get_sql(placeholder_style="static")

        with self._with_cursor(connection) as cursor:
            cursor.execute(final_sql)  # No parameters needed since they're embedded as literals
            return getattr(cursor, "statusmessage", "DONE")

    def select_to_arrow(  # pyright: ignore[reportUnknownParameterType]
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AdbcConnection]" = None,
        validator: "Optional[SQLValidator]" = None,
        sanitizer: "Optional[SQLTransformer]" = None,
        **kwargs: "Any",
    ) -> "ArrowTable":
        """Execute a SELECT statement and return results as an Apache Arrow Table.

        This is an optional method that provides high-performance data access
        for analytics workloads.

        Args:
            sql: The SQL query to execute.
            parameters: Parameters for the query.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            validator: Optional validator for the statement.
            sanitizer: Optional sanitizer for the statement.
            **kwargs: Additional keyword arguments.

        Returns:
            An Arrow Table containing the query results.
        """
        connection = self._connection(connection)
        final_sql, ordered_params, _ = self._process_sql_params(sql, parameters, *filters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(final_sql, ordered_params)
            return cursor.fetch_arrow_table()
