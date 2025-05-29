import contextlib
import logging
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from adbc_driver_manager.dbapi import Connection, Cursor

from sqlspec.base import SyncDriverAdapterProtocol
from sqlspec.sql.mixins import ResultConverter, SQLTranslatorMixin, SyncArrowMixin
from sqlspec.sql.parameters import ParameterStyle
from sqlspec.sql.result import ArrowResult, ExecuteResult, SelectResult
from sqlspec.sql.statement import SQLStatement, Statement, StatementConfig

if TYPE_CHECKING:
    from sqlspec.sql.filters import StatementFilter
    from sqlspec.typing import StatementParameterType

__all__ = ("AdbcConnection", "AdbcDriver")

logger = logging.getLogger("sqlspec")

AdbcConnection = Connection


class AdbcDriver(
    SQLTranslatorMixin["AdbcConnection"],
    SyncDriverAdapterProtocol["AdbcConnection"],
    SyncArrowMixin["AdbcConnection"],
    ResultConverter,
):
    """ADBC Sync Driver Adapter.

    This driver implements the new unified SQLSpec protocol with the core 3 methods:
    - execute() - Universal method for all SQL operations
    - execute_many() - Batch operations
    - execute_script() - Multi-statement scripts

    It also provides optional Arrow support via select_to_arrow().

    Enhanced Features:
    - Full SQLStatement integration with filters and validation
    - Automatic SQL dialect detection and placeholder style conversion
    - Comprehensive parameter validation and security checks
    - Support for filter composition (pagination, search, etc.)
    """

    connection: AdbcConnection
    __supports_arrow__: ClassVar[bool] = True
    dialect: str = "adbc"

    def __init__(self, connection: "AdbcConnection", statement_config: "Optional[StatementConfig]" = None) -> None:
        """Initialize the ADBC driver adapter."""
        super().__init__(connection, statement_config=statement_config)
        self.dialect = self._get_dialect(connection)

    @staticmethod
    def _get_dialect(connection: "AdbcConnection") -> str:
        """Get the database dialect based on the driver name.

        Args:
            connection: The ADBC connection object.

        Returns:
            The database dialect.
        """
        try:
            driver_info = connection.adbc_get_info()
            vendor_name = driver_info.get("vendor_name", "").lower()
            driver_name = driver_info.get("driver_name", "").lower()

            if "postgres" in vendor_name or "postgresql" in driver_name:
                return "postgres"
            if "bigquery" in vendor_name or "bigquery" in driver_name:
                return "bigquery"
            if "sqlite" in vendor_name or "sqlite" in driver_name:
                return "sqlite"
            if "duckdb" in vendor_name or "duckdb" in driver_name:
                return "duckdb"
            if "mysql" in vendor_name or "mysql" in driver_name:
                return "mysql"
            if "snowflake" in vendor_name or "snowflake" in driver_name:
                return "snowflake"
            if "flight" in driver_name or "flightsql" in driver_name:
                return "sqlite"
        except Exception:  # noqa: BLE001
            logger.warning("Could not reliably determine ADBC dialect from driver info. Defaulting to 'postgres'.")
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
        if self.dialect == "snowflake":
            return ParameterStyle.QMARK
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
        statement: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AdbcConnection]" = None,
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
        placeholder_style = self._get_placeholder_style()
        final_sql = stmt.to_sql(placeholder_style=placeholder_style)
        ordered_params = stmt.get_parameters(style=placeholder_style)

        if ordered_params is not None and not isinstance(ordered_params, list):
            ordered_params = (
                list(ordered_params)
                if isinstance(ordered_params, Iterable) and not isinstance(ordered_params, (str, bytes))
                else [ordered_params]
            )

        with self._with_cursor(conn) as cursor:
            cursor.execute(final_sql, ordered_params)

            if self.returns_rows(stmt.expression):
                results = cursor.fetchall()
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

    def execute_many(
        self,
        statement: "Statement",
        parameters: "Optional[Sequence[StatementParameterType]]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AdbcConnection]" = None,
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

        with self._with_cursor(conn) as cursor:
            if param_sequence:
                cursor.executemany(final_sql, processed_params_list)
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

    def execute_script(
        self,
        statement: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AdbcConnection]" = None,
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

        with self._with_cursor(conn) as cursor:
            cursor.execute(final_sql)
            return getattr(cursor, "statusmessage", "SCRIPT EXECUTED")

    def select_to_arrow(
        self,
        statement: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AdbcConnection]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: "Any",
    ) -> "ArrowResult":
        """Execute a SELECT statement and return results as an Apache Arrow Table.

        This is an optional method that provides high-performance data access
        for analytics workloads.

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

        Example:
            >>> from sqlspec.sql.filters import LimitOffset
            >>> # Get Arrow table with pagination
            >>> arrow_result = driver.select_to_arrow(
            ...     "SELECT * FROM large_table WHERE category = ?",
            ...     ["electronics"],
            ...     LimitOffset(limit=1000, offset=0),
            ... )
            >>> table = arrow_result.raw_result  # Apache Arrow Table
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

        placeholder_style = self._get_placeholder_style()
        final_sql = stmt.to_sql(placeholder_style=placeholder_style)
        ordered_params = stmt.get_parameters(style=placeholder_style)

        if ordered_params is not None and not isinstance(ordered_params, list):
            ordered_params = (
                list(ordered_params)
                if isinstance(ordered_params, Iterable) and not isinstance(ordered_params, (str, bytes))
                else [ordered_params]
            )

        with self._with_cursor(conn) as cursor:
            cursor.execute(final_sql, ordered_params)
            return ArrowResult(raw_result=cursor.fetch_arrow_table())
