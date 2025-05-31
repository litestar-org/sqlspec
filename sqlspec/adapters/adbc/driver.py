import contextlib
import logging
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union

from adbc_driver_manager.dbapi import Connection, Cursor

from sqlspec.driver import SyncDriverAdapterProtocol
from sqlspec.statement.builder import QueryBuilder
from sqlspec.statement.mixins import ResultConverter, SQLTranslatorMixin, SyncArrowMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, ExecuteResult, SelectResult
from sqlspec.statement.sql import SQL, SQLConfig, Statement

if TYPE_CHECKING:
    from sqlspec.statement.filters import StatementFilter
    from sqlspec.typing import DictRow, ModelDTOT, SQLParameterType

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

    def __init__(self, connection: "AdbcConnection", config: "Optional[SQLConfig]" = None) -> None:
        """Initialize the ADBC driver adapter."""
        super().__init__(connection, config=config)
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

    def _get_parameter_style(self) -> ParameterStyle:
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
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        connection: "Optional[AdbcConnection]" = None,
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
        if schema_type is not None:
            logger.warning("schema_type parameter is not yet fully implemented for adbc driver's execute method.")

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

        processed_params: Optional[list[Any]] = None  # ADBC typically expects a list
        if ordered_params is not None:
            if isinstance(ordered_params, list):
                processed_params = ordered_params
            elif isinstance(ordered_params, Iterable) and not isinstance(ordered_params, (str, bytes)):
                processed_params = list(ordered_params)
            else:
                processed_params = [ordered_params]

        with self._with_cursor(conn) as cursor:
            cursor.execute(final_sql, processed_params)

            if self.returns_rows(current_stmt.expression):
                results = cursor.fetchall()
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

    def execute_many(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[ExecuteResult[Any]]]",
        parameters: "Optional[Sequence[SQLParameterType]]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AdbcConnection]" = None,
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

        processed_params_list: list[list[Any]] = []  # ADBC executemany expects list of lists/tuples
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

                current_item_params_as_list: list[Any]
                if isinstance(ordered_params_for_item, list):
                    current_item_params_as_list = ordered_params_for_item
                elif ordered_params_for_item is None:
                    current_item_params_as_list = []
                elif isinstance(ordered_params_for_item, Iterable) and not isinstance(
                    ordered_params_for_item, (str, bytes)
                ):
                    current_item_params_as_list = list(ordered_params_for_item)
                else:
                    current_item_params_as_list = [ordered_params_for_item]
                processed_params_list.append(current_item_params_as_list)

        with self._with_cursor(conn) as cursor:
            total_affected = 0
            if processed_params_list:
                cursor.executemany(final_sql, processed_params_list)
                total_affected = getattr(cursor, "rowcount", -1)
                if total_affected == -1 and processed_params_list:  # Heuristic
                    total_affected = len(processed_params_list)

            operation_type = "EXECUTE"
            if template_stmt.expression and hasattr(template_stmt.expression, "key"):
                operation_type = str(template_stmt.expression.key).upper()

            return ExecuteResult(raw_result={}, rows_affected=total_affected, operation_type=operation_type)

    def execute_script(
        self,
        statement: "Union[str, SQL]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AdbcConnection]" = None,
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

        # ADBC DBAPI spec doesn't explicitly define executescript.
        # We execute the (potentially multi-statement) SQL string directly.
        # The underlying ADBC driver may or may not support multi-statement queries in a single execute call.
        # If it doesn't, this might only execute the first statement or error.
        with self._with_cursor(conn) as cursor:
            cursor.execute(final_sql)
            status_message = "SCRIPT EXECUTED"
            if hasattr(cursor, "statusmessage") and cursor.statusmessage:  # type: ignore[attr-defined]
                status_message = cursor.statusmessage  # type: ignore[attr-defined]
            return status_message

    def select_to_arrow(
        self,
        statement: "Statement",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AdbcConnection]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: "Any",
    ) -> "ArrowResult":
        """Execute a SELECT statement and return results as an Apache Arrow Table.

        This is an optional method that provides high-performance data access
        for analytics workloads.

        Args:
            statement: The SQL query (str, sqlglot.Expression, or SQL object) to execute.
            parameters: Parameters for the query.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            config: Optional statement configuration.
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
        effective_config = config or self.config

        current_stmt = SQL(statement, parameters, *filters, dialect=self.dialect, config=effective_config, **kwargs)

        current_stmt.validate()
        if not self.returns_rows(current_stmt.expression):
            op_type = "UNKNOWN"
            if current_stmt.expression and hasattr(current_stmt.expression, "key"):
                op_type = str(current_stmt.expression.key).upper()
            msg = f"Cannot fetch Arrow table for a non-query statement. Command type: {op_type}"
            raise TypeError(msg)

        parameter_style = self._get_parameter_style()
        final_sql = current_stmt.to_sql(placeholder_style=parameter_style)
        ordered_params = current_stmt.get_parameters(style=parameter_style)

        processed_params: Optional[list[Any]] = None
        if ordered_params is not None:
            if isinstance(ordered_params, list):
                processed_params = ordered_params
            elif isinstance(ordered_params, Iterable) and not isinstance(ordered_params, (str, bytes)):
                processed_params = list(ordered_params)
            else:
                processed_params = [ordered_params]

        with self._with_cursor(conn) as cursor:
            cursor.execute(final_sql, processed_params)
            arrow_table = cursor.fetch_arrow_table()  # type: ignore[attr-defined]
            return ArrowResult(raw_result=arrow_table)
