# ruff: noqa: PLR6301
import logging
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from duckdb import DuckDBPyConnection

from sqlspec.config import InstrumentationConfig
from sqlspec.driver import SyncDriverAdapterProtocol
from sqlspec.exceptions import SQLConversionError
from sqlspec.statement.mixins import ResultConverter, SQLTranslatorMixin, SyncArrowMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, ExecuteResult, SelectResult
from sqlspec.statement.sql import SQL, SQLConfig, Statement
from sqlspec.typing import DictRow, ModelDTOT, SQLParameterType
from sqlspec.utils.telemetry import instrument_operation

if TYPE_CHECKING:
    from sqlspec.statement.filters import StatementFilter


__all__ = ("DuckDBConnection", "DuckDBDriver")


DuckDBConnection = DuckDBPyConnection


logger = logging.getLogger("sqlspec")


class DuckDBDriver(
    SyncDriverAdapterProtocol["DuckDBConnection", DictRow],
    SQLTranslatorMixin["DuckDBConnection"],
    SyncArrowMixin["DuckDBConnection"],
    ResultConverter,
):
    """DuckDB Sync Driver Adapter with modern architecture.

    DuckDB is a fast, in-process analytical database built for modern data analysis.
    This driver provides:

    - High-performance columnar query execution
    - Excellent Arrow integration for analytics workloads
    - Direct file querying (CSV, Parquet, JSON) without imports
    - Extension ecosystem for cloud storage and formats
    - Zero-copy operations where possible
    """

    dialect: str = "duckdb"
    __supports_arrow__: ClassVar[bool] = True

    def __init__(
        self,
        connection: "DuckDBConnection",
        config: Optional[SQLConfig] = None,
        instrumentation_config: Optional[InstrumentationConfig] = None,
    ) -> None:
        super().__init__(
            connection=connection,
            config=config,
            instrumentation_config=instrumentation_config,
            default_row_type=DictRow,
        )

    def _get_placeholder_style(self) -> ParameterStyle:
        return ParameterStyle.QMARK

    @staticmethod
    @contextmanager
    def _get_cursor(connection: "DuckDBConnection") -> Generator["DuckDBConnection", None, None]:
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def _execute_impl(
        self,
        statement: SQL,
        parameters: Optional[SQLParameterType] = None,
        connection: Optional["DuckDBConnection"] = None,
        config: Optional[SQLConfig] = None,
        is_many: bool = False,
        is_script: bool = False,
        **kwargs: Any,
    ) -> Any:
        with instrument_operation(self, "duckdb_execute", "database"):
            conn = self._connection(connection)
            if config is not None and config != statement.config:
                statement = statement.copy(config=config)

            final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())

            final_exec_params: Optional[list[Any]] = None

            if is_many:
                if parameters and isinstance(parameters, Sequence):
                    final_exec_params = [list(p) if isinstance(p, (list, tuple)) else [p] for p in parameters]
                else:
                    final_exec_params = []
            else:
                single_params = statement.get_parameters(style=self._get_placeholder_style())
                if single_params is not None:
                    if isinstance(single_params, list):
                        final_exec_params = single_params
                    elif hasattr(single_params, "__iter__") and not isinstance(single_params, (str, bytes)):
                        final_exec_params = list(single_params)
                    else:
                        final_exec_params = [single_params]

            with self._get_cursor(conn) as cursor:
                if self.instrumentation_config.log_queries:
                    logger.debug("Executing SQL: %s", final_sql)

                if self.instrumentation_config.log_parameters and final_exec_params:
                    logger.debug("Query parameters: %s", final_exec_params)

                if is_script:
                    script_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
                    cursor.execute(script_sql)
                    return "SCRIPT EXECUTED"
                if is_many:
                    cursor.executemany(final_sql, cast("list[list[Any]]", final_exec_params))
                else:
                    cursor.execute(final_sql, final_exec_params or [])
                return cursor

    def _wrap_select_result(
        self,
        statement: SQL,
        raw_driver_result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SelectResult[ModelDTOT], SelectResult[dict[str, Any]]]:
        with instrument_operation(self, "duckdb_wrap_select", "database"):
            cursor = raw_driver_result
            fetched_data = cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]

            # Convert to list of dicts
            rows_as_dicts: list[dict[str, Any]] = []
            if fetched_data:
                rows_as_dicts = [dict(zip(column_names, row)) for row in fetched_data]

            if self.instrumentation_config.log_results_count:
                logger.debug("Query returned %d rows", len(rows_as_dicts))

            if schema_type:
                converted_data = self.to_schema(data=rows_as_dicts, schema_type=schema_type)
                return SelectResult[ModelDTOT](
                    statement=statement,
                    data=converted_data,
                    column_names=column_names,
                )
            return SelectResult[dict[str, Any]](
                statement=statement,
                data=rows_as_dicts,
                column_names=column_names,
            )

    def _wrap_execute_result(
        self,
        statement: SQL,
        raw_driver_result: Any,
        **kwargs: Any,
    ) -> ExecuteResult:
        with instrument_operation(self, "duckdb_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()

            if isinstance(raw_driver_result, str):
                execute_data = {
                    "rows_affected": 0,
                    "last_inserted_id": None,
                    "inserted_ids": [],
                    "returning_data": None,
                    "operation_type": operation_type or "SCRIPT",
                }
                return ExecuteResult(
                    statement=statement,
                    data=execute_data,
                    rows_affected=0,
                    operation_type=operation_type or "SCRIPT",
                )

            cursor = raw_driver_result
            rows_affected = cursor.rowcount if hasattr(cursor, "rowcount") else -1

            if self.instrumentation_config.log_results_count:
                logger.debug("Execute operation affected %d rows", rows_affected)

            execute_data = {
                "rows_affected": rows_affected,
                "last_inserted_id": None,
                "inserted_ids": [],
                "returning_data": None,
                "operation_type": operation_type,
            }
            return ExecuteResult(
                statement=statement,
                data=execute_data,
                rows_affected=rows_affected,
                operation_type=operation_type,
            )

    def select_to_arrow(
        self,
        statement: Statement,
        parameters: Optional[SQLParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional["DuckDBConnection"] = None,
        config: Optional[SQLConfig] = None,
        **kwargs: Any,
    ) -> ArrowResult:
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
            SQLConversionError: If DuckDB execute returned None for Arrow.

        Returns:
            An ArrowResult containing the query results as an Arrow Table.
        """
        # Build SQL statement if needed
        if isinstance(statement, SQL):
            sql_stmt = statement
        else:
            sql_stmt = self._build_statement(statement, config, *filters)
            if parameters:
                # Apply parameters to the statement
                sql_stmt = SQL(sql_stmt.sql, parameters, dialect=self.dialect, config=config or self.config)

        if not self.returns_rows(sql_stmt.expression):
            op_type = "UNKNOWN"
            if sql_stmt.expression and hasattr(sql_stmt.expression, "key"):
                op_type = str(sql_stmt.expression.key).upper()
            msg = f"Cannot fetch Arrow table for a non-query statement. Command type: {op_type}"
            raise TypeError(msg)

        final_sql = sql_stmt.to_sql(placeholder_style=self._get_placeholder_style())
        ordered_params = sql_stmt.get_parameters(style=self._get_placeholder_style())

        # Convert parameters to list format for DuckDB
        final_params: Optional[list[Any]] = None
        if ordered_params is not None:
            if isinstance(ordered_params, list):
                final_params = ordered_params
            elif hasattr(ordered_params, "__iter__") and not isinstance(ordered_params, (str, bytes)):
                final_params = list(ordered_params)
            else:
                final_params = [ordered_params]

        conn = self._connection(connection)
        with self._get_cursor(conn) as cursor:
            if self.instrumentation_config.log_queries:
                logger.debug("Executing Arrow query: %s", final_sql)

            relation = cursor.execute(final_sql, final_params or [])
            if relation is None:
                msg = "DuckDB execute returned None for a query expected to return rows for Arrow."
                raise SQLConversionError(msg)

            try:
                arrow_table = cursor.fetch_arrow_table()
                return ArrowResult(statement=sql_stmt, data=arrow_table)
            except Exception as e:
                msg = f"Failed to convert DuckDB result to Arrow table: {e}"
                raise SQLConversionError(msg) from e
