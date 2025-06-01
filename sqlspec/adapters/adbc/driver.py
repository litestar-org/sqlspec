import contextlib
import logging
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from adbc_driver_manager.dbapi import Connection, Cursor

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

__all__ = ("AdbcConnection", "AdbcDriver")

logger = logging.getLogger("sqlspec")

AdbcConnection = Connection


class AdbcDriver(
    SyncDriverAdapterProtocol["AdbcConnection", DictRow],
    SQLTranslatorMixin["AdbcConnection"],
    SyncArrowMixin["AdbcConnection"],
    ResultConverter,
):
    """ADBC Sync Driver Adapter with modern architecture.

    ADBC (Arrow Database Connectivity) provides a universal interface for connecting
    to multiple database systems with high-performance Arrow-native data transfer.

    This driver provides:
    - Universal connectivity across database backends (PostgreSQL, SQLite, DuckDB, etc.)
    - High-performance Arrow data streaming and bulk operations
    - Intelligent dialect detection and parameter style handling
    - Seamless integration with cloud databases (BigQuery, Snowflake)
    - Driver manager abstraction for easy multi-database support
    """

    dialect: str = "adbc"
    __supports_arrow__: ClassVar[bool] = True

    def __init__(
        self,
        connection: "AdbcConnection",
        config: Optional[SQLConfig] = None,
        instrumentation_config: Optional[InstrumentationConfig] = None,
    ) -> None:
        super().__init__(
            connection=connection,
            config=config,
            instrumentation_config=instrumentation_config,
            default_row_type=DictRow,
        )
        self.dialect = self._get_dialect(connection)

    def _get_placeholder_style(self) -> ParameterStyle:
        """Return the placeholder style based on the detected ADBC dialect."""
        dialect_style_map = {
            "sqlite": ParameterStyle.QMARK,
            "duckdb": ParameterStyle.QMARK,
            "postgres": ParameterStyle.NUMERIC,
            "mysql": ParameterStyle.QMARK,
            "bigquery": ParameterStyle.NAMED_AT,
            "snowflake": ParameterStyle.QMARK,
        }
        return dialect_style_map.get(self.dialect, ParameterStyle.QMARK)

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

    @staticmethod
    @contextmanager
    def _get_cursor(connection: "AdbcConnection") -> Iterator["Cursor"]:
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            with contextlib.suppress(Exception):
                cursor.close()  # type: ignore[no-untyped-call]

    def _execute_impl(
        self,
        statement: SQL,
        parameters: Optional[SQLParameterType] = None,
        connection: Optional["AdbcConnection"] = None,
        config: Optional[SQLConfig] = None,
        is_many: bool = False,
        is_script: bool = False,
        **kwargs: Any,
    ) -> Any:
        with instrument_operation(self, "adbc_execute", "database"):
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
                    status_message = "SCRIPT EXECUTED"
                    if hasattr(cursor, "statusmessage"):
                        with contextlib.suppress(Exception):
                            status_message = getattr(cursor, "statusmessage", "SCRIPT EXECUTED")
                    return status_message
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
        with instrument_operation(self, "adbc_wrap_select", "database"):
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
        with instrument_operation(self, "adbc_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()

            if isinstance(raw_driver_result, str):
                return ExecuteResult(
                    statement=statement,
                    data={
                        "rows_affected": 0,
                        "last_inserted_id": None,
                        "inserted_ids": [],
                        "returning_data": None,
                        "operation_type": operation_type or "SCRIPT",
                    },
                    rows_affected=0,
                    operation_type=operation_type or "SCRIPT",
                )

            cursor = raw_driver_result
            rows_affected = cursor.rowcount if hasattr(cursor, "rowcount") else -1

            if self.instrumentation_config.log_results_count:
                logger.debug("Execute operation affected %d rows", rows_affected)

            return ExecuteResult(
                statement=statement,
                data={
                    "rows_affected": rows_affected,
                    "last_inserted_id": None,
                    "inserted_ids": [],
                    "returning_data": None,
                    "operation_type": operation_type,
                },
                rows_affected=rows_affected,
                operation_type=operation_type,
            )

    def select_to_arrow(
        self,
        statement: Statement,
        parameters: Optional[SQLParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional["AdbcConnection"] = None,
        config: Optional[SQLConfig] = None,
        **kwargs: Any,
    ) -> ArrowResult:
        """Execute a SELECT statement and return results as an Apache Arrow Table.

        This method leverages ADBC's excellent Arrow integration for high-performance
        analytics workloads across multiple database backends.

        Args:
            statement: The SQL query to execute.
            parameters: Parameters for the query.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            config: Optional statement configuration.
            **kwargs: Additional keyword arguments.

        Raises:
            TypeError: If the SQL statement is not a valid query.
            SQLConversionError: If ADBC execute returned None for Arrow.

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

        # Convert parameters to list format for ADBC
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

            cursor.execute(final_sql, final_params or [])

            try:
                arrow_table = cursor.fetch_arrow_table()
                return ArrowResult(statement=sql_stmt, data=arrow_table)
            except Exception as e:
                msg = f"Failed to convert ADBC result to Arrow table: {e}"
                raise SQLConversionError(msg) from e
