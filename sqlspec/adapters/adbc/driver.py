# ruff: noqa: TRY301
import contextlib
import logging
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any, ClassVar, Optional, Union, cast

from adbc_driver_manager.dbapi import Connection, Cursor

from sqlspec.config import InstrumentationConfig
from sqlspec.driver import SyncDriverAdapterProtocol
from sqlspec.exceptions import SQLConversionError
from sqlspec.statement.mixins import ResultConverter, SQLTranslatorMixin, SyncArrowMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT
from sqlspec.utils.telemetry import instrument_operation

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
        except Exception:
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
        connection: Optional["AdbcConnection"] = None,
        **kwargs: Any,
    ) -> Any:
        with instrument_operation(self, "adbc_execute", "database"):
            conn = self._connection(connection)

            if statement.is_script:
                final_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
                with self._get_cursor(conn) as cursor:
                    if self.instrumentation_config.log_queries:
                        logger.debug("Executing SQL Script: %s", final_sql)
                    cursor.execute(final_sql)
                    status_message = "SCRIPT EXECUTED"
                    if hasattr(cursor, "statusmessage"):
                        with contextlib.suppress(Exception):
                            status_message = getattr(cursor, "statusmessage", "SCRIPT EXECUTED")
                    return status_message

            final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())
            params_to_execute = statement.parameters

            # Ensure parameters are in a list format if they exist, suitable for ADBC
            final_exec_params: Optional[list[Any]] = None
            if statement.is_many:
                if params_to_execute and isinstance(params_to_execute, Sequence):
                    # For executemany, ADBC expects a list of lists/tuples
                    final_exec_params = [list(p) if isinstance(p, (list, tuple)) else [p] for p in params_to_execute]
                else:
                    final_exec_params = []  # Should not happen if statement.is_many is true with valid params
            elif params_to_execute is not None:  # Single execution
                # For execute, ADBC expects a list or tuple of parameters
                if isinstance(params_to_execute, list):
                    final_exec_params = params_to_execute
                elif hasattr(params_to_execute, "__iter__") and not isinstance(params_to_execute, (str, bytes)):
                    final_exec_params = list(params_to_execute)
                else:  # Single parameter
                    final_exec_params = [params_to_execute]

            with self._get_cursor(conn) as cursor:
                if self.instrumentation_config.log_queries:
                    logger.debug("Executing SQL: %s", final_sql)

                if self.instrumentation_config.log_parameters and final_exec_params:
                    logger.debug("Query parameters: %s", final_exec_params)

                if statement.is_many:
                    # Ensure final_exec_params is correctly a list of lists for executemany
                    # The previous logic for final_exec_params under is_many should handle this.
                    cursor.executemany(final_sql, cast("list[list[Any]]", final_exec_params))
                else:
                    cursor.execute(final_sql, final_exec_params or [])
                return cursor

    def _wrap_select_result(
        self,
        statement: SQL,
        result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SQLResult[ModelDTOT], SQLResult[dict[str, Any]]]:
        with instrument_operation(self, "adbc_wrap_select", "database"):
            cursor = result
            fetched_data = cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]

            rows_as_dicts: list[dict[str, Any]] = []
            if fetched_data:
                rows_as_dicts = [dict(zip(column_names, row)) for row in fetched_data]

            if self.instrumentation_config.log_results_count:
                logger.debug("Query returned %d rows", len(rows_as_dicts))

            if schema_type:
                converted_data_seq = self.to_schema(data=rows_as_dicts, schema_type=schema_type)
                converted_data_list = list(converted_data_seq) if converted_data_seq is not None else []
                return SQLResult[ModelDTOT](
                    statement=statement,
                    data=converted_data_list,
                    column_names=column_names,
                    operation_type="SELECT",
                )
            return SQLResult[dict[str, Any]](
                statement=statement,
                data=rows_as_dicts,
                column_names=column_names,
                operation_type="SELECT",
            )

    def _wrap_execute_result(
        self,
        statement: SQL,
        result: Any,
        **kwargs: Any,
    ) -> SQLResult[dict[str, Any]]:
        with instrument_operation(self, "adbc_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()

            if isinstance(result, str):
                return SQLResult[dict[str, Any]](
                    statement=statement,
                    data=[],
                    rows_affected=0,
                    operation_type=operation_type or "SCRIPT",
                    metadata={"status_message": result},
                )

            cursor = result
            rows_affected = cursor.rowcount if hasattr(cursor, "rowcount") else -1

            returned_data: list[dict[str, Any]] = []
            column_names_for_returning: list[str] = []

            if hasattr(cursor, "description") and cursor.description:
                try:
                    fetched_returning_data = cursor.fetchall()
                    if fetched_returning_data:
                        column_names_for_returning = [col[0] for col in cursor.description or []]
                        returned_data = [dict(zip(column_names_for_returning, row)) for row in fetched_returning_data]
                        if rows_affected == -1 and returned_data:
                            rows_affected = len(returned_data)
                except Exception as e:  # pragma: no cover
                    logger.debug("Could not fetch RETURNING data in ADBC _wrap_execute_result: %s", e)

            if self.instrumentation_config.log_results_count:
                logger.debug("Execute operation affected %d rows", rows_affected)
                if returned_data:
                    logger.debug("RETURNING clause potentially returned %d rows", len(returned_data))

            return SQLResult[dict[str, Any]](
                statement=statement,
                data=returned_data,
                rows_affected=rows_affected,
                operation_type=operation_type,
                column_names=column_names_for_returning,
            )

    def _select_to_arrow_impl(
        self,
        stmt_obj: "SQL",
        connection: "AdbcConnection",
        **kwargs: Any,
    ) -> "ArrowResult":
        final_sql = stmt_obj.to_sql(placeholder_style=self._get_placeholder_style())
        ordered_params = stmt_obj.get_parameters(style=self._get_placeholder_style())

        final_params: Optional[list[Any]] = None
        if ordered_params is not None:
            if isinstance(ordered_params, list):
                final_params = ordered_params
            elif hasattr(ordered_params, "__iter__") and not isinstance(ordered_params, (str, bytes)):
                final_params = list(ordered_params)
            else:
                final_params = [ordered_params]

        with self._get_cursor(connection) as cursor:
            if self.instrumentation_config.log_queries:
                logger.debug("Executing ADBC Arrow query: %s", final_sql)
            if self.instrumentation_config.log_parameters and final_params:
                logger.debug("Query parameters for ADBC Arrow: %s", final_params)

            cursor.execute(final_sql, final_params or [])

            try:
                arrow_table = cursor.fetch_arrow_table()
                if arrow_table is None:
                    msg = "ADBC cursor.fetch_arrow_table() returned None. Expected an Arrow Table."
                    raise SQLConversionError(msg)

                return ArrowResult(statement=stmt_obj, data=arrow_table)
            except Exception as e:
                if "fetch_arrow_table() returned None" in str(e):
                    import pyarrow as pa

                    return ArrowResult(statement=stmt_obj, data=pa.Table.from_arrays([], names=[]))

                msg = f"Failed to convert ADBC result to Arrow table: {e}"
                raise SQLConversionError(msg) from e
