# ruff: noqa: PLR6301
import logging
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from duckdb import DuckDBPyConnection

from sqlspec.driver import SyncDriverAdapterProtocol
from sqlspec.exceptions import SQLConversionError
from sqlspec.statement.mixins import ResultConverter, SQLTranslatorMixin, SyncArrowMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, RowT
from sqlspec.utils.telemetry import instrument_operation

if TYPE_CHECKING:
    from sqlspec.config import InstrumentationConfig

__all__ = ("DuckDBConnection", "DuckDBDriver")


DuckDBConnection = DuckDBPyConnection


logger = logging.getLogger("sqlspec")


class DuckDBDriver(
    SyncDriverAdapterProtocol["DuckDBConnection", RowT],
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
        config: "Optional[SQLConfig]" = None,
        instrumentation_config: "Optional[InstrumentationConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        super().__init__(
            connection=connection,
            config=config,
            instrumentation_config=instrumentation_config,
            default_row_type=default_row_type,
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
        connection: Optional["DuckDBConnection"] = None,
        **kwargs: Any,
    ) -> Any:
        with instrument_operation(self, "duckdb_execute", "database"):
            conn = self._connection(connection)

            if statement.is_script:
                final_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
                if self.instrumentation_config.log_queries:
                    logger.debug("Executing SQL script: %s", final_sql)
                with self._get_cursor(conn) as cursor:
                    cursor.execute(final_sql)
                return "SCRIPT EXECUTED"

            final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())
            params_to_execute = statement.parameters

            final_exec_params: Optional[list[Any]] = None
            if statement.is_many:
                if params_to_execute and isinstance(params_to_execute, Sequence):
                    final_exec_params = [list(p) if isinstance(p, (list, tuple)) else [p] for p in params_to_execute]
                else:
                    final_exec_params = []
            elif params_to_execute is not None:
                if isinstance(params_to_execute, list):
                    final_exec_params = params_to_execute
                elif hasattr(params_to_execute, "__iter__") and not isinstance(params_to_execute, (str, bytes)):
                    final_exec_params = list(params_to_execute)
                else:
                    final_exec_params = [params_to_execute]

            with self._get_cursor(conn) as cursor:
                if self.instrumentation_config.log_queries:
                    logger.debug("Executing SQL: %s", final_sql)
                if self.instrumentation_config.log_parameters and final_exec_params:
                    logger.debug("Query parameters: %s", final_exec_params)

                cursor.execute(final_sql, final_exec_params or [])

                if self.returns_rows(statement.expression):
                    fetched_data = cursor.fetchall()
                    column_names = [col[0] for col in cursor.description or []]
                    return {"data": fetched_data, "columns": column_names, "rowcount": cursor.rowcount}

                return {"rowcount": cursor.rowcount if hasattr(cursor, "rowcount") else -1}

    def _wrap_select_result(
        self,
        statement: SQL,
        result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        with instrument_operation(self, "duckdb_wrap_select", "database"):
            if not isinstance(result, dict) or "data" not in result:
                logger.warning("Unexpected result format in _wrap_select_result for DuckDB.")
                return SQLResult[RowT](
                    statement=statement,
                    data=cast("list[RowT]", []),
                    column_names=[],
                    operation_type="SELECT",
                    rows_affected=0,
                )

            fetched_tuples: list[tuple[Any, ...]] = result.get("data", [])
            column_names: list[str] = result.get("columns", [])

            rows_as_dicts: list[dict[str, Any]] = [dict(zip(column_names, row)) for row in fetched_tuples]

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
            return SQLResult[RowT](
                statement=statement,
                data=cast("list[RowT]", rows_as_dicts),
                column_names=column_names,
                operation_type="SELECT",
            )

    def _wrap_execute_result(
        self,
        statement: SQL,
        result: Any,
        **kwargs: Any,
    ) -> SQLResult[RowT]:
        with instrument_operation(self, "duckdb_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()

            rows_affected = -1

            if isinstance(result, str) and result == "SCRIPT EXECUTED":
                operation_type = "SCRIPT"
                rows_affected = 0
                return SQLResult[RowT](
                    statement=statement,
                    data=cast("list[RowT]", []),
                    rows_affected=rows_affected,
                    operation_type=operation_type,
                    metadata={"status_message": result},
                )
            if isinstance(result, dict) and "rowcount" in result:
                rows_affected = result["rowcount"]
            else:
                logger.warning(
                    "Unexpected result format in _wrap_execute_result for DuckDB DML. Expected dict with 'rowcount' or str 'SCRIPT EXECUTED'. Got: %s",
                    type(result),
                )

            if self.instrumentation_config.log_results_count:
                logger.debug("Execute operation affected %d rows", rows_affected)

            return SQLResult[RowT](
                statement=statement,
                data=cast("list[RowT]", []),
                rows_affected=rows_affected,
                operation_type=operation_type,
            )

    def _select_to_arrow_impl(
        self,
        stmt_obj: "SQL",
        connection: "DuckDBConnection",
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
                logger.debug("Executing DuckDB Arrow query: %s", final_sql)
            if self.instrumentation_config.log_parameters and final_params:
                logger.debug("Query parameters for DuckDB Arrow: %s", final_params)

            relation = cursor.execute(final_sql, final_params or [])
            if relation is None:
                import pyarrow as pa

                logger.warning(
                    "DuckDB execute returned None where a relation was expected for Arrow conversion. Returning empty table."
                )
                return ArrowResult(statement=stmt_obj, data=pa.Table.from_arrays([], names=[]))

            try:
                arrow_table = cursor.fetch_arrow_table()
                return ArrowResult(statement=stmt_obj, data=arrow_table)
            except Exception as e:
                msg = f"Failed to convert DuckDB result to Arrow table: {e}"
                raise SQLConversionError(msg) from e
