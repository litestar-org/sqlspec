# ruff: noqa: PLR6301
"""Psqlpy Driver Implementation."""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from psqlpy import Connection, QueryResult

from sqlspec.driver import AsyncDriverAdapterProtocol
from sqlspec.statement.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT
from sqlspec.utils.telemetry import instrument_operation_async

if TYPE_CHECKING:
    from sqlspec.config import InstrumentationConfig

__all__ = ("PsqlpyConnection", "PsqlpyDriver")

PsqlpyConnection = Connection
logger = logging.getLogger("sqlspec")


@dataclass
class BatchResult:
    def __init__(self, affected_rows: int) -> None:
        self.affected_rows = affected_rows
        self.rowcount = affected_rows


class PsqlpyDriver(
    AsyncDriverAdapterProtocol[PsqlpyConnection, dict[str, Any]],
    SQLTranslatorMixin[PsqlpyConnection],
    AsyncArrowMixin[PsqlpyConnection],
    ResultConverter,
):
    """Psqlpy Driver Adapter.

    Refactored to align with the new enhanced driver architecture and
    instrumentation standards following the psycopg pattern.
    """

    __supports_arrow__: ClassVar[bool] = False
    dialect: str = "postgres"

    def __init__(
        self,
        connection: PsqlpyConnection,
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
        return ParameterStyle.NUMERIC

    async def _execute_impl(
        self,
        statement: SQL,
        connection: Optional[PsqlpyConnection] = None,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "psqlpy_execute", "database"):
            conn = self._connection(connection)
            # config parameter removed, statement.config is the source of truth

            final_sql: str
            # psqlpy expects parameters as a list for its `parameters` argument.
            # statement.parameters should provide this after processing.
            final_driver_params: Optional[list[Any]] = None

            if statement.is_script:
                final_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
            else:
                final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())
                params_to_execute = statement.parameters  # This should be a list or tuple

                if statement.is_many:
                    # psqlpy does not have a direct executemany that takes a list of lists.
                    # It executes one by one. So params_to_execute here will be a list of parameter lists.
                    if params_to_execute and isinstance(params_to_execute, list):
                        # We expect a list of lists/tuples. Each inner list/tuple is for one execution.
                        final_driver_params = params_to_execute
                    else:
                        final_driver_params = []
                elif params_to_execute is not None:
                    if isinstance(params_to_execute, (list, tuple)):
                        final_driver_params = list(params_to_execute)
                    else:  # Single parameter value
                        final_driver_params = [params_to_execute]
                    # else final_driver_params remains None, meaning no parameters

            if self.instrumentation_config.log_queries:
                logger.debug("Executing psqlpy SQL: %s", final_sql)

            if self.instrumentation_config.log_parameters and final_driver_params:
                # For is_many, final_driver_params is a list of lists. Log appropriately.
                if statement.is_many:
                    logger.debug("Psqlpy query parameters (batch): %s", final_driver_params)
                else:
                    logger.debug("Psqlpy query parameters: %s", final_driver_params)

            if statement.is_script:
                await conn.execute(final_sql, parameters=None)  # Explicitly None for scripts
                return "SCRIPT EXECUTED"

            if statement.is_many:
                total_affected = 0
                # final_driver_params is a list of parameter lists
                for param_set in final_driver_params or []:
                    current_param_list = list(param_set) if isinstance(param_set, (list, tuple)) else [param_set]
                    result = await conn.execute(final_sql, parameters=current_param_list)
                    affected = getattr(result, "affected_rows", None) or getattr(result, "rowcount", 0)
                    if affected and affected != -1:
                        total_affected += affected
                    elif not self.returns_rows(statement.expression):  # Heuristic for DML
                        total_affected += 1
                return BatchResult(total_affected)

            # Single execution
            if self.returns_rows(statement.expression):
                # final_driver_params should be a list here or None
                return await conn.fetch(final_sql, parameters=final_driver_params)
            # For DML or other non-select queries
            return await conn.execute(final_sql, parameters=final_driver_params)

    async def _wrap_select_result(
        self,
        statement: SQL,
        result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SQLResult[ModelDTOT], SQLResult[dict[str, Any]]]:
        async with instrument_operation_async(self, "psqlpy_wrap_select", "database"):
            query_result = cast("QueryResult", result)
            dict_rows: list[dict[str, Any]] = query_result.result()
            column_names = list(dict_rows[0].keys()) if dict_rows else []

            if self.instrumentation_config.log_results_count:
                logger.debug("Psqlpy query returned %d rows", len(dict_rows))

            if schema_type:
                converted_data_seq = self.to_schema(data=dict_rows, schema_type=schema_type)
                converted_data_list = list(converted_data_seq) if converted_data_seq is not None else []
                return SQLResult[ModelDTOT](
                    statement=statement,
                    data=converted_data_list,
                    column_names=column_names,
                    operation_type="SELECT",
                )
            return SQLResult[dict[str, Any]](
                statement=statement,
                data=dict_rows,
                column_names=column_names,
                operation_type="SELECT",
            )

    async def _wrap_execute_result(
        self,
        statement: SQL,
        result: Any,
        **kwargs: Any,
    ) -> SQLResult[dict[str, Any]]:
        async with instrument_operation_async(self, "psqlpy_async_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()

            rows_affected = -1
            status_message: Optional[str] = None

            if isinstance(result, str) and result == "SCRIPT EXECUTED":
                operation_type = "SCRIPT"
                rows_affected = 0
                status_message = result
            elif isinstance(result, BatchResult):
                rows_affected = result.affected_rows
            elif isinstance(result, QueryResult):
                rows_affected = getattr(result, "affected_rows", -1)
                if rows_affected == -1:
                    rows_affected = getattr(result, "rowcount", -1)
            else:
                logger.warning("Psqlpy _wrap_execute_result: Unexpected result type: %s", type(result))

            if self.instrumentation_config.log_results_count:
                logger.debug("Psqlpy execute operation affected %d rows", rows_affected)

            return SQLResult[dict[str, Any]](
                statement=statement,
                data=[],
                rows_affected=rows_affected,
                operation_type=operation_type,
                metadata={"status_message": status_message} if status_message else {},
            )

    def _connection(self, connection: Optional[PsqlpyConnection] = None) -> PsqlpyConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection

    async def _select_to_arrow_impl(
        self,
        stmt_obj: SQL,
        connection: Optional[PsqlpyConnection] = None,
        **kwargs: Any,
    ) -> Any:
        """Implementation for select_to_arrow method."""
        async with instrument_operation_async(self, "psqlpy_select_to_arrow", "database"):
            conn = self._connection(connection)

            final_sql = stmt_obj.to_sql(placeholder_style=self._get_placeholder_style())
            final_driver_params: Optional[list[Any]] = None

            params_to_execute = stmt_obj.parameters
            if params_to_execute is not None:
                if isinstance(params_to_execute, (list, tuple)):
                    final_driver_params = list(params_to_execute)
                else:
                    final_driver_params = [params_to_execute]

            # Fetch data for Arrow conversion using the correct psqlpy method
            return await conn.fetch(final_sql, parameters=final_driver_params)
