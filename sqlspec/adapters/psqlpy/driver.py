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
from sqlspec.typing import DictRow, ModelDTOT, RowT
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
    AsyncDriverAdapterProtocol[PsqlpyConnection, RowT],
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

    async def _execute_statement(
        self,
        statement: SQL,
        connection: Optional[PsqlpyConnection] = None,
        **kwargs: Any,
    ) -> Any:
        if statement.is_script:
            return await self._execute_script(
                statement.to_sql(placeholder_style=ParameterStyle.STATIC),
                connection=connection,
                **kwargs,
            )
        if statement.is_many:
            return await self._execute_many(
                statement.to_sql(placeholder_style=self._get_placeholder_style()),
                statement.parameters,
                connection=connection,
                **kwargs,
            )
        return await self._execute(
            statement.to_sql(placeholder_style=self._get_placeholder_style()),
            statement.parameters,
            statement,
            connection=connection,
            **kwargs,
        )

    async def _execute(
        self,
        sql: str,
        params: Any,
        statement: SQL,
        connection: Optional[PsqlpyConnection] = None,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "psqlpy_execute", "database"):
            conn = self._connection(connection)
            final_driver_params: Optional[list[Any]] = None
            if params is not None:
                final_driver_params = list(params) if isinstance(params, (list, tuple)) else [params]
            if self.instrumentation_config.log_queries:
                logger.debug("Executing psqlpy SQL: %s", sql)
            if self.instrumentation_config.log_parameters and final_driver_params:
                logger.debug("Psqlpy query parameters: %s", final_driver_params)
            if AsyncDriverAdapterProtocol.returns_rows(statement.expression):
                return await conn.fetch(sql, parameters=final_driver_params)
            return await conn.execute(sql, parameters=final_driver_params)

    async def _execute_many(
        self,
        sql: str,
        param_list: Any,
        connection: Optional[PsqlpyConnection] = None,
        **kwargs: Any,
    ) -> BatchResult:
        async with instrument_operation_async(self, "psqlpy_execute_many", "database"):
            conn = self._connection(connection)
            total_affected = 0
            params_list = param_list if isinstance(param_list, list) else []
            if self.instrumentation_config.log_queries:
                logger.debug("Executing psqlpy SQL (executemany): %s", sql)
            if self.instrumentation_config.log_parameters and params_list:
                logger.debug("Psqlpy query parameters (batch): %s", params_list)
            for param_set in params_list:
                current_param_list = list(param_set) if isinstance(param_set, (list, tuple)) else [param_set]
                result = await conn.execute(sql, parameters=current_param_list)
                affected = getattr(result, "affected_rows", None) or getattr(result, "rowcount", 0)
                if affected and affected != -1:
                    total_affected += affected
                elif not self.returns_rows(None):
                    total_affected += 1
            return BatchResult(total_affected)

    async def _execute_script(
        self,
        script: str,
        connection: Optional[PsqlpyConnection] = None,
        **kwargs: Any,
    ) -> str:
        async with instrument_operation_async(self, "psqlpy_execute_script", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing psqlpy SQL script: %s", script)
            await conn.execute(script, parameters=None)
            return "SCRIPT EXECUTED"

    async def _wrap_select_result(
        self,
        statement: SQL,
        result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
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
            return SQLResult[RowT](
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
    ) -> SQLResult[RowT]:
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

            return SQLResult[RowT](
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
