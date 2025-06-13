"""Psqlpy Driver Implementation."""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from psqlpy import Connection

from sqlspec.driver import AsyncDriverAdapterProtocol
from sqlspec.driver.mixins import AsyncStorageMixin, SQLTranslatorMixin, ToSchemaMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import DMLResultDict, ScriptResultDict, SelectResultDict, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, RowT
from sqlspec.utils.telemetry import instrument_operation_async

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

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
    AsyncDriverAdapterProtocol[PsqlpyConnection, RowT], SQLTranslatorMixin, AsyncStorageMixin, ToSchemaMixin
):
    """Psqlpy Driver Adapter.

    Modern, high-performance driver for PostgreSQL.
    """

    __supports_arrow__: ClassVar[bool] = True
    __supports_parquet__: ClassVar[bool] = False
    dialect: "DialectType" = "postgres"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (ParameterStyle.NUMERIC,)
    default_parameter_style: ParameterStyle = ParameterStyle.NUMERIC

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

    async def _execute_statement(
        self,
        statement: SQL,
        connection: Optional[PsqlpyConnection] = None,
        **kwargs: Any,
    ) -> Union[SelectResultDict, DMLResultDict, ScriptResultDict]:
        if statement.is_script:
            sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
            return await self._execute_script(sql, connection=connection, **kwargs)

        # Let the SQL object handle parameter style conversion based on dialect support
        sql, params = statement.compile(placeholder_style=self.default_parameter_style)

        if statement.is_many:
            return await self._execute_many(sql, params, connection=connection, **kwargs)

        return await self._execute(sql, params, statement, connection=connection, **kwargs)

    async def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: Optional[PsqlpyConnection] = None,
        **kwargs: Any,
    ) -> Union[SelectResultDict, DMLResultDict]:
        async with instrument_operation_async(self, "psqlpy_execute", "database"):
            conn = self._connection(connection)

            if self.instrumentation_config.log_queries:
                logger.debug("Executing psqlpy SQL: %s", sql)
            if self.instrumentation_config.log_parameters and parameters:
                logger.debug("Psqlpy query parameters: %s", parameters)

            if self.returns_rows(statement.expression):
                query_result = await conn.fetch(sql, parameters=parameters)
                dict_rows: list[dict[str, Any]] = query_result or []
                column_names = list(dict_rows[0].keys()) if dict_rows else []
                return {
                    "data": dict_rows,
                    "column_names": column_names,
                    "rows_affected": len(dict_rows),
                }

            # For non-SELECT statements
            rows_affected = await conn.execute(sql, parameters=parameters)
            return {
                "rows_affected": -1 if rows_affected is None else rows_affected,
                "status_message": "OK",
            }

    async def _execute_many(
        self,
        sql: str,
        param_list: Any,
        connection: Optional[PsqlpyConnection] = None,
        **kwargs: Any,
    ) -> DMLResultDict:
        async with instrument_operation_async(self, "psqlpy_execute_many", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing psqlpy SQL (executemany): %s", sql)
            if self.instrumentation_config.log_parameters and param_list:
                logger.debug("Psqlpy query parameters (batch): %s", param_list)

            rows_affected = await conn.execute_many(sql, param_list or [])
            return {
                "rows_affected": -1 if rows_affected is None else rows_affected,
                "status_message": "OK",
            }

    async def _execute_script(
        self,
        script: str,
        connection: Optional[PsqlpyConnection] = None,
        **kwargs: Any,
    ) -> ScriptResultDict:
        async with instrument_operation_async(self, "psqlpy_execute_script", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing psqlpy SQL script: %s", script)

            # psqlpy can execute multi-statement scripts directly
            await conn.execute(script)
            return {
                "statements_executed": -1,  # Not directly supported, but script is executed
                "status_message": "SCRIPT EXECUTED",
            }

    async def _wrap_select_result(
        self,
        statement: SQL,
        result: SelectResultDict,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        async with instrument_operation_async(self, "psqlpy_wrap_select", "database"):
            dict_rows = result["data"]
            column_names = result["column_names"]
            rows_affected = result["rows_affected"]

            if self.instrumentation_config.log_results_count:
                logger.debug("Psqlpy query returned %d rows", len(dict_rows))

            if schema_type:
                converted_data = self.to_schema(data=dict_rows, schema_type=schema_type)
                return SQLResult[ModelDTOT](
                    statement=statement,
                    data=list(converted_data),
                    column_names=column_names,
                    rows_affected=rows_affected,
                    operation_type="SELECT",
                )
            return SQLResult[RowT](
                statement=statement,
                data=cast("list[RowT]", dict_rows),
                column_names=column_names,
                rows_affected=rows_affected,
                operation_type="SELECT",
            )

    async def _wrap_execute_result(
        self,
        statement: SQL,
        result: Union[DMLResultDict, ScriptResultDict],
        **kwargs: Any,
    ) -> SQLResult[RowT]:
        async with instrument_operation_async(self, "psqlpy_async_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression:
                operation_type = str(statement.expression.key).upper()

            if "statements_executed" in result:
                script_result = cast("ScriptResultDict", result)
                return SQLResult[RowT](
                    statement=statement,
                    data=[],
                    rows_affected=0,
                    operation_type="SCRIPT",
                    metadata={
                        "status_message": script_result.get("status_message", ""),
                        "statements_executed": script_result.get("statements_executed", -1),
                    },
                )

            dml_result = cast("DMLResultDict", result)
            rows_affected = dml_result.get("rows_affected", -1)
            status_message = dml_result.get("status_message", "")

            if self.instrumentation_config.log_results_count:
                logger.debug("Psqlpy execute operation affected %d rows", rows_affected)

            return SQLResult[RowT](
                statement=statement,
                data=[],
                rows_affected=rows_affected,
                operation_type=operation_type,
                metadata={"status_message": status_message},
            )

    def _connection(self, connection: Optional[PsqlpyConnection] = None) -> PsqlpyConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection
