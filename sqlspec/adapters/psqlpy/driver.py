"""Psqlpy Driver Implementation."""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from psqlpy import Connection

from sqlspec.driver import AsyncDriverAdapterProtocol
from sqlspec.driver.mixins import AsyncStorageMixin, SQLTranslatorMixin, ToSchemaMixin
from sqlspec.exceptions import wrap_exceptions
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

    Refactored to align with the new enhanced driver architecture and
    instrumentation standards following the psycopg pattern.
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

        # Determine if we need to convert parameter style
        detected_styles = {p.style for p in statement.parameter_info}
        target_style = self.default_parameter_style

        # Check if any detected style is not supported
        unsupported_styles = detected_styles - set(self.supported_parameter_styles)
        if unsupported_styles:
            # Convert to default style if we have unsupported styles
            target_style = self.default_parameter_style
        elif detected_styles:
            # Use the first detected style if all are supported
            # Prefer the first supported style found
            for style in detected_styles:
                if style in self.supported_parameter_styles:
                    target_style = style
                    break

        if statement.is_many:
            sql, params = statement.compile(placeholder_style=target_style)
            return await self._execute_many(sql, params, connection=connection, **kwargs)

        sql, params = statement.compile(placeholder_style=target_style)
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

            # Convert parameters to the format Psqlpy expects
            converted_params = self._convert_parameters_to_driver_format(
                sql, parameters, target_style=self.parameter_style
            )

            # Psqlpy expects positional parameters as a list
            final_driver_params: Optional[list[Any]] = None
            if converted_params is not None:
                if isinstance(converted_params, (list, tuple)):
                    final_driver_params = list(converted_params)
                else:
                    final_driver_params = [converted_params]

            if self.instrumentation_config.log_queries:
                logger.debug("Executing psqlpy SQL: %s", sql)
            if self.instrumentation_config.log_parameters and final_driver_params:
                logger.debug("Psqlpy query parameters: %s", final_driver_params)
            if AsyncDriverAdapterProtocol.returns_rows(statement.expression):
                query_result = await conn.fetch(sql, parameters=final_driver_params)
                dict_rows: list[dict[str, Any]] = query_result.result()
                column_names = list(dict_rows[0].keys()) if dict_rows else []
                result: SelectResultDict = {
                    "data": dict_rows,
                    "column_names": column_names,
                    "rows_affected": len(dict_rows),
                }
                return result

            # For non-SELECT statements
            execute_result = await conn.execute(sql, parameters=final_driver_params)
            dml_result: DMLResultDict = {
                "rows_affected": execute_result if isinstance(execute_result, int) else -1,
                "status_message": "OK",
            }
            return dml_result

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

            # Convert parameter list to proper format for psqlpy's native execute_many
            if param_list and isinstance(param_list, list):
                # Ensure each parameter set is a list
                formatted_params = []
                for param_set in param_list:
                    if isinstance(param_set, (list, tuple)):
                        formatted_params.append(list(param_set))
                    else:
                        formatted_params.append([param_set])

                # Use psqlpy's native execute_many method
                await conn.execute_many(sql, formatted_params)

                # Return DML result dict
                # PSQLPy's execute_many doesn't return affected rows count
                # Use the number of parameter sets as a reasonable estimate
                result: DMLResultDict = {
                    "rows_affected": len(formatted_params),
                    "status_message": "OK",
                }
                return result

            # No parameters provided
            empty_result: DMLResultDict = {
                "rows_affected": 0,
                "status_message": "OK",
            }
            return empty_result

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
            await conn.execute(script, parameters=None)
            result: ScriptResultDict = {
                "statements_executed": -1,  # PSQLPy doesn't provide this info
                "status_message": "SCRIPT EXECUTED",
            }
            return result

    async def _wrap_select_result(
        self,
        statement: SQL,
        result: SelectResultDict,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        async with instrument_operation_async(self, "psqlpy_wrap_select", "database"):
            # result must be a dict with keys: data, column_names, rows_affected
            dict_rows = result["data"]
            column_names = result["column_names"]
            rows_affected = result["rows_affected"]

            if self.instrumentation_config.log_results_count:
                logger.debug("Psqlpy query returned %d rows", len(dict_rows))

            if schema_type:
                converted_data_seq = self.to_schema(data=dict_rows, schema_type=schema_type)
                converted_data_list = list(converted_data_seq) if converted_data_seq is not None else []
                return SQLResult[ModelDTOT](
                    statement=statement,
                    data=converted_data_list,
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
            with wrap_exceptions(wrap_exceptions=False, suppress=AttributeError):
                if statement.expression:
                    operation_type = str(statement.expression.key).upper()

            # Handle TypedDict results
            if isinstance(result, dict):
                # Check if this is a ScriptResultDict
                if "statements_executed" in result:
                    return SQLResult[RowT](
                        statement=statement,
                        data=[],
                        rows_affected=0,
                        operation_type=operation_type or "SCRIPT",
                        metadata={"status_message": result["status_message"]},
                    )

                # Check if this is a DMLResultDict
                if "rows_affected" in result:
                    rows_affected = result["rows_affected"]
                    status_message = result["status_message"]

                    if self.instrumentation_config.log_results_count:
                        logger.debug("Psqlpy execute operation affected %d rows", rows_affected)

                    return SQLResult[RowT](
                        statement=statement,
                        data=[],
                        rows_affected=rows_affected,
                        operation_type=operation_type,
                        metadata={"status_message": status_message},
                    )

            # This shouldn't happen with TypedDict approach
            msg = f"Unexpected result type: {type(result)}"
            raise ValueError(msg)

    def _connection(self, connection: Optional[PsqlpyConnection] = None) -> PsqlpyConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection
