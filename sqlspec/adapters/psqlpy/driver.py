"""Psqlpy Driver Implementation."""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from psqlpy import Connection, QueryResult

from sqlspec.driver import AsyncDriverAdapterProtocol
from sqlspec.driver.mixins import AsyncStorageMixin, SQLTranslatorMixin, ToSchemaMixin
from sqlspec.exceptions import wrap_exceptions
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import SQLResult
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
        # Use the target parameter style from config if specified
        if self.config and hasattr(self.config, "target_parameter_style") and self.config.target_parameter_style:
            style_map = {
                "qmark": ParameterStyle.QMARK,
                "named": ParameterStyle.NAMED_COLON,
                "named_colon": ParameterStyle.NAMED_COLON,
                "named_at": ParameterStyle.NAMED_AT,
                "named_dollar": ParameterStyle.NAMED_DOLLAR,
                "numeric": ParameterStyle.NUMERIC,
                "pyformat_named": ParameterStyle.PYFORMAT_NAMED,
                "pyformat_positional": ParameterStyle.PYFORMAT_POSITIONAL,
            }
            return style_map.get(self.config.target_parameter_style, ParameterStyle.NUMERIC)
        # Default to numeric for psqlpy
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
            # For execute_many, we need to convert placeholders even if parsing is disabled
            # Get the SQL with proper placeholder conversion
            if statement._config.enable_parsing or statement.expression is not None:
                converted_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())
            else:
                # Manually convert placeholders when parsing is disabled
                converted_sql = self.convert_placeholders_in_raw_sql(str(statement._sql), self._get_placeholder_style())

            return await self._execute_many(
                converted_sql,
                statement.parameters,
                connection=connection,
                **kwargs,
            )

        return await self._execute(
            statement.to_sql(placeholder_style=self._get_placeholder_style()),
            statement.get_parameters(style=self._get_placeholder_style()),
            statement,
            connection=connection,
            **kwargs,
        )

    async def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: Optional[PsqlpyConnection] = None,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "psqlpy_execute", "database"):
            conn = self._connection(connection)
            # Psqlpy expects positional parameters as a list
            final_driver_params: Optional[list[Any]] = None
            if parameters is not None:
                final_driver_params = list(parameters) if isinstance(parameters, (list, tuple)) else [parameters]
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
    ) -> Any:
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

                # Return consistent dict format like _execute
                # PSQLPy's execute_many doesn't return affected rows count
                # Use the number of parameter sets as a reasonable estimate
                return {
                    "rowcount": len(formatted_params),
                    "data": [],  # executemany doesn't return data
                    "columns": [],
                }

            return {"rowcount": 0, "data": [], "columns": []}

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

            if not dict_rows:
                # TODO: Use the parsed SQL here
                # PSQLPy limitation: cannot get column names from empty result sets
                # This is a known limitation where schema information is not available
                return SQLResult[RowT](
                    statement=statement,
                    data=cast("list[RowT]", []),
                    column_names=[],
                    operation_type="SELECT",
                )

            column_names = list(dict_rows[0].keys())

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
                data=cast("list[RowT]", dict_rows),
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
            with wrap_exceptions(wrap_exceptions=False, suppress=AttributeError):
                if statement.expression:
                    operation_type = str(statement.expression.key).upper()

            rows_affected = -1
            status_message: Optional[str] = None

            if isinstance(result, str) and result == "SCRIPT EXECUTED":
                operation_type = "SCRIPT"
                rows_affected = 0
                status_message = result
            elif isinstance(result, dict):
                # New format from _execute_many: result is a dict
                rows_affected = result.get("rowcount", -1)
            elif isinstance(result, BatchResult):
                rows_affected = result.affected_rows
            elif isinstance(result, QueryResult):
                # Try to get result data to see if it contains row count info
                try:
                    result_data = result.result()
                    # For DML operations, psqlpy returns an empty list but we still affected rows
                    if isinstance(result_data, list) and operation_type in {"INSERT", "UPDATE", "DELETE"}:
                        # psqlpy doesn't provide row count for DML operations
                        # We'll need to assume success if no exception was raised
                        rows_affected = 1  # Default to 1 for single row operations
                    else:
                        rows_affected = getattr(result, "affected_rows", -1)
                        if rows_affected == -1:
                            rows_affected = getattr(result, "rowcount", -1)
                except Exception:
                    # If we can't get result data, try attributes
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
