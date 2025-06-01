# ruff: noqa: PLR6301
"""Psqlpy Driver Implementation."""

import logging
from dataclasses import dataclass
from typing import Any, ClassVar, Optional, Union, cast

from psqlpy import Connection, QueryResult

from sqlspec.config import InstrumentationConfig
from sqlspec.driver import AsyncDriverAdapterProtocol
from sqlspec.statement.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ExecuteResult, SelectResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import ModelDTOT, SQLParameterType
from sqlspec.utils.telemetry import instrument_operation_async

__all__ = ("PsqlpyConnection", "PsqlpyDriver")

PsqlpyConnection = Connection  # type: ignore[misc]
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
        config: Optional[SQLConfig] = None,
        instrumentation_config: Optional[InstrumentationConfig] = None,
    ) -> None:
        super().__init__(
            connection=connection,
            config=config,
            instrumentation_config=instrumentation_config,
            default_row_type=dict[str, Any],
        )

    def _get_placeholder_style(self) -> ParameterStyle:
        return ParameterStyle.NUMERIC

    async def _execute_impl(
        self,
        statement: SQL,
        parameters: Optional[SQLParameterType] = None,
        connection: Optional[PsqlpyConnection] = None,
        config: Optional[SQLConfig] = None,
        is_many: bool = False,
        is_script: bool = False,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "psqlpy_execute", "database"):
            conn = self._connection(connection)
            if config is not None and config != statement.config:
                statement = statement.copy(config=config)

            final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())

            if self.instrumentation_config.log_queries:
                logger.debug("Executing psqlpy SQL: %s", final_sql)

            # Convert parameters to list format for psqlpy
            final_exec_params = None
            if is_many:
                # For batch operations, convert sequence of parameters
                if parameters and hasattr(parameters, "__iter__"):
                    batch_params = []
                    for param_set in parameters:
                        if isinstance(param_set, (list, tuple)):
                            batch_params.append(list(param_set))
                        elif isinstance(param_set, dict):
                            # Convert dict to list based on parameter order from statement
                            stmt_params = statement.get_parameters(style=self._get_placeholder_style())
                            if isinstance(stmt_params, (list, tuple)):
                                batch_params.append(list(param_set.values()))
                            else:
                                batch_params.append([param_set])
                        else:
                            batch_params.append([param_set])
                    final_exec_params = batch_params
                else:
                    final_exec_params = []
            else:
                # Single execution
                single_params = statement.get_parameters(style=self._get_placeholder_style())
                if isinstance(single_params, (list, tuple)):
                    final_exec_params = list(single_params)
                elif single_params is not None:
                    final_exec_params = [single_params]  # type: ignore[list-item]

            if self.instrumentation_config.log_parameters and final_exec_params:
                logger.debug("Psqlpy query parameters: %s", final_exec_params)

            if is_script:
                # For scripts, use static SQL without parameters
                script_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
                await conn.execute(script_sql, parameters=None)
                return "SCRIPT EXECUTED"
            if is_many:
                # Execute batch operation - psqlpy doesn't have executemany, so execute individually
                total_affected = 0
                for param_set in final_exec_params or []:
                    result = await conn.execute(final_sql, parameters=param_set)
                    affected = getattr(result, "affected_rows", None) or getattr(result, "rowcount", 0)
                    if affected and affected != -1:
                        total_affected += affected
                    elif not self.returns_rows(statement.expression):
                        total_affected += 1  # Heuristic for DML operations

                # Return a mock result for batch operations

                return BatchResult(total_affected)
            # Single execution
            if self.returns_rows(statement.expression):
                return await conn.fetch(final_sql, parameters=final_exec_params)
            return await conn.execute(final_sql, parameters=final_exec_params)

    async def _wrap_select_result(
        self,
        statement: SQL,
        raw_driver_result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SelectResult[ModelDTOT], SelectResult[dict[str, Any]]]:
        async with instrument_operation_async(self, "psqlpy_wrap_select", "database"):
            query_result = cast("QueryResult", raw_driver_result)
            dict_rows: list[dict[str, Any]] = query_result.result()
            column_names = list(dict_rows[0].keys()) if dict_rows else []

            if self.instrumentation_config.log_results_count:
                logger.debug("Psqlpy query returned %d rows", len(dict_rows))

            if schema_type:
                converted_data = self.to_schema(data=dict_rows, schema_type=schema_type)
                return SelectResult[ModelDTOT](
                    statement=statement,
                    data=converted_data,
                    column_names=column_names,
                )
            return SelectResult[dict[str, Any]](
                statement=statement,
                data=dict_rows,
                column_names=column_names,
            )

    async def _wrap_execute_result(
        self,
        statement: SQL,
        raw_driver_result: Any,
        **kwargs: Any,
    ) -> ExecuteResult:
        async with instrument_operation_async(self, "psqlpy_async_wrap_execute", "database"):
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

            # Handle both QueryResult and mock BatchResult
            if hasattr(raw_driver_result, "affected_rows"):
                rows_affected = raw_driver_result.affected_rows
            elif hasattr(raw_driver_result, "rowcount"):
                rows_affected = raw_driver_result.rowcount
            else:
                rows_affected = -1

            if self.instrumentation_config.log_results_count:
                logger.debug("Psqlpy execute operation affected %d rows", rows_affected)

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

    def _connection(self, connection: Optional[PsqlpyConnection] = None) -> PsqlpyConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection
