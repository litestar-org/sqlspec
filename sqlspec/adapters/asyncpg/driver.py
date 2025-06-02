# ruff: noqa: PLR6301
import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from asyncpg import Connection as AsyncpgNativeConnection
from asyncpg import Record
from typing_extensions import TypeAlias

from sqlspec.config import InstrumentationConfig
from sqlspec.driver import AsyncDriverAdapterProtocol
from sqlspec.statement.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, ExecuteResult, SelectResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, SQLParameterType
from sqlspec.utils.telemetry import instrument_operation_async

if TYPE_CHECKING:
    from asyncpg.pool import PoolConnectionProxy

    from sqlspec.statement.filters import StatementFilter

__all__ = ("AsyncpgConnection", "AsyncpgDriver")

logger = logging.getLogger("sqlspec")

if TYPE_CHECKING:
    AsyncpgConnection: TypeAlias = Union[AsyncpgNativeConnection[Record], PoolConnectionProxy[Record]]
else:
    AsyncpgConnection: TypeAlias = Union[AsyncpgNativeConnection, Any]


class AsyncpgDriver(
    AsyncDriverAdapterProtocol[AsyncpgConnection, DictRow],
    AsyncArrowMixin[AsyncpgConnection],
    SQLTranslatorMixin[AsyncpgConnection],
    ResultConverter,
):
    """AsyncPG PostgreSQL Driver Adapter. Modern protocol implementation."""

    dialect: str = "postgres"
    __supports_arrow__: ClassVar[bool] = True

    def __init__(
        self,
        connection: AsyncpgConnection,
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
        return ParameterStyle.NUMERIC

    async def _execute_impl(
        self,
        statement: SQL,
        connection: Optional[AsyncpgConnection] = None,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "asyncpg_execute", "database"):
            conn = self._connection(connection)

            if statement.is_script:
                final_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
                if self.instrumentation_config.log_queries:
                    logger.debug("Executing SQL script: %s", final_sql)
                return await conn.execute(final_sql)

            final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())

            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL: %s", final_sql)

            if statement.is_many:
                params_list: list[tuple[Any, ...]] = []
                if statement.parameters and isinstance(statement.parameters, Sequence):
                    for param_set in statement.parameters:
                        if isinstance(param_set, (list, tuple)):
                            params_list.append(tuple(param_set))
                        elif param_set is None:
                            params_list.append(())
                        else:
                            params_list.append((param_set,))

                if self.instrumentation_config.log_parameters and params_list:
                    logger.debug("Query parameters (batch): %s", params_list)

                return await conn.executemany(final_sql, params_list)
            # Single execution
            # Use the statement's already-processed parameters directly
            processed_params = statement._merged_parameters if hasattr(statement, "_merged_parameters") else None
            args: list[Any] = []

            if isinstance(processed_params, (list, tuple)):
                args.extend(processed_params)
            elif processed_params is not None:
                args.append(processed_params)

            if self.instrumentation_config.log_parameters and args:
                logger.debug("Query parameters: %s", args)

            if AsyncDriverAdapterProtocol.returns_rows(statement.expression):
                return await conn.fetch(final_sql, *args)
            return await conn.execute(final_sql, *args)

    async def _wrap_select_result(
        self,
        statement: SQL,
        raw_driver_result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SelectResult[ModelDTOT], SelectResult[dict[str, Any]]]:
        async with instrument_operation_async(self, "asyncpg_wrap_select", "database"):
            records = cast("list[Record]", raw_driver_result)

            if not records:
                return SelectResult[dict[str, Any]](
                    statement=statement,
                    data=[],
                    column_names=[],
                )

            column_names = list(records[0].keys())
            rows_as_dicts: list[dict[str, Any]] = [dict(record) for record in records]

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

    async def _wrap_execute_result(
        self,
        statement: SQL,
        raw_driver_result: Any,
        **kwargs: Any,
    ) -> ExecuteResult:
        async with instrument_operation_async(self, "asyncpg_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()

            if isinstance(raw_driver_result, str):
                # Handle SCRIPT execution result or parse status string
                if "SCRIPT" in operation_type:
                    execute_data = {
                        "rows_affected": 0,
                        "last_inserted_id": None,
                        "inserted_ids": [],
                        "returning_data": None,
                        "operation_type": operation_type,
                    }
                    return ExecuteResult(
                        statement=statement,
                        data=execute_data,
                        rows_affected=0,
                        operation_type=operation_type,
                    )

                # Parse status string like "INSERT 0 1" or "UPDATE 5"
                import re

                rows_affected = 0
                match = re.match(r"^(?:INSERT|UPDATE|DELETE|MERGE)\s+\d+\s+(\d+)$", raw_driver_result, re.IGNORECASE)
                if match:
                    rows_affected = int(match.group(1))
            elif isinstance(raw_driver_result, int):
                rows_affected = raw_driver_result
            else:
                rows_affected = 0

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

    async def select_to_arrow(
        self,
        statement: "Union[str, SQL, Any]",
        parameters: Optional[SQLParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[AsyncpgConnection] = None,
        config: Optional[SQLConfig] = None,
        **kwargs: Any,
    ) -> "ArrowResult":
        async with instrument_operation_async(self, "asyncpg_select_to_arrow", "database"):
            import pyarrow as pa

            conn = self._connection(connection)
            current_config = config or self.config
            stmt_obj = SQL(
                statement, parameters, *(filters or ()), dialect=self.dialect, config=current_config, **kwargs
            )
            stmt_obj.validate()

            if not AsyncDriverAdapterProtocol.returns_rows(stmt_obj.expression):
                op_type = (
                    str(stmt_obj.expression.key).upper()
                    if stmt_obj.expression and hasattr(stmt_obj.expression, "key")
                    else "UNKNOWN"
                )
                msg = f"Cannot fetch Arrow table for a non-query statement. Command type: {op_type}"
                raise TypeError(msg)

            final_sql = stmt_obj.to_sql(placeholder_style=self._get_placeholder_style())
            # Use the statement's already-processed parameters instead of calling get_parameters()
            processed_params = stmt_obj._merged_parameters if hasattr(stmt_obj, "_merged_parameters") else None

            args: list[Any] = []
            if isinstance(processed_params, (list, tuple)):
                args.extend(processed_params)
            elif processed_params is not None:
                args.append(processed_params)

            records = await conn.fetch(final_sql, *args)

            if not records:
                return ArrowResult(statement=stmt_obj, data=pa.Table.from_arrays([], names=[]))

            # Convert asyncpg records to Arrow table
            column_names = list(records[0].keys())
            columns_data = []

            for col_name in column_names:
                column_values = [record[col_name] for record in records]
                columns_data.append(column_values)

            arrow_table = pa.Table.from_arrays(columns_data, names=column_names)
            return ArrowResult(statement=stmt_obj, data=arrow_table)

    def _connection(self, connection: Optional[AsyncpgConnection] = None) -> AsyncpgConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection
