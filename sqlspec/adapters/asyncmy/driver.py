# ruff: noqa: PLR6301
import logging
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from asyncmy import Connection
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
    from asyncmy.cursors import Cursor

    from sqlspec.statement.filters import StatementFilter

__all__ = ("AsyncmyConnection", "AsyncmyDriver")

logger = logging.getLogger("sqlspec")

AsyncmyConnection: TypeAlias = Connection


class AsyncmyDriver(
    AsyncDriverAdapterProtocol[AsyncmyConnection, DictRow],
    AsyncArrowMixin[AsyncmyConnection],
    SQLTranslatorMixin[AsyncmyConnection],
    ResultConverter,
):
    """Asyncmy MySQL/MariaDB Driver Adapter. Modern protocol implementation."""

    dialect: str = "mysql"
    __supports_arrow__: ClassVar[bool] = True

    def __init__(
        self,
        connection: AsyncmyConnection,  # pyright: ignore
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
        return ParameterStyle.PYFORMAT_POSITIONAL

    @asynccontextmanager
    async def _get_cursor(self, connection: Optional[AsyncmyConnection] = None) -> AsyncGenerator["Cursor", None]:  # pyright: ignore
        conn_to_use = connection or self.connection
        cursor: Cursor = await conn_to_use.cursor()
        try:
            yield cursor
        finally:
            if hasattr(cursor, "close") and callable(cursor.close):
                await cursor.close()

    async def _execute_impl(
        self,
        statement: SQL,
        parameters: Optional[SQLParameterType] = None,
        connection: Optional[AsyncmyConnection] = None,  # pyright: ignore
        config: Optional[SQLConfig] = None,
        is_many: bool = False,
        is_script: bool = False,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "asyncmy_execute", "database"):
            conn = self._connection(connection)
            if config is not None and config != statement.config:
                statement = statement.copy(config=config)

            if is_script:
                final_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
                if self.instrumentation_config.log_queries:
                    logger.debug("Executing SQL script: %s", final_sql)
                async with self._get_cursor(conn) as cursor:
                    await cursor.execute(final_sql, multi=True)  # type: ignore[call-arg]
                return "SCRIPT EXECUTED"

            final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())

            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL: %s", final_sql)

            if is_many:
                params_list: list[Union[list[Any], tuple[Any, ...]]] = []
                if parameters and isinstance(parameters, Sequence):
                    for param_set in parameters:
                        if isinstance(param_set, (list, tuple)):
                            params_list.append(param_set)
                        elif param_set is None:
                            params_list.append([])
                        else:
                            params_list.append([param_set])

                if self.instrumentation_config.log_parameters and params_list:
                    logger.debug("Query parameters (batch): %s", params_list)

                async with self._get_cursor(conn) as cursor:
                    await cursor.executemany(final_sql, params_list)  # type: ignore[arg-type]
                    return cursor.rowcount if cursor.rowcount != -1 else len(params_list)
            else:
                # Single execution
                ordered_params = statement.get_parameters(style=self._get_placeholder_style())
                processed_params: Optional[Union[list[Any], tuple[Any, ...]]] = None

                if ordered_params is not None:
                    processed_params = ordered_params if isinstance(ordered_params, (list, tuple)) else [ordered_params]

                if self.instrumentation_config.log_parameters and processed_params:
                    logger.debug("Query parameters: %s", processed_params)

                async with self._get_cursor(conn) as cursor:
                    await cursor.execute(final_sql, processed_params)  # type: ignore[arg-type]

                    if AsyncDriverAdapterProtocol.returns_rows(statement.expression):
                        results = await cursor.fetchall()
                        return results, cursor.description
                    return cursor

    async def _wrap_select_result(
        self,
        statement: SQL,
        raw_driver_result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SelectResult[ModelDTOT], SelectResult[dict[str, Any]]]:
        async with instrument_operation_async(self, "asyncmy_wrap_select", "database"):
            cursor = cast("Cursor", raw_driver_result)

            try:
                results = await cursor.fetchall()
            except Exception:  # noqa: BLE001
                results = []

            if not results:
                return SelectResult[dict[str, Any]](
                    statement=statement,
                    data=[],
                    column_names=[],
                )

            column_names = [desc[0] for desc in cursor.description or []]

            if self.instrumentation_config.log_results_count:
                logger.debug("Asyncmy query returned %d rows", len(results))

            if schema_type:
                converted_data = self.to_schema(data=results, schema_type=schema_type)
                return SelectResult[ModelDTOT](
                    statement=statement,
                    data=converted_data,
                    column_names=column_names,
                )

            return SelectResult[dict[str, Any]](
                statement=statement,
                data=[dict(zip(column_names, results[0])) if results else {}],
                column_names=column_names,
            )

    async def _wrap_execute_result(
        self,
        statement: SQL,
        raw_driver_result: Any,
        **kwargs: Any,
    ) -> ExecuteResult:
        async with instrument_operation_async(self, "asyncmy_wrap_execute", "database"):
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

            cursor = cast("Cursor", raw_driver_result)
            rows_affected = getattr(cursor, "rowcount", -1)

            if self.instrumentation_config.log_results_count:
                logger.debug("Asyncmy execute operation affected %d rows", rows_affected)

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
        connection: Optional[AsyncmyConnection] = None,  # pyright: ignore
        config: Optional[SQLConfig] = None,
        **kwargs: Any,
    ) -> "ArrowResult":
        async with instrument_operation_async(self, "asyncmy_select_to_arrow", "database"):
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
            final_params = stmt_obj.get_parameters(style=self._get_placeholder_style())

            cursor = await conn.cursor()
            await cursor.execute(final_sql, final_params or {})

            results, description = await cursor.fetchall(), cursor.description
            if not results:
                return ArrowResult(statement=stmt_obj, data=pa.Table.from_arrays([], names=[]))

            column_names = [column[0] for column in description or []]
            columns_data = []

            for col_name in column_names:
                column_values = [
                    row[description.index(next(desc for desc in description if desc[0] == col_name))] for row in results
                ]
                columns_data.append(column_values)

            arrow_table = pa.Table.from_arrays(columns_data, names=column_names)
            return ArrowResult(statement=stmt_obj, data=arrow_table)

    def _connection(self, connection: Optional[AsyncmyConnection] = None) -> AsyncmyConnection:  # pyright: ignore
        """Get the connection to use for the operation."""
        return connection or self.connection
