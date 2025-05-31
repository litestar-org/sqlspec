# ruff: noqa: PLR6301
import logging
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

import aiosqlite
import pyarrow as pa

from sqlspec.config import InstrumentationConfig
from sqlspec.driver import AsyncDriverAdapterProtocol
from sqlspec.statement.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, ExecuteResult, SelectResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, SQLParameterType
from sqlspec.utils.telemetry import instrument_operation_async

if TYPE_CHECKING:
    from sqlspec.statement.filters import StatementFilter

__all__ = ("AiosqliteConnection", "AiosqliteDriver")

logger = logging.getLogger("sqlspec")

AiosqliteConnection = aiosqlite.Connection


class AiosqliteDriver(
    AsyncDriverAdapterProtocol[AiosqliteConnection, DictRow],
    AsyncArrowMixin[AiosqliteConnection],
    SQLTranslatorMixin[AiosqliteConnection],
    ResultConverter,
):
    """Aiosqlite SQLite Driver Adapter. Modern protocol implementation."""

    dialect: str = "sqlite"
    __supports_arrow__: ClassVar[bool] = True

    def __init__(
        self,
        connection: AiosqliteConnection,
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
        return ParameterStyle.QMARK

    @asynccontextmanager
    async def _get_cursor(
        self, connection: Optional[AiosqliteConnection] = None
    ) -> AsyncGenerator[aiosqlite.Cursor, None]:
        conn_to_use = connection or self.connection
        conn_to_use.row_factory = aiosqlite.Row
        cursor = await conn_to_use.cursor()
        try:
            yield cursor
        finally:
            await cursor.close()

    async def _execute_impl(
        self,
        statement: SQL,
        parameters: Optional[SQLParameterType] = None,
        connection: Optional[AiosqliteConnection] = None,
        config: Optional[SQLConfig] = None,
        is_many: bool = False,
        is_script: bool = False,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "aiosqlite_execute", "database"):
            conn = self._connection(connection)

            if is_script:
                final_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
                if self.instrumentation_config.log_queries:
                    logger.debug("Executing SQL script: %s", final_sql)
                async with self._get_cursor(conn) as cursor:
                    await cursor.executescript(final_sql)
                    await conn.commit()
                return "SCRIPT EXECUTED"

            final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())

            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL: %s", final_sql)

            if is_many:
                params_list: list[tuple[Any, ...]] = []
                if parameters and isinstance(parameters, Sequence):
                    for param_set in parameters:
                        if isinstance(param_set, (list, tuple)):
                            params_list.append(tuple(param_set))
                        elif param_set is None:
                            params_list.append(())
                        else:
                            params_list.append((param_set,))

                if self.instrumentation_config.log_parameters and params_list:
                    logger.debug("Query parameters (batch): %s", params_list)

                async with self._get_cursor(conn) as cursor:
                    await cursor.executemany(final_sql, params_list)
                    await conn.commit()
                    return cursor.rowcount if cursor.rowcount != -1 else len(params_list)
            else:
                # Single execution
                ordered_params = statement.get_parameters(style=self._get_placeholder_style())
                db_params: tuple[Any, ...] = ()

                if ordered_params is not None:
                    if isinstance(ordered_params, (list, tuple)):
                        db_params = tuple(ordered_params)
                    else:
                        db_params = (ordered_params,)

                if self.instrumentation_config.log_parameters and db_params:
                    logger.debug("Query parameters: %s", db_params)

                async with self._get_cursor(conn) as cursor:
                    await cursor.execute(final_sql, db_params)

                    if AsyncDriverAdapterProtocol.returns_rows(statement.expression):
                        return await cursor.fetchall()
                    await conn.commit()
                    return cursor

    async def _wrap_select_result(
        self,
        statement: SQL,
        raw_driver_result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SelectResult[ModelDTOT], SelectResult[dict[str, Any]]]:
        async with instrument_operation_async(self, "aiosqlite_wrap_select", "database"):
            cursor = cast("aiosqlite.Cursor", raw_driver_result)
            fetched_data = await cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description or []]

            # Convert sqlite3.Row objects to dictionaries
            rows_as_dicts: list[dict[str, Any]] = [dict(row) for row in fetched_data]

            if self.instrumentation_config.log_results_count:
                logger.debug("Aiosqlite query returned %d rows", len(rows_as_dicts))

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
        async with instrument_operation_async(self, "aiosqlite_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()

            if isinstance(raw_driver_result, str):
                execute_data = {
                    "rows_affected": 0,
                    "last_inserted_id": None,
                    "inserted_ids": [],
                    "returning_data": None,
                    "operation_type": operation_type or "SCRIPT",
                }
                return ExecuteResult(
                    statement=statement,
                    data=execute_data,
                    rows_affected=0,
                    operation_type=operation_type or "SCRIPT",
                )

            cursor = cast("aiosqlite.Cursor", raw_driver_result)
            rows_affected = getattr(cursor, "rowcount", -1)

            if self.instrumentation_config.log_results_count:
                logger.debug("Aiosqlite execute operation affected %d rows", rows_affected)

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
        connection: Optional[AiosqliteConnection] = None,
        config: Optional[SQLConfig] = None,
        **kwargs: Any,
    ) -> "ArrowResult":
        async with instrument_operation_async(self, "aiosqlite_select_to_arrow", "database"):
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
            ordered_params = stmt_obj.get_parameters(style=self._get_placeholder_style())

            db_params: tuple[Any, ...] = ()
            if ordered_params is not None:
                db_params = tuple(ordered_params) if isinstance(ordered_params, (list, tuple)) else (ordered_params,)

            async with self._get_cursor(conn) as cursor:
                await cursor.execute(final_sql, db_params)
                rows = await cursor.fetchall()

                if not rows:
                    return ArrowResult(statement=stmt_obj, data=pa.Table.from_arrays([], names=[]))

                # Convert aiosqlite rows to Arrow table
                row_list = list(rows)
                column_names = list(row_list[0].keys())
                columns_data = []

                for col_name in column_names:
                    column_values = [row[col_name] for row in row_list]
                    columns_data.append(column_values)

                arrow_table = pa.Table.from_arrays(columns_data, names=column_names)
                return ArrowResult(statement=stmt_obj, data=arrow_table)

    def _connection(self, connection: Optional[AiosqliteConnection] = None) -> AiosqliteConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection
