# ruff: noqa: PLR6301
import logging
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

import aiosqlite
import pyarrow as pa

from sqlspec.driver import AsyncDriverAdapterProtocol
from sqlspec.statement.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, RowT
from sqlspec.utils.telemetry import instrument_operation_async

if TYPE_CHECKING:
    from sqlspec.config import InstrumentationConfig

__all__ = ("AiosqliteConnection", "AiosqliteDriver")

logger = logging.getLogger("sqlspec")

AiosqliteConnection = aiosqlite.Connection


class AiosqliteDriver(
    AsyncDriverAdapterProtocol[AiosqliteConnection, RowT],
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
        connection: Optional[AiosqliteConnection] = None,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "aiosqlite_execute", "database"):
            conn = self._connection(connection)

            if statement.is_script:
                final_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
                if self.instrumentation_config.log_queries:
                    logger.debug("Executing SQL script: %s", final_sql)
                async with self._get_cursor(conn) as cursor:
                    await cursor.executescript(final_sql)
                return "SCRIPT EXECUTED"

            final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())
            params_to_execute = statement.parameters

            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL: %s", final_sql)

            if statement.is_many:
                params_list: list[tuple[Any, ...]] = []
                if params_to_execute and isinstance(params_to_execute, Sequence):
                    for param_set in params_to_execute:
                        if isinstance(param_set, (list, tuple)):
                            params_list.append(tuple(param_set))
                        elif param_set is None:
                            params_list.append(())
                        else:
                            params_list.append((param_set,))
                else:
                    params_list = []

                if self.instrumentation_config.log_parameters and params_list:
                    logger.debug("Query parameters (batch): %s", params_list)

                async with self._get_cursor(conn) as cursor:
                    await cursor.executemany(final_sql, params_list)
                    return cursor.rowcount if cursor.rowcount != -1 else len(params_list)
            else:
                db_params: tuple[Any, ...] = ()
                if params_to_execute is not None:
                    if isinstance(params_to_execute, (list, tuple)):
                        db_params = tuple(params_to_execute)
                    else:
                        db_params = (params_to_execute,)

                if self.instrumentation_config.log_parameters and db_params:
                    logger.debug("Query parameters: %s", db_params)

                async with self._get_cursor(conn) as cursor:
                    await cursor.execute(final_sql, db_params)

                    if AsyncDriverAdapterProtocol.returns_rows(statement.expression):
                        # Return data and description for _wrap_select_result
                        return {"data": await cursor.fetchall(), "description": cursor.description}
                    return cursor  # For DML, return cursor to _wrap_execute_result

    async def _wrap_select_result(
        self,
        statement: SQL,
        result: Any,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        async with instrument_operation_async(self, "aiosqlite_wrap_select", "database"):
            if not isinstance(result, dict) or "data" not in result or "description" not in result:
                logger.warning("Aiosqlite _wrap_select_result expects a dict with 'data' and 'description'.")
                return SQLResult[RowT](statement=statement, data=[], column_names=[], operation_type="SELECT")

            column_names = [desc[0] for desc in result["description"] or []]

            rows_as_dicts: list[dict[str, Any]] = [dict(row) for row in result["data"]]

            if self.instrumentation_config.log_results_count:
                logger.debug("Aiosqlite query returned %d rows", len(rows_as_dicts))

            if schema_type:
                converted_data_seq = self.to_schema(data=rows_as_dicts, schema_type=schema_type)
                return SQLResult[ModelDTOT](
                    statement=statement,
                    data=list(converted_data_seq) if converted_data_seq is not None else [],
                    column_names=column_names,
                    operation_type="SELECT",
                )
            return SQLResult[RowT](
                statement=statement,
                data=rows_as_dicts,
                column_names=column_names,
                operation_type="SELECT",
            )

    async def _wrap_execute_result(
        self,
        statement: SQL,
        result: Any,
        **kwargs: Any,
    ) -> SQLResult[RowT]:
        async with instrument_operation_async(self, "aiosqlite_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()

            if isinstance(result, str) and result == "SCRIPT EXECUTED":
                return SQLResult[RowT](
                    statement=statement,
                    data=[],
                    rows_affected=0,
                    operation_type=operation_type or "SCRIPT",
                    metadata={"status_message": result},
                )

            cursor = cast("aiosqlite.Cursor", result)
            rows_affected = getattr(cursor, "rowcount", -1)
            last_inserted_id = getattr(cursor, "lastrowid", None)

            returned_data: list[dict[str, Any]] = []

            if self.instrumentation_config.log_results_count:
                logger.debug("Aiosqlite execute operation affected %d rows", rows_affected)
                if last_inserted_id is not None:
                    logger.debug("Aiosqlite last inserted ID: %s", last_inserted_id)

            return SQLResult[RowT](
                statement=statement,
                data=returned_data,
                rows_affected=rows_affected,
                last_inserted_id=last_inserted_id,
                operation_type=operation_type,
            )

    async def _select_to_arrow_impl(
        self,
        stmt_obj: "SQL",
        connection: "AiosqliteConnection",
        **kwargs: Any,
    ) -> "ArrowResult":
        final_sql = stmt_obj.to_sql(placeholder_style=self._get_placeholder_style())
        ordered_params = stmt_obj.get_parameters(style=self._get_placeholder_style())

        db_params: tuple[Any, ...] = ()
        if ordered_params is not None:
            db_params = tuple(ordered_params) if isinstance(ordered_params, (list, tuple)) else (ordered_params,)

        async with self._get_cursor(connection) as cursor:
            if self.instrumentation_config.log_queries:
                logger.debug("Executing aiosqlite Arrow query: %s", final_sql)
            if self.instrumentation_config.log_parameters and db_params:
                logger.debug("Query parameters for aiosqlite Arrow: %s", db_params)

            await cursor.execute(final_sql, db_params)
            rows = await cursor.fetchall()

            if not rows:
                column_names_from_desc = [desc[0] for desc in cursor.description or []]
                return ArrowResult(statement=stmt_obj, data=pa.Table.from_arrays([], names=column_names_from_desc))

            row_list = list(rows)
            if not row_list:
                return ArrowResult(statement=stmt_obj, data=pa.Table.from_arrays([], names=[]))

            column_names = list(row_list[0].keys())

            # Transpose list of dict-like rows to dict of lists (columnar format for PyArrow)
            data_for_arrow = {col_name: [row[col_name] for row in row_list] for col_name in column_names}

            # Alternative using from_arrays if data_for_arrow is a dict of lists (columns)
            columns_as_lists = list(data_for_arrow.values())
            arrow_table = pa.Table.from_arrays(columns_as_lists, names=column_names)

            return ArrowResult(statement=stmt_obj, data=arrow_table)

    def _connection(self, connection: Optional[AiosqliteConnection] = None) -> AiosqliteConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection
