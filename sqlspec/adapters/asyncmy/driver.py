# ruff: noqa: PLR6301
import logging
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from asyncmy import Connection
from typing_extensions import TypeAlias

from sqlspec.driver import AsyncDriverAdapterProtocol
from sqlspec.statement.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.typing import DictRow, ModelDTOT
from sqlspec.utils.telemetry import instrument_operation_async

if TYPE_CHECKING:
    from asyncmy.cursors import Cursor

    from sqlspec.config import InstrumentationConfig
    from sqlspec.statement.sql import SQL, SQLConfig


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
        connection: "AsyncmyConnection",  # pyright: ignore
        config: "Optional[SQLConfig]" = None,
        instrumentation_config: "Optional[InstrumentationConfig]" = None,
    ) -> None:
        super().__init__(
            connection=connection,
            config=config,
            instrumentation_config=instrumentation_config,
            default_row_type=DictRow,
        )

    def _get_placeholder_style(self) -> "ParameterStyle":
        return ParameterStyle.PYFORMAT_POSITIONAL

    @asynccontextmanager
    async def _get_cursor(self, connection: "Optional[AsyncmyConnection]" = None) -> "AsyncGenerator[Cursor, None]":  # pyright: ignore
        conn_to_use = connection or self.connection
        cursor: Cursor = await conn_to_use.cursor()
        try:
            yield cursor
        finally:
            if hasattr(cursor, "close") and callable(cursor.close):
                await cursor.close()

    async def _execute_impl(
        self,
        statement: "SQL",
        connection: "Optional[AsyncmyConnection]" = None,  # pyright: ignore
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "asyncmy_execute", "database"):
            conn = self._connection(connection)

            if statement.is_script:
                final_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
                if self.instrumentation_config.log_queries:
                    logger.debug("Executing SQL script: %s", final_sql)
                async with self._get_cursor(conn) as cursor:
                    # Asyncmy might require multi=True for scripts, depending on how they are structured.
                    # Assuming a single call can handle multiple statements if needed.
                    await cursor.execute(final_sql)  # For scripts, parameters are usually inlined
                return "SCRIPT EXECUTED"

            final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())
            params_to_execute = statement.parameters

            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL: %s", final_sql)

            if statement.is_many:
                # asyncmy executemany expects a list of sequences (tuples or lists)
                params_list: list[Union[list[Any], tuple[Any, ...]]] = []
                if params_to_execute and isinstance(params_to_execute, Sequence):
                    for param_set in params_to_execute:
                        if isinstance(param_set, (list, tuple)):
                            params_list.append(param_set)
                        elif param_set is None:  # Should be handled by SQL object or be an empty sequence
                            params_list.append([])
                        else:  # Single item in a batch set
                            params_list.append([param_set])
                # else: params_list remains empty for an empty batch

                if self.instrumentation_config.log_parameters and params_list:
                    logger.debug("Query parameters (batch): %s", params_list)

                async with self._get_cursor(conn) as cursor:
                    await cursor.executemany(final_sql, params_list)
                    # rowcount for executemany indicates number of affected rows or statements
                    return cursor.rowcount if cursor.rowcount != -1 else len(params_list)
            else:
                # Single execution
                # asyncmy execute expects params as a list or tuple, or dict for named placeholders
                # Since we use pyformat_positional (%), it expects a sequence.
                processed_params: Optional[Union[list[Any], tuple[Any, ...]]] = None
                if params_to_execute is not None:
                    if isinstance(params_to_execute, (list, tuple)):
                        processed_params = params_to_execute
                    else:  # Single parameter
                        processed_params = [params_to_execute]

                if self.instrumentation_config.log_parameters and processed_params:
                    logger.debug("Query parameters: %s", processed_params)

                async with self._get_cursor(conn) as cursor:
                    await cursor.execute(final_sql, processed_params)

                    if AsyncDriverAdapterProtocol.returns_rows(statement.expression):
                        # For asyncmy, fetchall() returns a list of tuples (default) or dicts if dict cursor used.
                        # We also need the description for column names if not using a dict cursor by default.
                        # Assuming _get_cursor provides a cursor that gives us what _wrap_select_result expects.
                        # The `_wrap_select_result` will handle fetching and processing.
                        return cursor  # Pass the cursor itself to _wrap_select_result

                    # For DML, the cursor contains rowcount etc.
                    return cursor

    async def _wrap_select_result(
        self,
        statement: "SQL",
        result: "Any",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: "Any",
    ) -> "Union[SQLResult[ModelDTOT], SQLResult[dict[str, Any]]]":
        async with instrument_operation_async(self, "asyncmy_wrap_select", "database"):
            cursor = cast("Cursor", result)

            try:
                results: list[tuple[Any, ...]] = await cursor.fetchall()
            except Exception:
                results = []

            column_names = [desc[0] for desc in cursor.description or []]

            if not results:
                return SQLResult[dict[str, Any]](
                    statement=statement,
                    data=[],
                    column_names=column_names,
                    operation_type="SELECT",
                )

            rows_as_dicts = [dict(zip(column_names, row)) for row in results]

            if self.instrumentation_config.log_results_count:
                logger.debug("Asyncmy query returned %d rows", len(rows_as_dicts))

            if schema_type:
                converted_data_seq = self.to_schema(data=rows_as_dicts, schema_type=schema_type)
                converted_data_list = list(converted_data_seq) if converted_data_seq is not None else []
                return SQLResult[ModelDTOT](
                    statement=statement,
                    data=converted_data_list,
                    column_names=column_names,
                    operation_type="SELECT",
                )

            return SQLResult[dict[str, Any]](
                statement=statement,
                data=rows_as_dicts,
                column_names=column_names,
                operation_type="SELECT",
            )

    async def _wrap_execute_result(
        self,
        statement: "SQL",
        result: "Any",
        **kwargs: "Any",
    ) -> "SQLResult[dict[str, Any]]":
        async with instrument_operation_async(self, "asyncmy_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()

            if isinstance(result, str) and result == "SCRIPT EXECUTED":
                return SQLResult[dict[str, Any]](
                    statement=statement,
                    data=[],
                    rows_affected=0,
                    operation_type=operation_type or "SCRIPT",
                    metadata={"status_message": result},
                )

            cursor = cast("Cursor", result)
            rows_affected = getattr(cursor, "rowcount", -1)
            last_inserted_id = getattr(cursor, "lastrowid", None)

            returned_data: list[dict[str, Any]] = []

            if self.instrumentation_config.log_results_count:
                logger.debug("Asyncmy execute operation affected %d rows", rows_affected)
                if last_inserted_id is not None:
                    logger.debug("Asyncmy last inserted ID: %s", last_inserted_id)

            return SQLResult[dict[str, Any]](
                statement=statement,
                data=returned_data,
                rows_affected=rows_affected,
                last_inserted_id=last_inserted_id,
                operation_type=operation_type,
            )

    async def _select_to_arrow_impl(
        self,
        stmt_obj: "SQL",
        connection: "AsyncmyConnection",  # pyright: ignore
        **kwargs: "Any",
    ) -> ArrowResult:
        import pyarrow as pa

        final_sql = stmt_obj.to_sql(placeholder_style=self._get_placeholder_style())
        final_params = stmt_obj.parameters

        cursor = await connection.cursor()
        await cursor.execute(final_sql, final_params or {})

        results, description = await cursor.fetchall(), cursor.description
        if not results:
            column_names_from_desc = [col[0] for col in description or []]
            return ArrowResult(statement=stmt_obj, data=pa.Table.from_arrays([], names=column_names_from_desc))

        column_names = [column[0] for column in description or []]
        if not column_names:
            return ArrowResult(statement=stmt_obj, data=pa.Table.from_arrays([], names=[]))

        columns_data = [list(col) for col in zip(*results)]

        arrow_table = pa.Table.from_arrays(columns_data, names=column_names)
        return ArrowResult(statement=stmt_obj, data=arrow_table)

    def _connection(self, connection: Optional[AsyncmyConnection] = None) -> AsyncmyConnection:  # pyright: ignore
        """Get the connection to use for the operation."""
        return connection or self.connection
