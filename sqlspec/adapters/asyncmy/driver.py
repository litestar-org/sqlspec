# ruff: noqa: PLR6301
import logging
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from asyncmy import Connection
from litestar.utils import ensure_async_callable
from typing_extensions import TypeAlias

from sqlspec.config import InstrumentationConfig
from sqlspec.driver import AsyncDriverAdapterProtocol
from sqlspec.driver.mixins import AsyncStorageMixin, SQLTranslatorMixin, ToSchemaMixin
from sqlspec.exceptions import wrap_exceptions
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import DMLResultDict, ScriptResultDict, SelectResultDict, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, RowT
from sqlspec.utils.telemetry import instrument_operation_async

if TYPE_CHECKING:
    from asyncmy.cursors import Cursor
    from sqlglot.dialects.dialect import DialectType

__all__ = ("AsyncmyConnection", "AsyncmyDriver")

logger = logging.getLogger("sqlspec")

AsyncmyConnection: TypeAlias = Connection


class AsyncmyDriver(
    AsyncDriverAdapterProtocol[AsyncmyConnection, RowT], SQLTranslatorMixin, AsyncStorageMixin, ToSchemaMixin
):
    """Asyncmy MySQL/MariaDB Driver Adapter. Modern protocol implementation."""

    dialect: "DialectType" = "mysql"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (ParameterStyle.POSITIONAL_PYFORMAT,)
    default_parameter_style: ParameterStyle = ParameterStyle.POSITIONAL_PYFORMAT
    __supports_arrow__: ClassVar[bool] = True
    __supports_parquet__: ClassVar[bool] = False

    def __init__(
        self,
        connection: AsyncmyConnection,  # pyright: ignore
        config: Optional[SQLConfig] = None,
        instrumentation_config: Optional[InstrumentationConfig] = None,
        default_row_type: type[DictRow] = DictRow,
    ) -> None:
        super().__init__(
            connection=connection,
            config=config,
            instrumentation_config=instrumentation_config,
            default_row_type=default_row_type,
        )

    @asynccontextmanager
    async def _get_cursor(self, connection: "Optional[AsyncmyConnection]" = None) -> "AsyncGenerator[Cursor, None]":
        conn_to_use = connection or self.connection
        cursor: Cursor = conn_to_use.cursor()
        try:
            yield cursor
        finally:
            with wrap_exceptions(wrap_exceptions=False, suppress=AttributeError):
                if callable(cursor.close):
                    await ensure_async_callable(cursor.close)()  # pyright: ignore

    async def _execute_statement(
        self,
        statement: SQL,
        connection: "Optional[AsyncmyConnection]" = None,  # pyright: ignore
        **kwargs: Any,
    ) -> Union[SelectResultDict, DMLResultDict, ScriptResultDict]:
        if statement.is_script:
            sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
            return await self._execute_script(sql, connection=connection, **kwargs)

        # Determine if we need to convert parameter style
        detected_style = statement.parameter_style
        target_style = self.default_parameter_style

        # Only convert if the detected style is not supported
        if detected_style and detected_style not in self.supported_parameter_styles:
            target_style = self.default_parameter_style
        elif detected_style:
            # Use the detected style if it's supported
            target_style = detected_style

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
        connection: "Optional[AsyncmyConnection]" = None,
        **kwargs: Any,
    ) -> Union[SelectResultDict, DMLResultDict]:
        async with instrument_operation_async(self, "asyncmy_execute", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL: %s", sql)

            # AsyncMy doesn't like empty lists/tuples, convert to None
            if parameters in ([], ()):
                parameters = None

            if self.instrumentation_config.log_parameters and parameters:
                logger.debug("Query parameters: %s", parameters)

            async with self._get_cursor(conn) as cursor:
                # AsyncMy expects list/tuple parameters or dict for named params
                await cursor.execute(sql, parameters)

                # For SELECT queries, return cursor so _wrap_select_result can fetch from it
                is_select = self.returns_rows(statement.expression)
                # If expression is None (parsing disabled or failed), check SQL string
                if not is_select and statement.expression is None:
                    sql_upper = sql.strip().upper()
                    is_select = any(sql_upper.startswith(prefix) for prefix in ["SELECT", "WITH", "VALUES", "TABLE"])

                if is_select:
                    # For SELECT queries, fetch data and return SelectResultDict
                    data = await cursor.fetchall()
                    column_names = [desc[0] for desc in cursor.description or []]
                    result: SelectResultDict = {
                        "data": data,
                        "column_names": column_names,
                        "rows_affected": len(data),
                    }
                    return result

                # For DML/DDL queries, return DMLResultDict
                dml_result: DMLResultDict = {
                    "rows_affected": cursor.rowcount if cursor.rowcount is not None else -1,
                    "status_message": "OK",
                }
                return dml_result

    async def _execute_many(
        self,
        sql: str,
        param_list: Any,
        connection: "Optional[AsyncmyConnection]" = None,
        **kwargs: Any,
    ) -> DMLResultDict:
        async with instrument_operation_async(self, "asyncmy_execute_many", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL (executemany): %s", sql)
            if self.instrumentation_config.log_parameters and param_list:
                logger.debug("Query parameters (batch): %s", param_list)

            # Convert parameter list to proper format for executemany
            params_list: list[Union[list[Any], tuple[Any, ...]]] = []
            if param_list and isinstance(param_list, Sequence):
                for param_set in param_list:
                    if isinstance(param_set, (list, tuple)):
                        params_list.append(param_set)
                    elif param_set is None:
                        params_list.append([])
                    else:
                        params_list.append([param_set])

            async with self._get_cursor(conn) as cursor:
                await cursor.executemany(sql, params_list)
                result: DMLResultDict = {
                    "rows_affected": cursor.rowcount if cursor.rowcount != -1 else len(params_list),
                    "status_message": "OK",
                }
                return result

    async def _execute_script(
        self,
        script: str,
        connection: "Optional[AsyncmyConnection]" = None,
        **kwargs: Any,
    ) -> ScriptResultDict:
        async with instrument_operation_async(self, "asyncmy_execute_script", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL script: %s", script)

            # AsyncMy may not support multi-statement scripts without CLIENT_MULTI_STATEMENTS flag
            # Use the shared implementation to split and execute statements individually
            statements = self._split_script_statements(script)
            statements_executed = 0

            async with self._get_cursor(conn) as cursor:
                for statement in statements:
                    if statement:
                        if self.instrumentation_config.log_queries:
                            logger.debug("Executing statement: %s", statement)
                        await cursor.execute(statement)
                        statements_executed += 1

            result: ScriptResultDict = {
                "statements_executed": statements_executed,
                "status_message": "SCRIPT EXECUTED",
            }
            return result

    async def _wrap_select_result(
        self,
        statement: SQL,
        result: SelectResultDict,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[SQLResult[ModelDTOT], SQLResult[RowT]]":
        async with instrument_operation_async(self, "asyncmy_wrap_select", "database"):
            data = result["data"]
            column_names = result["column_names"]
            rows_affected = result["rows_affected"]

            if not data:
                return SQLResult[RowT](
                    statement=statement,
                    data=[],
                    column_names=column_names,
                    rows_affected=0,
                    operation_type="SELECT",
                )

            rows_as_dicts = [dict(zip(column_names, row)) for row in data]

            if self.instrumentation_config.log_results_count:
                logger.debug("Asyncmy query returned %d rows", len(rows_as_dicts))

            if schema_type:
                converted_data_seq = self.to_schema(data=rows_as_dicts, schema_type=schema_type)
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
                data=rows_as_dicts,
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
        async with instrument_operation_async(self, "asyncmy_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            with wrap_exceptions(wrap_exceptions=False, suppress=AttributeError):
                if statement.expression:
                    operation_type = str(statement.expression.key).upper()

            # Handle script results
            if "statements_executed" in result:
                return SQLResult[RowT](
                    statement=statement,
                    data=cast("list[RowT]", []),
                    rows_affected=0,
                    operation_type="SCRIPT",
                    metadata={
                        "status_message": result.get("status_message", ""),
                        "statements_executed": result.get("statements_executed", -1),
                    },
                )

            # Handle DML results
            rows_affected = result.get("rows_affected", -1)
            status_message = result.get("status_message", "")

            if self.instrumentation_config.log_results_count:
                logger.debug("Asyncmy execute operation affected %d rows", rows_affected)

            return SQLResult[RowT](
                statement=statement,
                data=cast("list[RowT]", []),
                rows_affected=rows_affected,
                operation_type=operation_type,
                metadata={"status_message": status_message},
            )

    def _connection(self, connection: Optional[Connection] = None) -> Connection:  # pyright: ignore
        """Get the connection to use for the operation."""
        return connection or self.connection
