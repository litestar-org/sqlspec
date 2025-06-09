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
from sqlspec.statement.result import SQLResult
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

    def _get_placeholder_style(self) -> "ParameterStyle":
        return ParameterStyle.PYFORMAT_POSITIONAL

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
    ) -> Any:
        if statement.is_script:
            return await self._execute_script(
                statement.to_sql(placeholder_style=ParameterStyle.STATIC),
                connection=connection,
                **kwargs,
            )
        if statement.is_many:
            return await self._execute_many(
                statement.to_sql(placeholder_style=self._get_placeholder_style()),
                statement.parameters,
                connection=connection,
                **kwargs,
            )

        return await self._execute(
            statement.to_sql(placeholder_style=self._get_placeholder_style()),
            statement.parameters,  # Use raw merged parameters
            statement,
            connection=connection,
            **kwargs,
        )

    async def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: "Optional[AsyncmyConnection]" = None,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "asyncmy_execute", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL: %s", sql)
            # Convert parameters to the format AsyncMy expects
            # TODO: improve this.  why can't use just use parameter parsing?
            asyncmy_params: Any = None
            if parameters is not None:
                if isinstance(parameters, dict):
                    if not parameters:  # Empty dict
                        asyncmy_params = None
                    elif all(key.startswith("param_") and key[6:].isdigit() for key in parameters):
                        # Convert to positional list based on param indices
                        param_list = []
                        for i in range(len(parameters)):
                            param_key = f"param_{i}"
                            if param_key in parameters:
                                param_list.append(parameters[param_key])
                        asyncmy_params = param_list
                    else:
                        asyncmy_params = parameters
                elif isinstance(parameters, (list, tuple)):
                    asyncmy_params = parameters or None
                else:
                    asyncmy_params = [parameters]

            if self.instrumentation_config.log_parameters and asyncmy_params:
                logger.debug("Query parameters: %s", asyncmy_params)

            async with self._get_cursor(conn) as cursor:
                # AsyncMy expects list/tuple parameters or dict for named params
                await cursor.execute(sql, asyncmy_params)

                # For SELECT queries, return cursor so _wrap_select_result can fetch from it
                is_select = self.returns_rows(statement.expression)
                # If expression is None (parsing disabled or failed), check SQL string
                if not is_select and statement.expression is None:
                    sql_upper = sql.strip().upper()
                    is_select = any(sql_upper.startswith(prefix) for prefix in ["SELECT", "WITH", "VALUES", "TABLE"])

                if is_select:
                    return cursor
                # For DML/DDL queries, extract cursor info while it's still valid
                return {
                    "rowcount": cursor.rowcount,
                    "lastrowid": cursor.lastrowid,
                    "description": cursor.description,
                }

    async def _execute_many(
        self,
        sql: str,
        param_list: Any,
        connection: "Optional[AsyncmyConnection]" = None,
        **kwargs: Any,
    ) -> int:
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
                return {
                    "rowcount": cursor.rowcount if cursor.rowcount != -1 else len(params_list),
                    "lastrowid": cursor.lastrowid,
                    "description": cursor.description,
                }

    async def _execute_script(
        self,
        script: str,
        connection: "Optional[AsyncmyConnection]" = None,
        **kwargs: Any,
    ) -> str:
        async with instrument_operation_async(self, "asyncmy_execute_script", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL script: %s", script)
            async with self._get_cursor(conn) as cursor:
                await cursor.execute(script)
            return "SCRIPT EXECUTED"

    async def _wrap_select_result(
        self,
        statement: SQL,
        result: Any,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[SQLResult[ModelDTOT], SQLResult[RowT]]":
        async with instrument_operation_async(self, "asyncmy_wrap_select", "database"):
            cursor = cast("Cursor", result)

            try:
                results: list[tuple[Any, ...]] = await cursor.fetchall()
            except Exception:
                results = []

            column_names = [desc[0] for desc in cursor.description or []]

            if not results:
                return SQLResult[RowT](
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
        async with instrument_operation_async(self, "asyncmy_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            with wrap_exceptions(wrap_exceptions=False, suppress=AttributeError):
                if statement.expression:
                    operation_type = str(statement.expression.key).upper()

            if isinstance(result, str) and result == "SCRIPT EXECUTED":
                return SQLResult[RowT](
                    statement=statement,
                    data=[],
                    rows_affected=0,
                    operation_type=operation_type or "SCRIPT",
                    metadata={"status_message": result},
                )

            # Handle dict result from _execute (DML/DDL operations)
            # TODO: improve this.  why can't use just use parameter parsing?
            if isinstance(result, dict) and "rowcount" in result:
                rows_affected = result["rowcount"]
                last_inserted_id = result.get("lastrowid")

                if self.instrumentation_config.log_results_count:
                    logger.debug("Asyncmy execute operation affected %d rows", rows_affected)
                    if last_inserted_id is not None:
                        logger.debug("Asyncmy last inserted ID: %s", last_inserted_id)

                return SQLResult[RowT](
                    statement=statement,
                    data=[],
                    rows_affected=rows_affected,
                    operation_type=operation_type,
                    inserted_ids=[last_inserted_id] if last_inserted_id is not None else [],
                )

            # Handle cursor object (legacy path for backward compatibility)
            cursor = cast("Cursor", result)
            rows_affected = getattr(cursor, "rowcount", -1)
            last_inserted_id = getattr(cursor, "lastrowid", None)

            returned_data: list[dict[str, Any]] = []

            if self.instrumentation_config.log_results_count:
                logger.debug("Asyncmy execute operation affected %d rows", rows_affected)
                if last_inserted_id is not None:
                    logger.debug("Asyncmy last inserted ID: %s", last_inserted_id)

            return SQLResult[RowT](
                statement=statement,
                data=returned_data,
                rows_affected=rows_affected,
                last_inserted_id=last_inserted_id,
                operation_type=operation_type,
            )

    def _connection(self, connection: Optional[Connection] = None) -> Connection:  # pyright: ignore
        """Get the connection to use for the operation."""
        return connection or self.connection
