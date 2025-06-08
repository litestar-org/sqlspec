# ruff: noqa: PLR6301
import logging
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

import aiosqlite

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

__all__ = ("AiosqliteConnection", "AiosqliteDriver")

logger = logging.getLogger("sqlspec")

AiosqliteConnection = aiosqlite.Connection


class AiosqliteDriver(
    AsyncDriverAdapterProtocol[AiosqliteConnection, RowT], SQLTranslatorMixin, AsyncStorageMixin, ToSchemaMixin
):
    """Aiosqlite SQLite Driver Adapter. Modern protocol implementation."""

    dialect: "DialectType" = "sqlite"
    __supports_arrow__: ClassVar[bool] = True
    __supports_parquet__: ClassVar[bool] = False

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

    async def _execute_statement(
        self,
        statement: SQL,
        connection: Optional[AiosqliteConnection] = None,
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

        # Aiosqlite supports both qmark and named_colon styles
        # Use named_colon for dict params, qmark for positional
        if statement.parameters and isinstance(statement.parameters, dict):
            sql = statement.to_sql(placeholder_style=ParameterStyle.NAMED_COLON)
            params = statement.get_parameters(style=ParameterStyle.NAMED_COLON)
        else:
            sql = statement.to_sql(placeholder_style=self._get_placeholder_style())
            params = statement.get_parameters(style=self._get_placeholder_style())

        return await self._execute(
            sql,
            params,
            statement,
            connection=connection,
            **kwargs,
        )

    async def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: Optional[AiosqliteConnection] = None,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "aiosqlite_execute", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL: %s", sql)
            if self.instrumentation_config.log_parameters and parameters:
                logger.debug("Query parameters: %s", parameters)
            async with self._get_cursor(conn) as cursor:
                # Aiosqlite handles both dict and tuple parameters
                await cursor.execute(sql, parameters or ())
                if AsyncDriverAdapterProtocol.returns_rows(statement.expression):
                    return {"data": await cursor.fetchall(), "description": cursor.description}
                return cursor

    async def _execute_many(
        self,
        sql: str,
        param_list: Any,
        connection: Optional[AiosqliteConnection] = None,
        **kwargs: Any,
    ) -> int:
        async with instrument_operation_async(self, "aiosqlite_execute_many", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL (executemany): %s", sql)
            if self.instrumentation_config.log_parameters and param_list:
                logger.debug("Query parameters (batch): %s", param_list)

            # Convert parameter list to proper format for executemany
            params_list: list[tuple[Any, ...]] = []
            if param_list and isinstance(param_list, Sequence):
                for param_set in param_list:
                    if isinstance(param_set, (list, tuple)):
                        params_list.append(tuple(param_set))
                    elif param_set is None:
                        params_list.append(())
                    else:
                        params_list.append((param_set,))

            async with self._get_cursor(conn) as cursor:
                await cursor.executemany(sql, params_list)
                return len(params_list) if params_list else 0

    async def _execute_script(
        self,
        script: str,
        connection: Optional[AiosqliteConnection] = None,
        **kwargs: Any,
    ) -> str:
        async with instrument_operation_async(self, "aiosqlite_execute_script", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL script: %s", script)
            async with self._get_cursor(conn) as cursor:
                await cursor.executescript(script)
            return "SCRIPT EXECUTED"

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

            # result is an integer (rowcount) for execute_many operations
            if isinstance(result, int):
                rows_affected = result
                last_inserted_id = None
            else:
                # Assume cursor object
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

    def _connection(self, connection: Optional[AiosqliteConnection] = None) -> AiosqliteConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection
