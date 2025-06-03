# ruff: noqa: PLR6301
import logging
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, Optional, Union, cast

from psycopg import AsyncConnection, Connection
from psycopg.rows import DictRow as PsycopgDictRow

from sqlspec.config import InstrumentationConfig
from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
from sqlspec.statement.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin, SyncArrowMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, RowT
from sqlspec.utils.telemetry import instrument_operation, instrument_operation_async

logger = logging.getLogger("sqlspec")

__all__ = ("PsycopgAsyncConnection", "PsycopgAsyncDriver", "PsycopgSyncConnection", "PsycopgSyncDriver")

PsycopgSyncConnection = Connection[PsycopgDictRow]
PsycopgAsyncConnection = AsyncConnection[PsycopgDictRow]


class PsycopgSyncDriver(
    SyncDriverAdapterProtocol[PsycopgSyncConnection, RowT],
    SQLTranslatorMixin[PsycopgSyncConnection],
    SyncArrowMixin[PsycopgSyncConnection],
    ResultConverter,
):
    """Psycopg Sync Driver Adapter. Refactored for new protocol."""

    dialect: str = "postgres"
    __supports_arrow__: ClassVar[bool] = False

    def __init__(
        self,
        connection: PsycopgSyncConnection,
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
        return ParameterStyle.PYFORMAT_NAMED

    @staticmethod
    @contextmanager
    def _get_cursor(connection: PsycopgSyncConnection) -> Generator[Any, None, None]:
        with connection.cursor() as cursor:
            yield cursor

    def _execute_statement(
        self,
        statement: SQL,
        connection: Optional[PsycopgSyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        if statement.is_script:
            return self._execute_script(
                statement.to_sql(placeholder_style=ParameterStyle.STATIC),
                connection=connection,
                **kwargs,
            )
        if statement.is_many:
            return self._execute_many(
                statement.to_sql(placeholder_style=self._get_placeholder_style()),
                statement.parameters,
                connection=connection,
                **kwargs,
            )
        return self._execute(
            statement.to_sql(placeholder_style=self._get_placeholder_style()),
            statement.parameters,
            statement,
            connection=connection,
            **kwargs,
        )

    def _execute(
        self,
        sql: str,
        params: Any,
        statement: SQL,
        connection: Optional[PsycopgSyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        with instrument_operation(self, "psycopg_execute", "database"):
            conn = self._connection(connection)
            final_driver_params = params if params is not None and isinstance(params, dict) else {}
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL: %s", sql)
            if self.instrumentation_config.log_parameters and final_driver_params:
                logger.debug("Query parameters: %s", final_driver_params)
            with self._get_cursor(conn) as cursor:
                cursor.execute(sql, cast("Optional[dict[str, Any]]", final_driver_params))
                return cursor

    def _execute_many(
        self,
        sql: str,
        param_list: Any,
        connection: Optional[PsycopgSyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        with instrument_operation(self, "psycopg_execute_many", "database"):
            conn = self._connection(connection)
            params_list = param_list if isinstance(param_list, list) else []
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL (executemany): %s", sql)
            if self.instrumentation_config.log_parameters and params_list:
                logger.debug("Query parameters (batch): %s", params_list)
            with self._get_cursor(conn) as cursor:
                cursor.executemany(sql, cast("list[dict[str, Any]]", params_list))
                return cursor

    def _execute_script(
        self,
        script: str,
        connection: Optional[PsycopgSyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        with instrument_operation(self, "psycopg_execute_script", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL script: %s", script)
            with self._get_cursor(conn) as cursor:
                cursor.execute(script_sql=script)
                return cursor.statusmessage or "SCRIPT EXECUTED"

    def _wrap_select_result(  # pyright: ignore
        self,
        statement: SQL,
        result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        with instrument_operation(self, "psycopg_wrap_select", "database"):
            cursor = result
            fetched_data: list[PsycopgDictRow] = cursor.fetchall()
            column_names = [col.name for col in cursor.description or []]
            rows_as_dicts: list[dict[str, Any]] = [dict(row) for row in fetched_data]

            if self.instrumentation_config.log_results_count:
                logger.debug("Query returned %d rows", len(rows_as_dicts))

            if schema_type:
                converted_data_seq = self.to_schema(data=fetched_data, schema_type=schema_type)
                # Ensure data is a list for SQLResult
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

    def _wrap_execute_result(  # pyright: ignore
        self,
        statement: SQL,
        result: Any,
        **kwargs: Any,
    ) -> SQLResult[RowT]:
        with instrument_operation(self, "psycopg_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()

            if isinstance(result, str):
                return SQLResult[RowT](
                    statement=statement,
                    data=[],
                    rows_affected=0,
                    operation_type=operation_type or "SCRIPT",
                    metadata={"status_message": result},
                )

            cursor = result
            rows_affected = getattr(cursor, "rowcount", -1)

            returned_data: list[dict[str, Any]] = []
            if hasattr(cursor, "description") and cursor.description:
                try:
                    fetched_returning_data = cursor.fetchall()
                    if fetched_returning_data:
                        returned_data = [dict(row) for row in fetched_returning_data]
                        if not rows_affected or rows_affected == -1:
                            rows_affected = len(returned_data)
                except Exception as e:  # pragma: no cover
                    logger.debug("Could not fetch RETURNING data in _wrap_execute_result: %s", e)

            if self.instrumentation_config.log_results_count:
                logger.debug("Execute operation affected %d rows", rows_affected)
                if returned_data:
                    logger.debug("RETURNING clause returned %d rows", len(returned_data))

            return SQLResult[RowT](
                statement=statement,
                data=returned_data,
                rows_affected=rows_affected,
                operation_type=operation_type,
                column_names=[col.name for col in cursor.description or []] if returned_data else [],
            )

    def _connection(self, connection: Optional[PsycopgSyncConnection] = None) -> PsycopgSyncConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection

    def _select_to_arrow_impl(
        self, stmt_obj: "SQL", connection: "PsycopgSyncConnection", **kwargs: "Any"
    ) -> "ArrowResult":
        msg = "Arrow export is not implemented for Psycopg sync driver."
        raise NotImplementedError(msg)


class PsycopgAsyncDriver(
    AsyncDriverAdapterProtocol[PsycopgAsyncConnection, RowT],
    SQLTranslatorMixin[PsycopgAsyncConnection],
    AsyncArrowMixin[PsycopgAsyncConnection],
    ResultConverter,
):
    """Psycopg Async Driver Adapter. Refactored for new protocol."""

    dialect: str = "postgres"
    __supports_arrow__: ClassVar[bool] = False

    def __init__(
        self,
        connection: PsycopgAsyncConnection,
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
        return ParameterStyle.PYFORMAT_NAMED

    @staticmethod
    @asynccontextmanager
    async def _get_cursor(connection: PsycopgAsyncConnection) -> AsyncGenerator[Any, None]:
        async with connection.cursor() as cursor:
            yield cursor

    async def _execute_statement(
        self,
        statement: SQL,
        connection: Optional[PsycopgAsyncConnection] = None,
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
            statement.parameters,
            statement,
            connection=connection,
            **kwargs,
        )

    async def _execute(
        self,
        sql: str,
        params: Any,
        statement: SQL,
        connection: Optional[PsycopgAsyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "psycopg_async_execute", "database"):
            conn = self._connection(connection)
            final_driver_params = params if params is not None and isinstance(params, dict) else {}
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL: %s", sql)
            if self.instrumentation_config.log_parameters and final_driver_params:
                logger.debug("Query parameters: %s", final_driver_params)
            async with self._get_cursor(conn) as cursor:
                await cursor.execute(sql, final_driver_params)
                return cursor

    async def _execute_many(
        self,
        sql: str,
        param_list: Any,
        connection: Optional[PsycopgAsyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "psycopg_async_execute_many", "database"):
            conn = self._connection(connection)
            params_list = param_list if isinstance(param_list, list) else []
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL (executemany): %s", sql)
            if self.instrumentation_config.log_parameters and params_list:
                logger.debug("Query parameters (batch): %s", params_list)
            async with self._get_cursor(conn) as cursor:
                await cursor.executemany(sql, params_list)
                return cursor

    async def _execute_script(
        self,
        script: str,
        connection: Optional[PsycopgAsyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "psycopg_async_execute_script", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL script: %s", script)
            async with self._get_cursor(conn) as cursor:
                await cursor.execute(script_sql=script)
                return cursor.statusmessage if hasattr(cursor, "statusmessage") else "SCRIPT EXECUTED"

    async def _wrap_select_result(  # pyright: ignore
        self,
        statement: SQL,
        result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        with instrument_operation(self, "psycopg_wrap_select", "database"):
            cursor = result
            fetched_data: list[PsycopgDictRow] = await cursor.fetchall()
            column_names = [col.name for col in cursor.description or []]
            rows_as_dicts: list[dict[str, Any]] = [dict(row) for row in fetched_data]

            if self.instrumentation_config.log_results_count:
                logger.debug("Query returned %d rows", len(rows_as_dicts))

            if schema_type:
                converted_data_seq = self.to_schema(data=fetched_data, schema_type=schema_type)
                # Ensure data is a list for SQLResult
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

    async def _wrap_execute_result(  # pyright: ignore
        self,
        statement: SQL,
        result: Any,
        **kwargs: Any,
    ) -> SQLResult[RowT]:
        with instrument_operation(self, "psycopg_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()

            if isinstance(result, str):
                return SQLResult[RowT](
                    statement=statement,
                    data=[],
                    rows_affected=0,
                    operation_type=operation_type or "SCRIPT",
                    metadata={"status_message": result},
                )

            cursor = result
            rows_affected = getattr(cursor, "rowcount", -1)

            returned_data: list[dict[str, Any]] = []
            if hasattr(cursor, "description") and cursor.description:
                try:
                    fetched_returning_data = await cursor.fetchall()
                    if fetched_returning_data:
                        returned_data = [dict(row) for row in fetched_returning_data]
                        if not rows_affected or rows_affected == -1:
                            rows_affected = len(returned_data)
                except Exception as e:  # pragma: no cover
                    logger.debug("Could not fetch RETURNING data in async _wrap_execute_result: %s", e)

            if self.instrumentation_config.log_results_count:
                logger.debug("Execute operation affected %d rows", rows_affected)
                if returned_data:
                    logger.debug("RETURNING clause returned %d rows", len(returned_data))

            return SQLResult[RowT](
                statement=statement,
                data=returned_data,
                rows_affected=rows_affected,
                operation_type=operation_type,
                column_names=[col.name for col in cursor.description or []] if returned_data else [],
            )

    def _connection(self, connection: Optional[PsycopgAsyncConnection] = None) -> PsycopgAsyncConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection

    async def _select_to_arrow_impl(
        self, stmt_obj: "SQL", connection: "PsycopgAsyncConnection", **kwargs: "Any"
    ) -> "ArrowResult":
        msg = "Arrow export is not implemented for Psycopg async driver."
        raise NotImplementedError(msg)
