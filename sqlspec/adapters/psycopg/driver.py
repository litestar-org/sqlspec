# ruff: noqa: PLR6301
import logging
from collections.abc import AsyncGenerator, Generator, Sequence
from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, Optional, Union, cast

from psycopg import AsyncConnection, Connection
from psycopg.rows import DictRow

from sqlspec.config import InstrumentationConfig
from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
from sqlspec.statement.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin, SyncArrowMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ExecuteResult, SelectResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import ModelDTOT, SQLParameterType
from sqlspec.utils.telemetry import instrument_operation, instrument_operation_async

logger = logging.getLogger("sqlspec")

__all__ = ("PsycopgAsyncConnection", "PsycopgAsyncDriver", "PsycopgSyncConnection", "PsycopgSyncDriver")

PsycopgSyncConnection = Connection[DictRow]
PsycopgAsyncConnection = AsyncConnection[DictRow]


class PsycopgSyncDriver(
    SyncDriverAdapterProtocol[PsycopgSyncConnection, DictRow],
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
    @contextmanager
    def _get_cursor(connection: PsycopgSyncConnection) -> Generator[Any, None, None]:
        with connection.cursor() as cursor:
            yield cursor

    def _execute_impl(
        self,
        statement: SQL,
        parameters: Optional[SQLParameterType] = None,
        connection: Optional[PsycopgSyncConnection] = None,
        config: Optional[SQLConfig] = None,
        is_many: bool = False,
        is_script: bool = False,
        **kwargs: Any,
    ) -> Any:
        with instrument_operation(self, "psycopg_execute", "database"):
            conn = self._connection(connection)
            final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())

            final_exec_params: Union[dict[str, Any], list[dict[str, Any]], None] = None

            if is_many:
                final_exec_params = []
                if parameters and isinstance(parameters, Sequence):
                    final_exec_params = [p for p in parameters if isinstance(p, dict)]
            else:
                single_params = statement.get_parameters(style=self._get_placeholder_style())
                final_exec_params = single_params if isinstance(single_params, dict) else {}

            with self._get_cursor(conn) as cursor:
                if self.instrumentation_config.log_queries:
                    logger.debug("Executing SQL: %s", final_sql)

                if self.instrumentation_config.log_parameters and final_exec_params:
                    logger.debug("Query parameters: %s", final_exec_params)

                if is_script:
                    script_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
                    cursor.execute(script_sql)
                    return cursor.statusmessage or "SCRIPT EXECUTED"
                if is_many:
                    cursor.executemany(final_sql, cast("list[dict[str, Any]]", final_exec_params))
                else:
                    cursor.execute(final_sql, cast("dict[str, Any]", final_exec_params))
                return cursor

    def _wrap_select_result(
        self,
        statement: SQL,
        raw_driver_result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SelectResult[ModelDTOT], SelectResult[dict[str, Any]]]:
        with instrument_operation(self, "psycopg_wrap_select", "database"):
            cursor = raw_driver_result
            fetched_data: list[DictRow] = cursor.fetchall()
            column_names = [col.name for col in cursor.description or []]
            rows_as_dicts: list[dict[str, Any]] = [dict(row) for row in fetched_data]

            if self.instrumentation_config.log_results_count:
                logger.debug("Query returned %d rows", len(rows_as_dicts))

            if schema_type:
                return SelectResult[ModelDTOT](
                    raw_result=cast("ModelDTOT", fetched_data),
                    rows=rows_as_dicts,
                    column_names=column_names,
                )
            return SelectResult[dict[str, Any]](
                raw_result=fetched_data[0] if fetched_data else {},
                rows=rows_as_dicts,
                column_names=column_names,
            )

    def _wrap_execute_result(
        self,
        statement: SQL,
        raw_driver_result: Any,
        **kwargs: Any,
    ) -> ExecuteResult[Any]:
        with instrument_operation(self, "psycopg_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()

            if isinstance(raw_driver_result, str):
                return ExecuteResult(
                    raw_result=raw_driver_result,
                    rows_affected=0,
                    operation_type=operation_type or "SCRIPT",
                )

            cursor = raw_driver_result
            rows_affected = getattr(cursor, "rowcount", -1)

            if self.instrumentation_config.log_results_count:
                logger.debug("Execute operation affected %d rows", rows_affected)

            return ExecuteResult(
                raw_result=None,
                rows_affected=rows_affected,
                operation_type=operation_type,
            )


class PsycopgAsyncDriver(
    AsyncDriverAdapterProtocol[PsycopgAsyncConnection, DictRow],
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

    async def _execute_impl(
        self,
        statement: SQL,
        parameters: Optional[SQLParameterType] = None,
        connection: Optional[PsycopgAsyncConnection] = None,
        config: Optional[SQLConfig] = None,
        is_many: bool = False,
        is_script: bool = False,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "psycopg_async_execute", "database"):
            conn = self._connection(connection)
            final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())

            final_exec_params: Union[dict[str, Any], list[dict[str, Any]], None] = None

            if is_many:
                final_exec_params = []
                if parameters and isinstance(parameters, Sequence):
                    final_exec_params = [p for p in parameters if isinstance(p, dict)]
            else:
                single_params = statement.get_parameters(style=self._get_placeholder_style())
                final_exec_params = single_params if isinstance(single_params, dict) else {}

            async with self._get_cursor(conn) as cursor:
                if self.instrumentation_config.log_queries:
                    logger.debug("Executing SQL: %s", final_sql)

                if self.instrumentation_config.log_parameters and final_exec_params:
                    logger.debug("Query parameters: %s", final_exec_params)

                if is_script:
                    script_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
                    await cursor.execute(script_sql)
                    return cursor.statusmessage or "SCRIPT EXECUTED"
                if is_many:
                    await cursor.executemany(final_sql, cast("list[dict[str, Any]]", final_exec_params))
                else:
                    await cursor.execute(final_sql, cast("dict[str, Any]", final_exec_params))
                return cursor

    async def _wrap_select_result(
        self,
        statement: SQL,
        raw_driver_result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SelectResult[ModelDTOT], SelectResult[dict[str, Any]]]:
        async with instrument_operation_async(self, "psycopg_async_wrap_select", "database"):
            cursor = raw_driver_result
            fetched_data: list[DictRow] = await cursor.fetchall()
            column_names = [col.name for col in cursor.description or []]
            rows_as_dicts: list[dict[str, Any]] = [dict(row) for row in fetched_data]

            if self.instrumentation_config.log_results_count:
                logger.debug("Query returned %d rows", len(rows_as_dicts))

            if schema_type:
                return SelectResult[ModelDTOT](
                    raw_result=cast("ModelDTOT", fetched_data),
                    rows=rows_as_dicts,
                    column_names=column_names,
                )
            return SelectResult[dict[str, Any]](
                raw_result=fetched_data[0] if fetched_data else {},
                rows=rows_as_dicts,
                column_names=column_names,
            )

    async def _wrap_execute_result(
        self,
        statement: SQL,
        raw_driver_result: Any,
        **kwargs: Any,
    ) -> ExecuteResult[Any]:
        async with instrument_operation_async(self, "psycopg_async_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()

            if isinstance(raw_driver_result, str):
                return ExecuteResult(
                    raw_result=raw_driver_result,
                    rows_affected=0,
                    operation_type=operation_type or "SCRIPT",
                )

            cursor = raw_driver_result
            rows_affected = getattr(cursor, "rowcount", -1)

            if self.instrumentation_config.log_results_count:
                logger.debug("Execute operation affected %d rows", rows_affected)

            return ExecuteResult(
                raw_result=None,
                rows_affected=rows_affected,
                operation_type=operation_type,
            )
