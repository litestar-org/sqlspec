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
        conn = self._connection(connection)
        final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())

        final_exec_params: Union[dict[str, Any], list[dict[str, Any]], None] = None

        if is_many:
            many_params_list: list[dict[str, Any]] = []
            if parameters is not None and isinstance(parameters, Sequence):
                for param_set in parameters:
                    if isinstance(param_set, dict):
                        many_params_list.append(param_set)
                    else:
                        logger.warning(
                            "executemany with PYFORMAT_NAMED expects dict, got %s. Skipping.", type(param_set)
                        )
            final_exec_params = many_params_list
        else:
            single_params = statement.get_parameters(style=self._get_placeholder_style())
            if single_params is None:
                final_exec_params = {}
            elif isinstance(single_params, dict):
                final_exec_params = single_params
            elif isinstance(single_params, (list, tuple)):
                logger.warning("PYFORMAT_NAMED style resolved to sequence, expected dict. Adapting.")
                final_exec_params = {f"param_{i}": v for i, v in enumerate(single_params)}
            else:
                logger.warning("PYFORMAT_NAMED style resolved to scalar, expected dict. Adapting.")
                final_exec_params = {"param_0": single_params}

        with self._get_cursor(conn) as cursor:
            logger.debug("Executing SQL (Psycopg Sync): %s with params: %s", final_sql, final_exec_params)
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
        cursor = raw_driver_result
        fetched_data: list[DictRow] = cursor.fetchall()
        column_names = [col.name for col in cursor.description or []]
        rows_as_dicts: list[dict[str, Any]] = [dict(row) for row in fetched_data]

        if schema_type:
            converted_rows = self.to_schema(rows_as_dicts, schema_type=schema_type)
            return SelectResult(
                rows=converted_rows,
                column_names=column_names,
                raw_result=fetched_data,
                statement=statement,
            )
        return SelectResult(
            rows=rows_as_dicts,
            column_names=column_names,
            raw_result=fetched_data,
            statement=statement,
        )

    def _wrap_execute_result(
        self,
        statement: SQL,
        raw_driver_result: Any,
        **kwargs: Any,
    ) -> ExecuteResult[Any]:
        operation_type = "UNKNOWN"
        if statement.expression and hasattr(statement.expression, "key"):
            operation_type = str(statement.expression.key).upper()

        rows_affected = -1

        if isinstance(raw_driver_result, str):
            return ExecuteResult(
                raw_result=raw_driver_result,
                rows_affected=0,
                operation_type=operation_type or "SCRIPT",
                statement=statement,
            )

        cursor = raw_driver_result
        if cursor and hasattr(cursor, "rowcount"):
            rows_affected = cursor.rowcount

        return ExecuteResult(
            raw_result=None,
            rows_affected=rows_affected,
            operation_type=operation_type,
            statement=statement,
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
        conn = self._connection(connection)
        final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())

        final_exec_params: Union[dict[str, Any], list[dict[str, Any]], None] = None

        if is_many:
            many_params_list: list[dict[str, Any]] = []
            if parameters is not None and isinstance(parameters, Sequence):
                for param_set in parameters:
                    if isinstance(param_set, dict):
                        many_params_list.append(param_set)
                    else:
                        logger.warning(
                            "executemany with PYFORMAT_NAMED expects dict, got %s. Skipping.", type(param_set)
                        )
            final_exec_params = many_params_list
        else:
            single_params = statement.get_parameters(style=self._get_placeholder_style())
            if single_params is None:
                final_exec_params = {}
            elif isinstance(single_params, dict):
                final_exec_params = single_params
            elif isinstance(single_params, (list, tuple)):
                logger.warning("PYFORMAT_NAMED style resolved to sequence, expected dict. Adapting.")
                final_exec_params = {f"param_{i}": v for i, v in enumerate(single_params)}
            else:
                logger.warning("PYFORMAT_NAMED style resolved to scalar, expected dict. Adapting.")
                final_exec_params = {"param_0": single_params}

        async with self._get_cursor(conn) as cursor:
            logger.debug("Executing SQL (Psycopg Async): %s with params: %s", final_sql, final_exec_params)
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
        cursor = raw_driver_result
        fetched_data: list[DictRow] = await cursor.fetchall()
        column_names = [col.name for col in cursor.description or []]
        rows_as_dicts: list[dict[str, Any]] = [dict(row) for row in fetched_data]

        if schema_type:
            converted_rows = self.to_schema(rows_as_dicts, schema_type=schema_type)
            return SelectResult(
                rows=converted_rows,
                column_names=column_names,
                raw_result=fetched_data,
                statement=statement,
            )
        return SelectResult(
            rows=rows_as_dicts,
            column_names=column_names,
            raw_result=fetched_data,
            statement=statement,
        )

    async def _wrap_execute_result(
        self,
        statement: SQL,
        raw_driver_result: Any,
        **kwargs: Any,
    ) -> ExecuteResult[Any]:
        operation_type = "UNKNOWN"
        if statement.expression and hasattr(statement.expression, "key"):
            operation_type = str(statement.expression.key).upper()

        rows_affected = -1

        if isinstance(raw_driver_result, str):
            return ExecuteResult(
                raw_result=raw_driver_result,
                rows_affected=0,
                operation_type=operation_type or "SCRIPT",
                statement=statement,
            )

        cursor = raw_driver_result
        if cursor and hasattr(cursor, "rowcount"):
            rows_affected = cursor.rowcount

        return ExecuteResult(
            raw_result=None,
            rows_affected=rows_affected,
            operation_type=operation_type,
            statement=statement,
        )
