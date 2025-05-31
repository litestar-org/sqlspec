# ruff: noqa: PLR6301
import logging
from collections.abc import AsyncGenerator, Generator, Sequence
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

import pyarrow as pa
from oracledb import AsyncConnection, AsyncCursor, Connection, Cursor

from sqlspec.config import InstrumentationConfig
from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
from sqlspec.statement.mixins import (
    AsyncArrowMixin,
    ResultConverter,
    SQLTranslatorMixin,
    SyncArrowMixin,
)
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, ExecuteResult, SelectResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, SQLParameterType
from sqlspec.utils.sync_tools import ensure_async_
from sqlspec.utils.telemetry import instrument_operation, instrument_operation_async

if TYPE_CHECKING:
    from sqlspec.statement.filters import StatementFilter

__all__ = ("OracleAsyncConnection", "OracleAsyncDriver", "OracleSyncConnection", "OracleSyncDriver")

OracleSyncConnection = Connection
OracleAsyncConnection = AsyncConnection

logger = logging.getLogger("sqlspec")


class OracleSyncDriver(
    SyncDriverAdapterProtocol[OracleSyncConnection, DictRow],
    SyncArrowMixin[OracleSyncConnection],
    SQLTranslatorMixin[OracleSyncConnection],
    ResultConverter,
):
    """Oracle Sync Driver Adapter. Refactored for new protocol."""

    dialect: str = "oracle"
    __supports_arrow__: ClassVar[bool] = True

    def __init__(
        self,
        connection: OracleSyncConnection,
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
        return ParameterStyle.NAMED_COLON

    @contextmanager
    def _get_cursor(self, connection: Optional[OracleSyncConnection] = None) -> Generator[Cursor, None, None]:
        conn_to_use = connection or self.connection
        cursor: Cursor = conn_to_use.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def _execute_impl(
        self,
        statement: SQL,
        parameters: Optional[SQLParameterType] = None,
        connection: Optional[OracleSyncConnection] = None,
        config: Optional[SQLConfig] = None,
        is_many: bool = False,
        is_script: bool = False,
        **kwargs: Any,
    ) -> Any:
        with instrument_operation(self, "oracle_execute", "database"):
            conn = self._connection(connection)
            final_sql: str
            final_driver_params: Union[dict[str, Any], list[dict[str, Any]], None] = None

            if is_script:
                final_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
            else:
                final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())
                if is_many:
                    batch_params_list: list[dict[str, Any]] = []
                    if parameters is not None and isinstance(parameters, Sequence):
                        for param_set_item in parameters:
                            if isinstance(param_set_item, dict):
                                batch_params_list.append(param_set_item)
                            elif isinstance(param_set_item, (list, tuple)):
                                batch_params_list.append({f"p{i}": v for i, v in enumerate(param_set_item)})
                            elif param_set_item is None:
                                batch_params_list.append({})
                            else:
                                batch_params_list.append({"p0": param_set_item})
                    final_driver_params = batch_params_list
                else:
                    single_params = statement.get_parameters(style=self._get_placeholder_style())
                    if single_params is None:
                        final_driver_params = None
                    elif isinstance(single_params, dict):
                        final_driver_params = single_params
                    elif isinstance(single_params, (list, tuple)):
                        final_driver_params = {f"p{i}": v for i, v in enumerate(single_params)}
                    else:
                        final_driver_params = {"p0": single_params}

            with self._get_cursor(conn) as cursor:
                if self.instrumentation_config.log_queries:
                    logger.debug("Executing SQL: %s", final_sql)

                if self.instrumentation_config.log_parameters and final_driver_params:
                    logger.debug("Query parameters: %s", final_driver_params)

                if is_script:
                    cursor.execute(final_sql)
                    return "SCRIPT EXECUTED"
                if is_many:
                    cursor.executemany(
                        final_sql,
                        cast("list[dict[str, Any]]", final_driver_params) if final_driver_params is not None else [],
                    )
                else:
                    cursor.execute(final_sql, final_driver_params or {})
                return cursor

    def _wrap_select_result(
        self,
        statement: SQL,
        raw_driver_result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SelectResult[ModelDTOT], SelectResult[dict[str, Any]]]:
        with instrument_operation(self, "oracle_wrap_select", "database"):
            cursor = cast("Cursor", raw_driver_result)
            if not cursor.description:
                return SelectResult[dict[str, Any]](statement=statement, data=[], column_names=[])
            column_names = [col[0] for col in cursor.description]
            fetched_tuples = cursor.fetchall()
            rows_as_dicts: list[dict[str, Any]] = [dict(zip(column_names, row_tuple)) for row_tuple in fetched_tuples]

            if self.instrumentation_config.log_results_count:
                logger.debug("Query returned %d rows", len(rows_as_dicts))

            if schema_type:
                converted_data = self.to_schema(rows_as_dicts, schema_type=schema_type)
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

    def _wrap_execute_result(
        self,
        statement: SQL,
        raw_driver_result: Any,
        **kwargs: Any,
    ) -> ExecuteResult:
        with instrument_operation(self, "oracle_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()
            rows_affected = -1
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
            cursor = cast("Cursor", raw_driver_result)
            if cursor and hasattr(cursor, "rowcount"):
                rc = cursor.rowcount
                if isinstance(rc, list):
                    rows_affected = sum(rc)
                elif isinstance(rc, int):
                    rows_affected = rc

            if self.instrumentation_config.log_results_count:
                logger.debug("Execute operation affected %d rows", rows_affected)

            execute_data = {
                "rows_affected": rows_affected,
                "last_inserted_id": None,
                "inserted_ids": [],
                "returning_data": None,
                "operation_type": operation_type,
            }
            return ExecuteResult(
                statement=statement, data=execute_data, rows_affected=rows_affected, operation_type=operation_type
            )

    def select_to_arrow(
        self,
        statement: "Union[str, SQL, Any]",
        parameters: Optional[SQLParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[OracleSyncConnection] = None,
        config: Optional[SQLConfig] = None,
        **kwargs: Any,
    ) -> "ArrowResult":
        with instrument_operation(self, "oracle_select_to_arrow", "database"):
            conn = self._connection(connection)
            current_config = config or self.config
            stmt_obj = SQL(
                statement, parameters, *(filters or ()), dialect=self.dialect, config=current_config, **kwargs
            )
            stmt_obj.validate()
            if not SyncDriverAdapterProtocol.returns_rows(stmt_obj.expression):
                op_type = (
                    str(stmt_obj.expression.key).upper()
                    if stmt_obj.expression and hasattr(stmt_obj.expression, "key")
                    else "UNKNOWN"
                )
                msg = f"Cannot fetch Arrow table for a non-query statement. Command type: {op_type}"
                raise TypeError(msg)

            final_sql = stmt_obj.to_sql(placeholder_style=self._get_placeholder_style())
            oracle_params = stmt_obj.get_parameters(style=self._get_placeholder_style())
            oracle_params_dict: dict[str, Any]
            if oracle_params is None:
                oracle_params_dict = {}
            elif isinstance(oracle_params, dict):
                oracle_params_dict = oracle_params
            elif isinstance(oracle_params, (list, tuple)):
                oracle_params_dict = {f"p{i}": v for i, v in enumerate(oracle_params)}
            else:
                oracle_params_dict = {"p0": oracle_params}

            # Use Oracle cursor for Arrow table generation
            with self._get_cursor(conn) as cursor:
                cursor.execute(final_sql, oracle_params_dict)
                rows = cursor.fetchall()
                if not rows:
                    return ArrowResult(statement=stmt_obj, data=pa.Table.from_arrays([], names=[]))
                column_names = [col[0] for col in cursor.description or []]
                list_of_cols = list(zip(*rows)) if rows else [[] for _ in column_names]
                arrow_table = pa.Table.from_arrays(list_of_cols, names=column_names)
                return ArrowResult(statement=stmt_obj, data=arrow_table)


class OracleAsyncDriver(
    AsyncDriverAdapterProtocol[OracleAsyncConnection, DictRow],
    AsyncArrowMixin[OracleAsyncConnection],
    SQLTranslatorMixin[OracleAsyncConnection],
    ResultConverter,
):
    """Oracle Async Driver Adapter. Refactored for new protocol."""

    dialect: str = "oracle"
    __supports_arrow__: ClassVar[bool] = True

    def __init__(
        self,
        connection: OracleAsyncConnection,
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
        return ParameterStyle.NAMED_COLON

    @asynccontextmanager
    async def _get_cursor(
        self, connection: Optional[OracleAsyncConnection] = None
    ) -> AsyncGenerator[AsyncCursor, None]:
        conn_to_use = connection or self.connection
        cursor: AsyncCursor = conn_to_use.cursor()
        try:
            yield cursor
        finally:
            await ensure_async_(cursor.close)()

    async def _execute_impl(
        self,
        statement: SQL,
        parameters: Optional[SQLParameterType] = None,
        connection: Optional[OracleAsyncConnection] = None,
        config: Optional[SQLConfig] = None,
        is_many: bool = False,
        is_script: bool = False,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "oracle_async_execute", "database"):
            conn = self._connection(connection)
            final_sql: str
            final_driver_params: Union[dict[str, Any], list[dict[str, Any]], None] = None

            if is_script:
                final_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
            else:
                final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())
                if is_many:
                    batch_params_list: list[dict[str, Any]] = []
                    if parameters is not None and isinstance(parameters, Sequence):
                        for param_set_item in parameters:
                            if isinstance(param_set_item, dict):
                                batch_params_list.append(param_set_item)
                            elif isinstance(param_set_item, (list, tuple)):
                                batch_params_list.append({f"p{i}": v for i, v in enumerate(param_set_item)})
                            elif param_set_item is None:
                                batch_params_list.append({})
                            else:
                                batch_params_list.append({"p0": param_set_item})
                    final_driver_params = batch_params_list
                else:
                    single_params = statement.get_parameters(style=self._get_placeholder_style())
                    if single_params is None:
                        final_driver_params = None
                    elif isinstance(single_params, dict):
                        final_driver_params = single_params
                    elif isinstance(single_params, (list, tuple)):
                        final_driver_params = {f"p{i}": v for i, v in enumerate(single_params)}
                    else:
                        final_driver_params = {"p0": single_params}

            async with self._get_cursor(conn) as cursor:
                if self.instrumentation_config.log_queries:
                    logger.debug("Executing SQL: %s", final_sql)

                if self.instrumentation_config.log_parameters and final_driver_params:
                    logger.debug("Query parameters: %s", final_driver_params)

                if is_script:
                    await cursor.execute(final_sql)
                    return "SCRIPT EXECUTED"
                if is_many:
                    await cursor.executemany(final_sql, cast("list[dict[str, Any]]", final_driver_params) or [])
                else:
                    await cursor.execute(final_sql, final_driver_params or {})
                return cursor

    async def _wrap_select_result(
        self,
        statement: SQL,
        raw_driver_result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SelectResult[ModelDTOT], SelectResult[dict[str, Any]]]:
        async with instrument_operation_async(self, "oracle_async_wrap_select", "database"):
            cursor = cast("AsyncCursor", raw_driver_result)
            if not cursor.description:
                return SelectResult[dict[str, Any]](statement=statement, data=[], column_names=[])
            column_names = [col[0] for col in cursor.description]
            fetched_tuples = await cursor.fetchall()
            rows_as_dicts: list[dict[str, Any]] = [dict(zip(column_names, row_tuple)) for row_tuple in fetched_tuples]

            if self.instrumentation_config.log_results_count:
                logger.debug("Query returned %d rows", len(rows_as_dicts))

            if schema_type:
                converted_data = self.to_schema(rows_as_dicts, schema_type=schema_type)
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
        async with instrument_operation_async(self, "oracle_async_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()
            rows_affected = -1
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
            cursor = cast("AsyncCursor", raw_driver_result)
            if cursor and hasattr(cursor, "rowcount"):
                rc = cursor.rowcount
                if isinstance(rc, list):
                    rows_affected = sum(rc)
                elif isinstance(rc, int):
                    rows_affected = rc

            if self.instrumentation_config.log_results_count:
                logger.debug("Execute operation affected %d rows", rows_affected)

            execute_data = {
                "rows_affected": rows_affected,
                "last_inserted_id": None,
                "inserted_ids": [],
                "returning_data": None,
                "operation_type": operation_type,
            }
            return ExecuteResult(
                statement=statement, data=execute_data, rows_affected=rows_affected, operation_type=operation_type
            )

    async def select_to_arrow(
        self,
        statement: "Union[str, SQL, Any]",
        parameters: Optional[SQLParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[OracleAsyncConnection] = None,
        config: Optional[SQLConfig] = None,
        **kwargs: Any,
    ) -> "ArrowResult":
        async with instrument_operation_async(self, "oracle_async_select_to_arrow", "database"):
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
            oracle_params = stmt_obj.get_parameters(style=self._get_placeholder_style())
            oracle_params_dict: dict[str, Any]
            if oracle_params is None:
                oracle_params_dict = {}
            elif isinstance(oracle_params, dict):
                oracle_params_dict = oracle_params
            elif isinstance(oracle_params, (list, tuple)):
                oracle_params_dict = {f"p{i}": v for i, v in enumerate(oracle_params)}
            else:
                oracle_params_dict = {"p0": oracle_params}

            # Use Oracle cursor for Arrow table generation
            async with self._get_cursor(conn) as cursor:
                await cursor.execute(final_sql, oracle_params_dict)
                rows = await cursor.fetchall()
                if not rows:
                    return ArrowResult(statement=stmt_obj, data=pa.Table.from_arrays([], names=[]))
                column_names = [col[0] for col in cursor.description or []]
                list_of_cols = list(zip(*rows)) if rows else [[] for _ in column_names]
                arrow_table = pa.Table.from_arrays(list_of_cols, names=column_names)
                return ArrowResult(statement=stmt_obj, data=arrow_table)
