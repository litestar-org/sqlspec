# ruff: noqa: PLR6301
import logging
from collections.abc import AsyncGenerator, Generator, Sequence
from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, Optional, Union, cast

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
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import ArrowTable, DictRow, ModelDTOT, RowT
from sqlspec.utils.sync_tools import ensure_async_
from sqlspec.utils.telemetry import instrument_operation, instrument_operation_async

__all__ = ("OracleAsyncConnection", "OracleAsyncDriver", "OracleSyncConnection", "OracleSyncDriver")

OracleSyncConnection = Connection
OracleAsyncConnection = AsyncConnection

logger = logging.getLogger("sqlspec")


class OracleSyncDriver(
    SyncDriverAdapterProtocol[OracleSyncConnection, RowT],
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
        default_row_type: type[DictRow] = DictRow,
    ) -> None:
        super().__init__(
            connection=connection,
            config=config,
            instrumentation_config=instrumentation_config,
            default_row_type=default_row_type,
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

    def _execute_statement(
        self,
        statement: SQL,
        connection: Optional[OracleSyncConnection] = None,
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
        connection: Optional[OracleSyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        with instrument_operation(self, "oracle_execute", "database"):
            conn = self._connection(connection)
            final_driver_params = params
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL: %s", sql)
            if self.instrumentation_config.log_parameters and final_driver_params:
                logger.debug("Query parameters: %s", final_driver_params)
            with self._get_cursor(conn) as cursor:
                if final_driver_params is None:
                    cursor.execute(sql)
                elif isinstance(final_driver_params, dict):
                    cursor.execute(sql, final_driver_params)
                elif isinstance(final_driver_params, (list, tuple)):
                    cursor.execute(sql, list(final_driver_params))
                else:
                    cursor.execute(sql, [final_driver_params])
                return cursor

    def _execute_many(
        self,
        sql: str,
        param_list: Any,
        connection: Optional[OracleSyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        with instrument_operation(self, "oracle_execute_many", "database"):
            conn = self._connection(connection)
            params_list = param_list if isinstance(param_list, Sequence) else []
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL (executemany): %s", sql)
            if self.instrumentation_config.log_parameters and params_list:
                logger.debug("Query parameters (batch): %s", params_list)
            with self._get_cursor(conn) as cursor:
                cursor.executemany(sql, params_list)
                return cursor

    def _execute_script(
        self,
        script: str,
        connection: Optional[OracleSyncConnection] = None,
        **kwargs: Any,
    ) -> str:
        with instrument_operation(self, "oracle_execute_script", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL script: %s", script)
            with self._get_cursor(conn) as cursor:
                cursor.execute(script)
            return "SCRIPT EXECUTED"

    def _wrap_select_result(
        self,
        statement: SQL,
        result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        with instrument_operation(self, "oracle_wrap_select", "database"):
            cursor = cast("Cursor", result)
            if not cursor.description:
                return SQLResult[RowT](statement=statement, data=[], column_names=[], operation_type="SELECT")
            column_names = [col[0] for col in cursor.description]
            fetched_tuples = cursor.fetchall()
            rows_as_dicts: list[dict[str, Any]] = [dict(zip(column_names, row_tuple)) for row_tuple in fetched_tuples]

            if self.instrumentation_config.log_results_count:
                logger.debug("Query returned %d rows", len(rows_as_dicts))

            if schema_type:
                converted_data_seq = self.to_schema(rows_as_dicts, schema_type=schema_type)
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

    def _wrap_execute_result(
        self,
        statement: SQL,
        result: Any,
        **kwargs: Any,
    ) -> SQLResult[RowT]:
        with instrument_operation(self, "oracle_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()

            rows_affected = -1
            status_message: Optional[str] = None

            if isinstance(result, str) and result == "SCRIPT EXECUTED":
                operation_type = "SCRIPT"
                rows_affected = 0  # No specific row count for script success message
                status_message = result
            else:
                cursor = cast("Cursor", result)
                if cursor and hasattr(cursor, "rowcount"):
                    rc = cursor.rowcount
                    if isinstance(rc, list):  # For batch operations, rowcount might be a list
                        rows_affected = sum(rc)
                    elif isinstance(rc, int):
                        rows_affected = rc

            if self.instrumentation_config.log_results_count:
                logger.debug("Execute operation affected %d rows", rows_affected)

            # Data is empty as DML with RETURNING needs special handling in Oracle (out params)
            # not covered by generic cursor.fetchall() in _wrap_execute_result.
            # last_inserted_id is not standard available from cursor.rowcount or cursor.lastrowid.
            return SQLResult[RowT](
                statement=statement,
                data=[],
                rows_affected=rows_affected,
                operation_type=operation_type,
                metadata={"status_message": status_message} if status_message else {},
            )

    def _select_to_arrow_impl(
        self,
        stmt_obj: "SQL",
        connection: "OracleSyncConnection",
        **kwargs: Any,
    ) -> "ArrowResult":
        # SQL object (stmt_obj) is already built and validated by the mixin.
        # returns_rows check is done by the mixin.
        # Instrumentation is handled by the mixin (SyncArrowMixin).
        # pyarrow as pa is imported at the top of the file.

        final_sql = stmt_obj.to_sql(placeholder_style=self._get_placeholder_style())
        oracle_params = stmt_obj.get_parameters(style=self._get_placeholder_style())

        oracle_params_dict: dict[str, Any]
        if oracle_params is None:
            oracle_params_dict = {}
        elif isinstance(oracle_params, dict):
            oracle_params_dict = oracle_params
        elif isinstance(oracle_params, (list, tuple)):
            # Oracle named params are like :p0, :p1. SQL.get_parameters should ideally return a dict.
            # If it returns a list/tuple for named style, this conversion is a fallback.
            oracle_params_dict = {f"p{i}": v for i, v in enumerate(oracle_params)}
        else:  # Single parameter
            oracle_params_dict = {"p0": oracle_params}  # Fallback for a single non-dict/sequence param

        if self.instrumentation_config.log_queries:
            logger.debug("Executing Oracle Sync Arrow query: %s", final_sql)
        if self.instrumentation_config.log_parameters and oracle_params_dict:
            logger.debug("Query parameters for Oracle Sync Arrow: %s", oracle_params_dict)

        with self._get_cursor(connection) as cursor:
            cursor.execute(final_sql, oracle_params_dict)
            native_df = False
            if hasattr(cursor, "fetch_df_all"):
                rows = cursor.fetch_df_all()  # pyright: ignore
                native_df = True
            else:
                rows = cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]
            if not rows:
                column_names_from_desc = [col[0] for col in cursor.description or []]
                return ArrowResult(statement=stmt_obj, data=pa.Table.from_arrays([], names=column_names_from_desc))
            if native_df:
                arrow_table = rows
            else:
                empty_cols: list[list[str]] = [[] for _ in column_names]
                list_of_cols = list(zip(*rows)) if rows else empty_cols

                arrow_table = pa.Table.from_arrays(list_of_cols, names=column_names)
            return ArrowResult(statement=stmt_obj, data=cast("ArrowTable", arrow_table))


class OracleAsyncDriver(
    AsyncDriverAdapterProtocol[OracleAsyncConnection, RowT],
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

    async def _execute_statement(
        self,
        statement: SQL,
        connection: Optional[OracleAsyncConnection] = None,
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
        connection: Optional[OracleAsyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "oracle_async_execute", "database"):
            conn = self._connection(connection)
            final_driver_params = params
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL: %s", sql)
            if self.instrumentation_config.log_parameters and final_driver_params:
                logger.debug("Query parameters: %s", final_driver_params)
            async with self._get_cursor(conn) as cursor:
                if final_driver_params is None:
                    await cursor.execute(sql)
                elif isinstance(final_driver_params, dict):
                    await cursor.execute(sql, final_driver_params)
                elif isinstance(final_driver_params, (list, tuple)):
                    await cursor.execute(sql, list(final_driver_params))
                else:
                    await cursor.execute(sql, [final_driver_params])
                return cursor

    async def _execute_many(
        self,
        sql: str,
        param_list: Any,
        connection: Optional[OracleAsyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "oracle_async_execute_many", "database"):
            conn = self._connection(connection)
            params_list = param_list if isinstance(param_list, Sequence) else []
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
        connection: Optional[OracleAsyncConnection] = None,
        **kwargs: Any,
    ) -> str:
        async with instrument_operation_async(self, "oracle_async_execute_script", "database"):
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
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        async with instrument_operation_async(self, "oracle_async_wrap_select", "database"):
            cursor = cast("AsyncCursor", result)
            if not cursor.description:
                return SQLResult[RowT](statement=statement, data=[], column_names=[], operation_type="SELECT")
            column_names = [col[0] for col in cursor.description]
            fetched_tuples = await cursor.fetchall()
            rows_as_dicts: list[dict[str, Any]] = [dict(zip(column_names, row_tuple)) for row_tuple in fetched_tuples]

            if self.instrumentation_config.log_results_count:
                logger.debug("Query returned %d rows", len(rows_as_dicts))

            if schema_type:
                converted_data_seq = self.to_schema(rows_as_dicts, schema_type=schema_type)
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
        async with instrument_operation_async(self, "oracle_async_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()

            rows_affected = -1
            status_message: Optional[str] = None

            if isinstance(result, str) and result == "SCRIPT EXECUTED":
                operation_type = "SCRIPT"
                rows_affected = 0
                status_message = result
            else:
                cursor = cast("AsyncCursor", result)
                if cursor and hasattr(cursor, "rowcount"):
                    rc = cursor.rowcount
                    if isinstance(rc, list):
                        rows_affected = sum(rc)
                    elif isinstance(rc, int):
                        rows_affected = rc

            if self.instrumentation_config.log_results_count:
                logger.debug("Execute operation affected %d rows", rows_affected)

            return SQLResult[RowT](
                statement=statement,
                data=[],
                rows_affected=rows_affected,
                operation_type=operation_type,
                metadata={"status_message": status_message} if status_message else {},
            )

    async def _select_to_arrow_impl(
        self,
        stmt_obj: "SQL",
        connection: "OracleAsyncConnection",
        **kwargs: Any,
    ) -> "ArrowResult":
        # SQL object (stmt_obj) is already built and validated by the mixin.
        # returns_rows check is done by the mixin.
        # Instrumentation is handled by the mixin (AsyncArrowMixin).
        # pyarrow as pa is imported at the top of the file.

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

        if self.instrumentation_config.log_queries:
            logger.debug("Executing Oracle Async Arrow query: %s", final_sql)
        if self.instrumentation_config.log_parameters and oracle_params_dict:
            logger.debug("Query parameters for Oracle Async Arrow: %s", oracle_params_dict)

        async with self._get_cursor(connection) as cursor:
            await cursor.execute(final_sql, oracle_params_dict)
            native_df = False
            if hasattr(cursor, "fetch_df_all"):
                rows = await cursor.fetch_df_all()  # pyright: ignore
                native_df = True
            else:
                rows = await cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]
            if not rows:
                column_names_from_desc = [col[0] for col in cursor.description or []]
                return ArrowResult(statement=stmt_obj, data=pa.Table.from_arrays([], names=column_names_from_desc))
            if native_df:
                arrow_table = rows
            else:
                empty_cols: list[list[str]] = [[] for _ in column_names]
                list_of_cols = list(zip(*rows)) if rows else empty_cols

                arrow_table = pa.Table.from_arrays(list_of_cols, names=column_names)
            return ArrowResult(statement=stmt_obj, data=cast("ArrowTable", arrow_table))
