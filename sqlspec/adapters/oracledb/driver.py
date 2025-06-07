# ruff: noqa: PLR6301
from collections.abc import AsyncGenerator, Generator, Sequence
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from oracledb import AsyncConnection, AsyncCursor, Connection, Cursor

from sqlspec.config import InstrumentationConfig
from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
from sqlspec.driver.mixins import AsyncStorageMixin, SQLTranslatorMixin, SyncStorageMixin, ToSchemaMixin
from sqlspec.exceptions import wrap_exceptions
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, RowT
from sqlspec.utils.logging import get_logger
from sqlspec.utils.sync_tools import ensure_async_
from sqlspec.utils.telemetry import instrument_operation, instrument_operation_async

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

__all__ = ("OracleAsyncConnection", "OracleAsyncDriver", "OracleSyncConnection", "OracleSyncDriver")

OracleSyncConnection = Connection
OracleAsyncConnection = AsyncConnection

logger = get_logger("adapters.oracledb")


class OracleSyncDriver(
    SyncDriverAdapterProtocol[OracleSyncConnection, RowT], SQLTranslatorMixin, SyncStorageMixin, ToSchemaMixin
):
    """Oracle Sync Driver Adapter. Refactored for new protocol."""

    dialect: "DialectType" = "oracle"
    __supports_arrow__: ClassVar[bool] = True
    __supports_parquet__: ClassVar[bool] = False

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
        parameters: Any,
        statement: SQL,
        connection: Optional[OracleSyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        with instrument_operation(self, "oracle_execute", "database"):
            conn = self._connection(connection)
            final_driver_params = parameters
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
            # Convert param_list to list[Any] if it's not already
            if isinstance(param_list, list):
                final_param_list = param_list
            elif isinstance(param_list, (tuple, Sequence)):
                final_param_list = list(param_list)
            else:
                # Single parameter set, wrap in list
                final_param_list = [param_list] if param_list is not None else []

            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL (executemany): %s", sql)
            if self.instrumentation_config.log_parameters and final_param_list:
                logger.debug("Query parameters (batch): %s", final_param_list)
            with self._get_cursor(conn) as cursor:
                cursor.executemany(sql, final_param_list)
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
            with wrap_exceptions(wrap_exceptions=False, suppress=AttributeError):
                if statement.expression:
                    operation_type = str(statement.expression.key).upper()

            rows_affected = -1
            status_message: Optional[str] = None

            if isinstance(result, str) and result == "SCRIPT EXECUTED":
                operation_type = "SCRIPT"
                rows_affected = 0  # No specific row count for script success message
                status_message = result
            else:
                cursor = cast("Cursor", result)
                with wrap_exceptions(wrap_exceptions=False, suppress=AttributeError):
                    if cursor:
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


class OracleAsyncDriver(
    AsyncDriverAdapterProtocol[OracleAsyncConnection, RowT], SQLTranslatorMixin, AsyncStorageMixin, ToSchemaMixin
):
    """Oracle Async Driver Adapter. Refactored for new protocol."""

    dialect: str = "oracle"
    __supports_arrow__: ClassVar[bool] = True
    __supports_parquet__: ClassVar[bool] = False

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
        parameters: Any,
        statement: SQL,
        connection: Optional[OracleAsyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "oracle_async_execute", "database"):
            conn = self._connection(connection)
            final_driver_params = parameters
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
            # Convert param_list to list[Any] if it's not already
            if isinstance(param_list, list):
                final_param_list = param_list
            elif isinstance(param_list, (tuple, Sequence)):
                final_param_list = list(param_list)
            else:
                # Single parameter set, wrap in list
                final_param_list = [param_list] if param_list is not None else []

            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL (executemany): %s", sql)
            if self.instrumentation_config.log_parameters and final_param_list:
                logger.debug("Query parameters (batch): %s", final_param_list)
            async with self._get_cursor(conn) as cursor:
                await cursor.executemany(sql, final_param_list)
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
            with wrap_exceptions(wrap_exceptions=False, suppress=AttributeError):
                if statement.expression:
                    operation_type = str(statement.expression.key).upper()

            rows_affected = -1
            status_message: Optional[str] = None

            if isinstance(result, str) and result == "SCRIPT EXECUTED":
                operation_type = "SCRIPT"
                rows_affected = 0
                status_message = result
            else:
                cursor = cast("AsyncCursor", result)
                with wrap_exceptions(wrap_exceptions=False, suppress=AttributeError):
                    if cursor:
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
